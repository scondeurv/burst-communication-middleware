use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use dashmap::DashMap;
use deadpool_redis::{Config, Pool, Runtime};
use futures::lock::Mutex;
use redis::{
    aio::ConnectionLike,
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands,
};

use crate::{
    impl_chainable_setter, BurstOptions, RemoteBroadcastProxy, RemoteBroadcastReceiveProxy,
    RemoteBroadcastSendProxy, RemoteMessage, RemoteReceiveProxy, RemoteSendProxy,
    RemoteSendReceiveFactory, RemoteSendReceiveProxy, Result,
};

#[derive(Clone, Debug)]
pub struct RedisStreamOptions {
    pub redis_uri: String,
    pub direct_stream_prefix: String,
    pub broadcast_stream_prefix: String,
}

impl RedisStreamOptions {
    pub fn new(redis_uri: String) -> Self {
        Self {
            redis_uri,
            ..Default::default()
        }
    }

    impl_chainable_setter!(redis_uri, String);
    impl_chainable_setter!(direct_stream_prefix, String);
    impl_chainable_setter!(broadcast_stream_prefix, String);

    pub fn build(&self) -> Self {
        self.clone()
    }
}

impl Default for RedisStreamOptions {
    fn default() -> Self {
        Self {
            redis_uri: "redis://localhost:6379".into(),
            direct_stream_prefix: "direct_stream".into(),
            broadcast_stream_prefix: "broadcast_stream".into(),
        }
    }
}

pub struct RedisStreamImpl;

#[async_trait]
impl RemoteSendReceiveFactory<RedisStreamOptions> for RedisStreamImpl {
    async fn create_remote_proxies(
        burst_options: Arc<BurstOptions>,
        redis_options: RedisStreamOptions,
    ) -> Result<
        HashMap<
            u32,
            (
                Box<dyn RemoteSendReceiveProxy>,
                Box<dyn RemoteBroadcastProxy>,
            ),
        >,
    > {
        let redis_options = Arc::new(redis_options);
        // create redis pool with deadpool
        let group_size = burst_options
            .group_ranges
            .get(&burst_options.group_id)
            .unwrap()
            .len();
        let pool = Config::from_url(redis_options.redis_uri.clone())
            .builder()
            .unwrap()
            .max_size(group_size)
            .runtime(Runtime::Tokio1)
            .build()
            .unwrap();

        let current_group = burst_options
            .group_ranges
            .get(&burst_options.group_id)
            .unwrap();

        let mut proxies = HashMap::new();

        futures::future::try_join_all(current_group.iter().map(|worker_id| {
            let p = pool.clone();
            let r = redis_options.clone();
            let b = burst_options.clone();
            async move {
                let proxy = RedisStreamProxy::new(p.clone(), r.clone(), b.clone(), *worker_id)
                    .await
                    .unwrap();
                let broadcast_proxy = RedisStreamBroadcastProxy::new(p, r, b, *worker_id)
                    .await
                    .unwrap();
                Ok::<_, std::io::Error>((proxy, broadcast_proxy))
            }
        }))
        .await?
        .into_iter()
        .for_each(|(proxy, broadcast_proxy)| {
            proxies.insert(
                proxy.worker_id,
                (
                    Box::new(proxy) as Box<dyn RemoteSendReceiveProxy>,
                    Box::new(broadcast_proxy) as Box<dyn RemoteBroadcastProxy>,
                ),
            );
        });

        Ok(proxies)
    }
}

// DIRECT PROXIES

pub struct RedisStreamProxy {
    worker_id: u32,
    receiver: Box<dyn RemoteReceiveProxy>,
    sender: Box<dyn RemoteSendProxy>,
}

pub struct RedisStreamSendProxy {
    redis_pool: Pool,
    redis_options: Arc<RedisStreamOptions>,
    burst_options: Arc<BurstOptions>,
    worker_id: u32,
}

pub struct RedisStreamReceiveProxy {
    redis_pool: Pool,
    redis_options: Arc<RedisStreamOptions>,
    burst_options: Arc<BurstOptions>,
    worker_id: u32,
    stream_ids: DashMap<u32, String>,
}

impl RemoteSendReceiveProxy for RedisStreamProxy {}

#[async_trait]
impl RemoteSendProxy for RedisStreamProxy {
    async fn remote_send(&self, dest: u32, msg: RemoteMessage) -> Result<()> {
        self.sender.remote_send(dest, msg).await
    }
}

#[async_trait]
impl RemoteReceiveProxy for RedisStreamProxy {
    async fn remote_recv(&self, source: u32) -> Result<RemoteMessage> {
        self.receiver.remote_recv(source).await
    }
}

impl RedisStreamProxy {
    pub async fn new(
        redis_pool: Pool,
        redis_options: Arc<RedisStreamOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Result<Self> {
        Ok(Self {
            worker_id,
            sender: Box::new(RedisStreamSendProxy::new(
                redis_pool.clone(),
                redis_options.clone(),
                burst_options.clone(),
                worker_id,
            )),
            receiver: Box::new(RedisStreamReceiveProxy::new(
                redis_pool.clone(),
                redis_options.clone(),
                burst_options.clone(),
                worker_id,
            )),
        })
    }
}

#[async_trait]
impl RemoteSendProxy for RedisStreamSendProxy {
    async fn remote_send(&self, dest: u32, msg: RemoteMessage) -> Result<()> {
        let con = self.redis_pool.get().await?;
        Ok(send_direct(
            con,
            msg,
            self.worker_id,
            dest,
            &self.redis_options,
            &self.burst_options,
        )
        .await?)
    }
}

impl RedisStreamSendProxy {
    pub fn new(
        redis_pool: Pool,
        redis_options: Arc<RedisStreamOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Self {
        Self {
            redis_pool,
            redis_options,
            burst_options,
            worker_id,
        }
    }
}

#[async_trait]
impl RemoteReceiveProxy for RedisStreamReceiveProxy {
    async fn remote_recv(&self, source: u32) -> Result<RemoteMessage> {
        log::debug!(
            "[Redis Stream] Getting RemoteMessage from source {}",
            source
        );
        let last_id = match self.stream_ids.get(&source) {
            Some(id) => id.value().clone(),
            None => "0".to_string(),
        };

        let mut con = self.redis_pool.get().await?;
        let (new_last_id, reply) = read_stream(
            &mut con,
            &get_direct_stream_name(
                &self.redis_options.direct_stream_prefix,
                &self.burst_options.burst_id,
                source,
                self.worker_id,
            ),
            &last_id,
        )
        .await?;

        self.stream_ids.insert(source, new_last_id);

        let msg = deserialize_stream_reply(reply)?;
        // log::debug!("[Redis Stream] Got RemoteMessage {:?}", msg);
        Ok(msg)
    }
}

impl RedisStreamReceiveProxy {
    pub fn new(
        redis_pool: Pool,
        redis_options: Arc<RedisStreamOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Self {
        let stream_offsets = DashMap::with_capacity(burst_options.burst_size as usize);
        Self {
            redis_pool,
            redis_options,
            burst_options,
            worker_id,
            stream_ids: stream_offsets,
        }
    }
}

// BROADCAST PROXIES

pub struct RedisStreamBroadcastProxy {
    broadcast_sender: Box<dyn RemoteBroadcastSendProxy>,
    broadcast_receiver: Box<dyn RemoteBroadcastReceiveProxy>,
}

pub struct RedisStreamBroadcastSendProxy {
    redis_pool: Pool,
    redis_options: Arc<RedisStreamOptions>,
    burst_options: Arc<BurstOptions>,
}

pub struct RedisStreamBroadcastReceiveProxy {
    redis_pool: Pool,
    broadcast_recv_key: String,
    broadcast_stream_id: Mutex<String>,
}

impl RemoteBroadcastProxy for RedisStreamBroadcastProxy {}

impl RedisStreamBroadcastProxy {
    pub async fn new(
        redis_pool: Pool,
        redis_options: Arc<RedisStreamOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Result<Self> {
        let send_proxy = RedisStreamBroadcastSendProxy::new(
            redis_pool.clone(),
            redis_options.clone(),
            burst_options.clone(),
        );
        let receive_proxy = RedisStreamBroadcastReceiveProxy::new(
            redis_pool.clone(),
            redis_options.clone(),
            burst_options.clone(),
            worker_id,
        );
        Ok(Self {
            broadcast_sender: Box::new(send_proxy),
            broadcast_receiver: Box::new(receive_proxy),
        })
    }
}

#[async_trait]
impl RemoteBroadcastSendProxy for RedisStreamBroadcastProxy {
    async fn remote_broadcast_send(&self, msg: RemoteMessage) -> Result<()> {
        self.broadcast_sender.remote_broadcast_send(msg).await
    }
}

#[async_trait]
impl RemoteBroadcastReceiveProxy for RedisStreamBroadcastProxy {
    async fn remote_broadcast_recv(&self) -> Result<RemoteMessage> {
        self.broadcast_receiver.remote_broadcast_recv().await
    }
}

#[async_trait]
impl RemoteBroadcastSendProxy for RedisStreamBroadcastSendProxy {
    async fn remote_broadcast_send(&self, msg: RemoteMessage) -> Result<()> {
        let msg = &msg;
        futures::future::try_join_all(
            self.burst_options
                .group_ranges
                .keys()
                .filter(|dest| **dest != self.burst_options.group_id)
                .map(|dest| {
                    send_broadcast(
                        &self.redis_pool,
                        msg,
                        dest,
                        &self.redis_options,
                        &self.burst_options,
                    )
                }),
        )
        .await?;
        Ok(())
    }
}

impl RedisStreamBroadcastSendProxy {
    pub fn new(
        redis_pool: Pool,
        redis_options: Arc<RedisStreamOptions>,
        burst_options: Arc<BurstOptions>,
    ) -> Self {
        Self {
            redis_pool,
            redis_options,
            burst_options,
        }
    }
}

#[async_trait]
impl RemoteBroadcastReceiveProxy for RedisStreamBroadcastReceiveProxy {
    async fn remote_broadcast_recv(&self) -> Result<RemoteMessage> {
        let mut con = self.redis_pool.get().await?;
        let mut last_id = self.broadcast_stream_id.lock().await;
        let (new_last_id, stream_reply) = read_stream(&mut con, &self.broadcast_recv_key, &last_id)
            .await
            .unwrap();
        *last_id = new_last_id;
        let msg = deserialize_stream_reply(stream_reply).unwrap();
        Ok(msg)
    }
}

impl RedisStreamBroadcastReceiveProxy {
    pub fn new(
        redis_pool: Pool,
        redis_options: Arc<RedisStreamOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Self {
        let broadcast_recv_key = format!(
            "{}:{}:g{}",
            redis_options.broadcast_stream_prefix, burst_options.burst_id, worker_id
        );
        let broadcast_stream_id = Mutex::new("0".to_string());
        Self {
            redis_pool,
            broadcast_recv_key,
            broadcast_stream_id,
        }
    }
}

// Helper functions

async fn send_direct<C>(
    connection: C,
    msg: RemoteMessage,
    source: u32,
    dest: u32,
    redis_options: &RedisStreamOptions,
    burst_options: &BurstOptions,
) -> Result<()>
where
    C: ConnectionLike + Send,
{
    send_redis(
        connection,
        &msg,
        &get_direct_stream_name(
            &redis_options.direct_stream_prefix,
            &burst_options.burst_id,
            source,
            dest,
        ),
    )
    .await
}

async fn send_broadcast(
    pool: &Pool,
    msg: &RemoteMessage,
    dest: &str,
    redis_options: &RedisStreamOptions,
    burst_options: &BurstOptions,
) -> Result<()> {
    let conn = pool.get().await?;
    send_redis(
        conn,
        msg,
        &get_broadcast_stream_name(
            &redis_options.broadcast_stream_prefix,
            &burst_options.burst_id,
            dest,
        ),
    )
    .await
}

async fn send_redis<C>(mut connection: C, msg: &RemoteMessage, key: &str) -> Result<()>
where
    C: ConnectionLike + Send,
{
    log::debug!("[Redis Stream] Adding RemoteMessage to stream {}", key);
    let data: [&[u8]; 2] = msg.into();
    connection
        .xadd::<_, _, _, _, ()>(key, "*", &[("h", data[0]), ("p", data[1])])
        .await?;
    Ok(())
}

async fn read_stream<C>(
    connection: &mut C,
    stream: &str,
    last_id: &str,
) -> Result<(String, StreamReadReply)>
where
    C: ConnectionLike + Send,
{
    log::debug!(
        "[Redis Stream] Reading from stream {} with last_id {}",
        stream,
        last_id
    );
    let reply: StreamReadReply = connection
        .xread_options(
            &[stream],
            &[last_id],
            &StreamReadOptions::default().count(1).block(0),
        )
        .await?;

    let stream_key = reply.keys.first().unwrap();
    let last_id = stream_key.ids.first().unwrap().id.clone();

    log::debug!(
        "[Redis Stream] Got last_id {} from stream {}",
        last_id,
        stream
    );

    Ok((last_id, reply))
}

fn deserialize_stream_reply(reply: StreamReadReply) -> Result<RemoteMessage> {
    let mut reply_map = reply
        .keys
        .into_iter()
        .next()
        .unwrap()
        .ids
        .into_iter()
        .next()
        .unwrap()
        .map;
    let header = match reply_map.remove("h").unwrap() {
        redis::Value::Data(d) => d,
        _ => panic!("Expected header to be a redis::Value::Data"),
    };
    let payload = match reply_map.remove("p").unwrap() {
        redis::Value::Data(d) => d,
        _ => panic!("Expected payload to be a redis::Value::Data"),
    };
    Ok(RemoteMessage::from((header, payload)))
}

fn get_direct_stream_name(
    prefix: &str,
    burst_id: &str,
    worker_source: u32,
    worker_dest: u32,
) -> String {
    format!(
        "{}:{}:s{}-d{}",
        prefix, burst_id, worker_source, worker_dest
    )
}

fn get_broadcast_stream_name(prefix: &str, burst_id: &str, group_id: &str) -> String {
    format!("{}:{}:g{}", prefix, burst_id, group_id)
}
