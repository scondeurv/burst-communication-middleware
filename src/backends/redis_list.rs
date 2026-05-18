use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use redis::{aio::ConnectionLike, cmd, AsyncCommands, Client};
use tokio::sync::Mutex;

use crate::{
    impl_chainable_setter, BurstOptions, RemoteBroadcastProxy, RemoteBroadcastReceiveProxy,
    RemoteBroadcastSendProxy, RemoteMessage, RemoteReceiveProxy, RemoteSendProxy,
    RemoteSendReceiveFactory, RemoteSendReceiveProxy, Result,
};

#[derive(Clone, Debug)]
pub struct RedisListOptions {
    pub redis_uri: String,
    pub list_key_prefix: String,
    pub broadcast_topic_prefix: String,
}

impl RedisListOptions {
    pub fn new(redis_uri: String) -> Self {
        Self {
            redis_uri,
            ..Default::default()
        }
    }

    impl_chainable_setter!(redis_uri, String);
    impl_chainable_setter!(list_key_prefix, String);
    impl_chainable_setter!(broadcast_topic_prefix, String);

    pub fn build(&self) -> Self {
        self.clone()
    }
}

impl Default for RedisListOptions {
    fn default() -> Self {
        Self {
            redis_uri: "redis://localhost:6379".to_string(),
            list_key_prefix: "direct_stream".into(),
            broadcast_topic_prefix: "broadcast_stream".into(),
        }
    }
}

pub struct RedisListImpl;

#[async_trait]
impl RemoteSendReceiveFactory<RedisListOptions> for RedisListImpl {
    async fn create_remote_proxies(
        burst_options: Arc<BurstOptions>,
        redis_options: RedisListOptions,
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
        let redis_client = Arc::new(Client::open(redis_options.redis_uri.clone())?);

        let current_group = burst_options
            .group_ranges
            .get(&burst_options.group_id)
            .unwrap();

        let mut proxies = HashMap::new();

        for worker_id in current_group {
            let proxy = RedisListProxy::new(
                redis_client.clone(),
                redis_options.clone(),
                burst_options.clone(),
                *worker_id,
            )
            .await?;
            let broadcast_proxy = RedisListBroadcastProxy::new(
                redis_client.clone(),
                redis_options.clone(),
                burst_options.clone(),
            )
            .await?;
            proxies.insert(
                proxy.worker_id,
                (
                    Box::new(proxy) as Box<dyn RemoteSendReceiveProxy>,
                    Box::new(broadcast_proxy) as Box<dyn RemoteBroadcastProxy>,
                ),
            );
        }

        Ok(proxies)
    }
}

// DIRECT PROXIES

pub struct RedisListProxy {
    worker_id: u32,
    receiver: Box<dyn RemoteReceiveProxy>,
    sender: Box<dyn RemoteSendProxy>,
}

pub struct RedisListSendProxy {
    redis_client: Arc<Client>,
    connection: Arc<Mutex<Option<redis::aio::MultiplexedConnection>>>,
    redis_options: Arc<RedisListOptions>,
    burst_options: Arc<BurstOptions>,
    worker_id: u32,
}

pub struct RedisListReceiveProxy {
    redis_client: Arc<Client>,
    connection: Arc<Mutex<Option<redis::aio::MultiplexedConnection>>>,
    redis_options: Arc<RedisListOptions>,
    burst_options: Arc<BurstOptions>,
    worker_id: u32,
}

impl RemoteSendReceiveProxy for RedisListProxy {}

#[async_trait]
impl RemoteSendProxy for RedisListProxy {
    async fn remote_send(&self, dest: u32, msg: RemoteMessage) -> Result<()> {
        self.sender.remote_send(dest, msg).await
    }
}

#[async_trait]
impl RemoteReceiveProxy for RedisListProxy {
    async fn remote_recv(&self, source: u32) -> Result<RemoteMessage> {
        self.receiver.remote_recv(source).await
    }
}

impl RedisListProxy {
    pub async fn new(
        redis_client: Arc<Client>,
        redis_options: Arc<RedisListOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Result<Self> {
        Ok(Self {
            worker_id,
            sender: Box::new(RedisListSendProxy::new(
                redis_client.clone(),
                Arc::new(Mutex::new(None)),
                redis_options.clone(),
                burst_options.clone(),
                worker_id,
            )),
            receiver: Box::new(RedisListReceiveProxy::new(
                redis_client,
                Arc::new(Mutex::new(None)),
                redis_options,
                burst_options,
                worker_id,
            )),
        })
    }
}

impl RedisListSendProxy {
    pub fn new(
        redis_client: Arc<Client>,
        connection: Arc<Mutex<Option<redis::aio::MultiplexedConnection>>>,
        redis_options: Arc<RedisListOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Self {
        Self {
            redis_client,
            connection,
            redis_options,
            burst_options,
            worker_id,
        }
    }
}

#[async_trait]
impl RemoteSendProxy for RedisListSendProxy {
    async fn remote_send(&self, dest: u32, msg: RemoteMessage) -> Result<()> {
        log::debug!(
            "[Redis List] remote_send worker={} dest={} using connection",
            self.worker_id,
            dest
        );
        let mut con = self.connection.lock().await;
        if con.is_none() {
            *con = Some(self.redis_client.get_multiplexed_tokio_connection().await?);
        }
        Ok(send_direct(
            con.as_mut().unwrap(),
            msg,
            self.worker_id,
            dest,
            &self.redis_options,
            &self.burst_options,
        )
        .await?)
    }
}

impl RedisListReceiveProxy {
    pub fn new(
        redis_client: Arc<Client>,
        connection: Arc<Mutex<Option<redis::aio::MultiplexedConnection>>>,
        redis_options: Arc<RedisListOptions>,
        burst_options: Arc<BurstOptions>,
        worker_id: u32,
    ) -> Self {
        Self {
            redis_client,
            connection,
            redis_options,
            burst_options,
            worker_id,
        }
    }
}

#[async_trait]
impl RemoteReceiveProxy for RedisListReceiveProxy {
    async fn remote_recv(&self, source: u32) -> Result<RemoteMessage> {
        log::debug!(
            "[Redis List] remote_recv worker={} source={} using connection",
            self.worker_id,
            source
        );
        let mut con = self.connection.lock().await;
        if con.is_none() {
            *con = Some(self.redis_client.get_multiplexed_tokio_connection().await?);
        }
        let msg = read_redis(
            con.as_mut().unwrap(),
            &get_redis_list_key(
                &self.redis_options.list_key_prefix,
                &self.burst_options.burst_id,
                source,
                self.worker_id,
            ),
        )
        .await?;
        Ok(msg)
    }
}

// BROADCAST PROXIES

pub struct RedisListBroadcastProxy {
    broadcast_sender: Box<dyn RemoteBroadcastSendProxy>,
    broadcast_receiver: Box<dyn RemoteBroadcastReceiveProxy>,
}

pub struct RedisListBroadcastSendProxy {
    redis_client: Arc<Client>,
    connection: Arc<Mutex<Option<redis::aio::MultiplexedConnection>>>,
    redis_options: Arc<RedisListOptions>,
    burst_options: Arc<BurstOptions>,
}

pub struct RedisListBroadcastReceiveProxy {
    redis_client: Arc<Client>,
    connection: Arc<Mutex<Option<redis::aio::MultiplexedConnection>>>,
    broadcast_recv_key: String,
}

impl RemoteBroadcastProxy for RedisListBroadcastProxy {}

impl RedisListBroadcastProxy {
    pub async fn new(
        redis_client: Arc<Client>,
        redis_options: Arc<RedisListOptions>,
        burst_options: Arc<BurstOptions>,
    ) -> Result<Self> {
        let send_proxy = RedisListBroadcastSendProxy::new(
            redis_client.clone(),
            Arc::new(Mutex::new(None)),
            redis_options.clone(),
            burst_options.clone(),
        );
        let receive_proxy =
            RedisListBroadcastReceiveProxy::new(redis_client, Arc::new(Mutex::new(None)), redis_options, burst_options);
        Ok(Self {
            broadcast_sender: Box::new(send_proxy),
            broadcast_receiver: Box::new(receive_proxy),
        })
    }
}

#[async_trait]
impl RemoteBroadcastSendProxy for RedisListBroadcastProxy {
    async fn remote_broadcast_send(&self, msg: RemoteMessage) -> Result<()> {
        self.broadcast_sender.remote_broadcast_send(msg).await
    }
}

#[async_trait]
impl RemoteBroadcastReceiveProxy for RedisListBroadcastProxy {
    async fn remote_broadcast_recv(&self) -> Result<RemoteMessage> {
        self.broadcast_receiver.remote_broadcast_recv().await
    }
}

#[async_trait]
impl RemoteBroadcastSendProxy for RedisListBroadcastSendProxy {
    async fn remote_broadcast_send(&self, msg: RemoteMessage) -> Result<()> {
        let mut conn = self.connection.lock().await;
        if conn.is_none() {
            *conn = Some(self.redis_client.get_multiplexed_tokio_connection().await?);
        }
        let conn = conn.as_mut().unwrap();

        let bcast_key = format!(
            "{}:broadcast:{}:{}",
            self.burst_options.burst_id, msg.metadata.counter, msg.metadata.chunk_id
        );
        log::debug!("SET {:?} {}:header {}:payload", msg, bcast_key, bcast_key);
        let [header, payload]: [&[u8]; 2] = (&msg).into();
        conn.set::<_, _, ()>(format!("{}:header", bcast_key), header).await?;
        conn.set::<_, _, ()>(format!("{}:payload", bcast_key), payload).await?;

        for dest in self.burst_options.group_ranges.keys() {
            if *dest == self.burst_options.group_id {
                continue;
            }
            let dest_group_key = get_broadcast_list_key(
                &self.redis_options.broadcast_topic_prefix,
                &self.burst_options.burst_id,
                dest,
            );
            log::debug!("RPUSH {} {}", dest_group_key, bcast_key);
            conn.rpush::<_, _, ()>(dest_group_key, &bcast_key).await?;
        }
        Ok(())
    }
}

impl RedisListBroadcastSendProxy {
    pub fn new(
        redis_client: Arc<Client>,
        connection: Arc<Mutex<Option<redis::aio::MultiplexedConnection>>>,
        redis_options: Arc<RedisListOptions>,
        burst_options: Arc<BurstOptions>,
    ) -> Self {
        Self {
            redis_client,
            connection,
            redis_options,
            burst_options,
        }
    }
}

#[async_trait]
impl RemoteBroadcastReceiveProxy for RedisListBroadcastReceiveProxy {
    async fn remote_broadcast_recv(&self) -> Result<RemoteMessage> {
        let mut conn = self.connection.lock().await;
        if conn.is_none() {
            *conn = Some(self.redis_client.get_multiplexed_tokio_connection().await?);
        }
        let conn = conn.as_mut().unwrap();

        // wait for the next RemoteMessage containing the broadcast key
        log::debug!("BLPOP on key: {:?}", &self.broadcast_recv_key);
        let (_, bcast_key): (String, String) = cmd("BLPOP")
            .arg(&self.broadcast_recv_key)
            .arg(0)
            .query_async(&mut *conn)
            .await?;
        // log::debug!("Received broadcast key: {:?}", &bcast_key);

        // get the RemoteMessage header and body from redis using GET
        let header: Vec<u8> = conn.get(format!("{}:header", bcast_key)).await.unwrap();
        let payload: Vec<u8> = conn.get(format!("{}:payload", bcast_key)).await.unwrap();
        let msg = RemoteMessage::from((header, payload));
        Ok(msg)
    }
}

impl RedisListBroadcastReceiveProxy {
    pub fn new(
        redis_client: Arc<Client>,
        connection: Arc<Mutex<Option<redis::aio::MultiplexedConnection>>>,
        redis_options: Arc<RedisListOptions>,
        burst_options: Arc<BurstOptions>,
    ) -> Self {
        let broadcast_recv_key = get_broadcast_list_key(
            &redis_options.broadcast_topic_prefix,
            &burst_options.burst_id,
            &burst_options.group_id,
        );
        Self {
            redis_client,
            connection,
            broadcast_recv_key,
        }
    }
}

// Helper functions

async fn send_direct<C>(
    connection: &mut C,
    msg: RemoteMessage,
    source: u32,
    dest: u32,
    redis_options: &RedisListOptions,
    burst_options: &BurstOptions,
) -> Result<()>
where
    C: ConnectionLike + Send,
{
    send_redis(
        connection,
        &msg,
        get_redis_list_key(
            &redis_options.list_key_prefix,
            &burst_options.burst_id,
            source,
            dest,
        ),
    )
    .await
}

async fn send_redis<C>(connection: &mut C, msg: &RemoteMessage, key: String) -> Result<()>
where
    C: ConnectionLike + Send,
{
    let data: [&[u8]; 2] = msg.into();
    let payload = data.concat();
    log::debug!("RPUSH {:?}", key);
    connection.rpush::<_, _, ()>(key, payload).await?;
    Ok(())
}

async fn read_redis<C>(connection: &mut C, key: &str) -> Result<RemoteMessage>
where
    C: ConnectionLike + Send,
{
    log::debug!("BLPOP {:?}", key);
    let (_, payload): (String, Vec<u8>) = cmd("BLPOP")
        .arg(key)
        .arg(0)
        .query_async(connection)
        .await?;
    let msg = RemoteMessage::from(payload);
    Ok(msg)
}

fn get_redis_list_key(
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

fn get_broadcast_list_key(prefix: &str, burst_id: &str, group_id: &str) -> String {
    format!("{}:{}:g{}", prefix, burst_id, group_id)
}
