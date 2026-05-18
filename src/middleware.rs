use crate::{
    chunk_store::chunk_message,
    impl_chainable_setter,
    message_buffer::{LocalMessageBuffer, RemoteMessageBuffer},
    CollectiveType, LocalMessage, MessageMetadata, RemoteMessage, Result,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    sync::Arc,
};

const MB: usize = 1024 * 1024;
const DEFAULT_MESSAGE_CHUNK_SIZE: usize = 1 * MB;

#[async_trait]
pub trait RemoteSendProxy: Send + Sync {
    async fn remote_send(&self, dest: u32, msg: RemoteMessage) -> Result<()>;
}

#[async_trait]
pub trait RemoteReceiveProxy: Send + Sync {
    async fn remote_recv(&self, source: u32) -> Result<RemoteMessage>;
}

pub trait RemoteSendReceiveProxy: RemoteSendProxy + RemoteReceiveProxy + Send + Sync {}

#[async_trait]
pub trait LocalSendProxy<T>: Send + Sync
where
    T: From<Bytes> + Into<Bytes>,
{
    async fn local_send(&self, dest: u32, msg: LocalMessage<T>) -> Result<()>;
}

#[async_trait]
pub trait LocalReceiveProxy<T>: Send + Sync
where
    T: From<Bytes> + Into<Bytes>,
{
    async fn local_recv(&self, source: u32) -> Result<LocalMessage<T>>;
}

pub trait LocalSendReceiveProxy<T>: LocalSendProxy<T> + LocalReceiveProxy<T> + Send + Sync
where
    T: From<Bytes> + Into<Bytes>,
{
}

#[async_trait]
pub trait RemoteBroadcastSendProxy: Send + Sync {
    async fn remote_broadcast_send(&self, msg: RemoteMessage) -> Result<()>;
}

#[async_trait]
pub trait RemoteBroadcastReceiveProxy: Send + Sync {
    async fn remote_broadcast_recv(&self) -> Result<RemoteMessage>;
}

pub trait RemoteBroadcastProxy:
    RemoteBroadcastSendProxy + RemoteBroadcastReceiveProxy + Send + Sync
{
}

#[async_trait]
pub trait LocalBroadcastSendProxy<T>: Send + Sync
where
    T: From<Bytes> + Into<Bytes>,
{
    async fn local_broadcast_send(&self, msg: LocalMessage<T>) -> Result<()>;
}

#[async_trait]
pub trait LocalBroadcastReceiveProxy<T>: Send + Sync
where
    T: From<Bytes> + Into<Bytes>,
{
    async fn local_broadcast_recv(&self) -> Result<LocalMessage<T>>;
}

pub trait LocalBroadcastProxy<T>:
    LocalBroadcastSendProxy<T> + LocalBroadcastReceiveProxy<T> + Send + Sync
where
    T: From<Bytes> + Into<Bytes>,
{
}

#[async_trait]
pub trait RemoteSendReceiveFactory<T>: Send + Sync {
    async fn create_remote_proxies(
        burst_options: Arc<BurstOptions>,
        options: T,
    ) -> Result<
        HashMap<
            u32,
            (
                Box<dyn RemoteSendReceiveProxy>,
                Box<dyn RemoteBroadcastProxy>,
            ),
        >,
    >;
}

#[async_trait]
pub trait SendReceiveLocalFactory<O, T>: Send + Sync {
    async fn create_local_proxies(
        burst_options: Arc<BurstOptions>,
        options: O,
    ) -> Result<
        HashMap<
            u32,
            (
                Box<dyn LocalSendReceiveProxy<T>>,
                Box<dyn LocalBroadcastProxy<T>>,
            ),
        >,
    >;
}

#[derive(Clone, Debug)]
pub struct BurstOptions {
    pub burst_id: String,
    pub burst_size: u32,
    pub group_ranges: HashMap<String, HashSet<u32>>,
    pub group_id: String,
    pub enable_message_chunking: bool,
    pub message_chunk_size: usize,
}

#[derive(Clone, Debug)]
pub struct BurstInfo {
    pub burst_id: String,
    pub burst_size: u32,
    pub group_ranges: HashMap<String, HashSet<u32>>,
    pub worker_id: u32,
    pub group_id: String,
}

impl BurstOptions {
    pub fn new(
        burst_size: u32,
        group_ranges: HashMap<String, HashSet<u32>>,
        group_id: String,
    ) -> Self {
        Self {
            burst_id: "default".to_string(),
            burst_size,
            group_ranges,
            group_id,
            enable_message_chunking: false,
            message_chunk_size: DEFAULT_MESSAGE_CHUNK_SIZE,
        }
    }

    impl_chainable_setter!(burst_id, String);
    impl_chainable_setter!(burst_size, u32);
    impl_chainable_setter!(group_ranges, HashMap<String, HashSet<u32>>);
    impl_chainable_setter!(group_id, String);
    impl_chainable_setter!(enable_message_chunking, bool);
    impl_chainable_setter!(message_chunk_size, usize);

    pub fn build(&self) -> Self {
        self.clone()
    }
}

pub struct BurstMiddleware<T> {
    options: Arc<BurstOptions>,

    worker_id: u32,
    group: HashSet<u32>,
    group_worker_leader: u32,

    local_send_receive: Arc<dyn LocalSendReceiveProxy<T>>,
    remote_send_receive: Arc<dyn RemoteSendReceiveProxy>,

    local_broadcast: Arc<dyn LocalBroadcastProxy<T>>,
    remote_broadcast: Arc<dyn RemoteBroadcastProxy>,

    collective_counters: HashMap<CollectiveType, u32>,
    send_counters: HashMap<u32, u32>,
    receive_counters: HashMap<u32, u32>,

    remote_message_buffer: RemoteMessageBuffer,
    local_message_buffer: LocalMessageBuffer<T>,
    enable_message_chunking: bool,
    message_chunk_size: usize,
}

impl<T> BurstMiddleware<T>
where
    T: From<Bytes> + Into<Bytes> + Send + Clone + 'static,
{
    pub async fn create_proxies<LocalImpl, RemoteImpl, LocalOptions, RemoteOptions>(
        options: BurstOptions,
        local_impl_options: LocalOptions,
        remote_impl_options: RemoteOptions,
    ) -> Result<HashMap<u32, Self>>
    where
        LocalImpl: SendReceiveLocalFactory<LocalOptions, T>,
        RemoteImpl: RemoteSendReceiveFactory<RemoteOptions>,
        LocalOptions: Send + Sync,
        RemoteOptions: Send + Sync,
    {
        log::debug!("Creating proxies {:?}", options);
        let options = Arc::new(options);
        let current_group = options.group_ranges.get(&options.group_id).unwrap();

        let mut local_proxies =
            LocalImpl::create_local_proxies(options.clone(), local_impl_options).await?;
        let mut remote_proxies =
            RemoteImpl::create_remote_proxies(options.clone(), remote_impl_options).await?;

        let mut proxies = HashMap::new();

        for id in current_group {
            let (local_direct_proxy, local_broadcast_proxy) = local_proxies.remove(id).unwrap();
            let (remote_direct_proxy, remote_broadcast_proxy) = remote_proxies.remove(id).unwrap();
            let proxy = BurstMiddleware::new(
                options.clone(),
                local_direct_proxy,
                remote_direct_proxy,
                local_broadcast_proxy,
                remote_broadcast_proxy,
                *id,
                current_group.clone(),
            );
            proxies.insert(*id, proxy);
        }

        Ok(proxies)
    }

    pub fn new(
        options: Arc<BurstOptions>,
        local_send_receive: Box<dyn LocalSendReceiveProxy<T>>,
        remote_send_receive: Box<dyn RemoteSendReceiveProxy>,
        local_broadcast: Box<dyn LocalBroadcastProxy<T>>,
        remote_broadcast: Box<dyn RemoteBroadcastProxy>,
        worker_id: u32,
        group: HashSet<u32>,
    ) -> Self {
        let enable_message_chunking = options.enable_message_chunking;
        let message_chunk_size = options.message_chunk_size;

        // create counters
        let counters = [
            CollectiveType::Broadcast,
            CollectiveType::Gather,
            CollectiveType::Scatter,
            CollectiveType::AllToAll,
        ]
        .into_iter()
        .map(|c| (c, 0))
        .collect();
        let send_counters: HashMap<u32, u32> = (0..options.burst_size).map(|id| (id, 0)).collect();
        let receive_counters = send_counters.clone();

        let remote_message_buffer = RemoteMessageBuffer::new(options.message_chunk_size);

        // The worker with the lowest id in the group is the group leader
        let group_worker_leader = *group.iter().min().unwrap();

        Self {
            options,
            worker_id,
            group,
            group_worker_leader,
            local_send_receive: local_send_receive.into(),
            remote_send_receive: remote_send_receive.into(),
            local_broadcast: local_broadcast.into(),
            remote_broadcast: remote_broadcast.into(),
            collective_counters: counters,
            send_counters,
            receive_counters,
            remote_message_buffer,
            local_message_buffer: LocalMessageBuffer::new(),
            enable_message_chunking,
            message_chunk_size,
        }
    }

    pub async fn send(&mut self, dest: u32, data: T) -> Result<()> {
        let counter = Self::get_counter(&self.send_counters, &dest)?;
        let msg = LocalMessage {
            metadata: MessageMetadata {
                sender_id: self.worker_id,
                chunk_id: 0,
                num_chunks: 1,
                counter,
                collective: CollectiveType::Direct,
            },
            data,
        };
        self.send_message(dest, msg).await?;
        Self::increment_counter(&mut self.send_counters, &dest)?;
        Ok(())
    }

    pub async fn recv(&mut self, from: u32) -> Result<LocalMessage<T>> {
        let counter = Self::get_counter(&self.receive_counters, &from)?;
        let msg = self
            .get_message(from, &CollectiveType::Direct, counter)
            .await?;
        Self::increment_counter(&mut self.receive_counters, &from)?;
        Ok(LocalMessage::from(msg))
    }

    pub async fn broadcast(&mut self, data: Option<T>, root: u32) -> Result<LocalMessage<T>> {
        let counter = Self::get_counter(&self.collective_counters, &CollectiveType::Broadcast)?;

        if self.worker_id == root {
            // The root worker sends the broadcast message
            let data = data.expect("Root worker must send data");

            let msg = LocalMessage {
                metadata: MessageMetadata {
                    sender_id: self.worker_id,
                    chunk_id: 0,
                    num_chunks: 1,
                    counter,
                    collective: CollectiveType::Broadcast,
                },
                data,
            };

            // Send the message to the local channel for the local group
            let local_fut = self.local_broadcast.local_broadcast_send(msg.clone());

            if self.enable_message_chunking {
                let remote_msg = RemoteMessage::from(msg);
                // do remote send with chunking if enabled
                let chunked_messages = chunk_message(&remote_msg, self.message_chunk_size);
                log::debug!("Chunked message in {} parts", chunked_messages.len());
                for msg in chunked_messages {
                    Self::remote_broadcast_send(Arc::clone(&self.remote_broadcast), msg).await?;
                }
            } else {
                // do remote send in one chunk
                self.remote_broadcast
                    .remote_broadcast_send(RemoteMessage::from(msg))
                    .await?;
            }

            local_fut.await?;
        } else {
            // For non-root workers, check if the root worker is remote
            // and if this worker is the group leader
            if !self.group.contains(&root) && self.worker_id == self.group_worker_leader {
                // Only the group leader receives the broadcast message via the remote channel
                // And it will send it to the rest of the group via the local channel
                let msg = self.get_broadcast_message(root, counter).await?;
                // let msg = self.remote_broadcast.broadcast_recv().await?;
                self.local_broadcast.local_broadcast_send(msg).await?;
            }
        }

        // Eventually all workers (root, local and remote)
        // will receive the broadcast message via the local channel
        let msg = self.local_broadcast.local_broadcast_recv().await?;
        // Increment broadcast counter
        Self::increment_counter(&mut self.collective_counters, &CollectiveType::Broadcast)?;

        Ok(msg)
    }

    pub async fn gather(&mut self, data: T, root: u32) -> Result<Option<Vec<LocalMessage<T>>>> {
        let counter = Self::get_counter(&self.collective_counters, &CollectiveType::Gather)?;

        let mut result = None;
        let msg = LocalMessage {
            metadata: MessageMetadata {
                sender_id: self.worker_id,
                chunk_id: 0,
                num_chunks: 1,
                counter,
                collective: CollectiveType::Gather,
            },
            data,
        };

        if self.worker_id == root {
            let mut gathered = Vec::with_capacity(self.options.burst_size as usize);
            gathered.push(msg);

            // create a new hashset with all worker ids except self
            let sender_ids = HashSet::<u32>::from_iter(0..self.options.burst_size)
                .difference(&HashSet::from_iter(vec![self.worker_id]))
                .copied()
                .collect::<HashSet<u32>>();
            let messages = self
                .get_messages(&CollectiveType::Gather, counter, sender_ids)
                .await?;

            gathered.extend(messages);
            gathered.sort_by_key(|msg| msg.metadata.sender_id);

            result = Some(gathered)
        } else {
            self.send_message(root, msg).await?;
        }

        Self::increment_counter(&mut self.collective_counters, &CollectiveType::Gather)?;
        Ok(result)
    }

    pub async fn scatter(&mut self, data: Option<Vec<T>>, root: u32) -> Result<LocalMessage<T>> {
        let counter = Self::get_counter(&self.collective_counters, &CollectiveType::Scatter)?;

        let result;

        if self.worker_id == root {
            let data = data.expect("Root worker must send data");
            if data.len() != (self.options.burst_size as usize) {
                return Err("Data size must be equal to burst size".into());
            }

            result = LocalMessage {
                metadata: MessageMetadata {
                    sender_id: root,
                    chunk_id: 0,
                    num_chunks: 1,
                    counter,
                    collective: CollectiveType::Scatter,
                },
                data: data[root as usize].clone(),
            };

            let messages = data
                .into_iter()
                .enumerate()
                .filter(|(to, _)| *to != root as usize)
                .map(|(to, data)| {
                    (
                        to as u32,
                        LocalMessage {
                            metadata: MessageMetadata {
                                sender_id: root,
                                chunk_id: 0,
                                num_chunks: 1,
                                counter,
                                collective: CollectiveType::Scatter,
                            },
                            data,
                        },
                    )
                })
                .collect();

            self.send_messages(messages).await?;
        } else {
            result = LocalMessage::from(
                self.get_message(root, &CollectiveType::Scatter, counter)
                    .await?,
            );
        }

        // Increment scatter counter
        Self::increment_counter(&mut self.collective_counters, &CollectiveType::Scatter)?;
        Ok(result)
    }

    pub async fn all_to_all(&mut self, data: Vec<T>) -> Result<Vec<LocalMessage<T>>> {
        let counter = Self::get_counter(&self.collective_counters, &CollectiveType::AllToAll)?;

        if data.len() != (self.options.burst_size as usize) {
            return Err("Data size must be equal to burst size".into());
        }

        // first send to all workers
        let send_messages = data
            .into_iter()
            .enumerate()
            .map(|(to, data)| {
                (
                    to as u32,
                    LocalMessage {
                        metadata: MessageMetadata {
                            sender_id: self.worker_id,
                            chunk_id: 0,
                            num_chunks: 1,
                            counter,
                            collective: CollectiveType::AllToAll,
                        },
                        data,
                    },
                )
            })
            .collect();

        self.send_messages(send_messages).await?;

        let mut received_messages = self
            .get_messages(
                &CollectiveType::AllToAll,
                counter,
                HashSet::from_iter(0..self.options.burst_size),
            )
            .await?;

        // Sort by sender_id
        received_messages.sort_by_key(|msg| msg.metadata.sender_id);

        // Increment all_to_all counter
        Self::increment_counter(&mut self.collective_counters, &CollectiveType::AllToAll)?;
        Ok(received_messages)
    }

    pub async fn reduce(&mut self, data: T, op: fn(T, T) -> T) -> Result<Option<T>> {
        assert!(self.options.burst_size.is_power_of_two());
        assert!(self.group.len().is_power_of_two());

        let reduce_levels = (self.options.burst_size as f64).log2() as u32;
        let mut data = data;

        for level in 0..reduce_levels {
            log::debug!("[Worker {}] Reduce level {}", self.worker_id, level);
            let worker_offset = 1 << level;
            if self.worker_id % (worker_offset << 1) == 0 {
                let partner = self.worker_id + worker_offset;
                log::debug!(
                    "[Worker {}] Reduce ==> Get message from partner {}",
                    self.worker_id,
                    partner,
                );

                let msg = self
                    .get_message(partner, &CollectiveType::Direct, 0)
                    .await?;
                let partner_data = T::from(msg.data);
                let reduced_data = op(data, partner_data);
                data = reduced_data;
            } else {
                let partner = self.worker_id - worker_offset;
                log::debug!(
                    "[Worker {}] Reduce ==> Send message to partner {}",
                    self.worker_id,
                    partner,
                );
                let msg = LocalMessage {
                    metadata: MessageMetadata {
                        sender_id: self.worker_id,
                        chunk_id: 0,
                        num_chunks: 1,
                        counter: 0,
                        collective: CollectiveType::Direct,
                    },
                    data,
                };
                self.send_message(partner, msg).await?;

                return Ok(None);
            }
        }

        Ok(Some(data))
    }

    pub fn info(&self) -> BurstInfo {
        BurstInfo {
            burst_id: self.options.burst_id.clone(),
            burst_size: self.options.burst_size,
            group_ranges: self.options.group_ranges.clone(),
            worker_id: self.worker_id,
            group_id: self.options.group_id.clone(),
        }
    }

    async fn send_message(&self, to: u32, msg: LocalMessage<T>) -> Result<()> {
        if to >= self.options.burst_size {
            return Err("worker with id {} does not exist".into());
        }

        if self.group.contains(&to) {
            // do local send always in one chunk
            log::debug!(
                "[Worker {}] Sending message to local worker {}",
                self.worker_id,
                to
            );
            return self.local_send_receive.local_send(to, msg).await;
        } else if self.enable_message_chunking {
            // do remote send with chunking if enabled
            let chunked_messages =
                chunk_message(&RemoteMessage::from(msg), self.message_chunk_size);
            log::debug!("Chunked message in {} parts", chunked_messages.len());
            for msg in chunked_messages {
                Self::remote_send(to, Arc::clone(&self.remote_send_receive), msg).await?;
            }
        } else {
            // do remote send in one chunk
            return self
                .remote_send_receive
                .remote_send(to, RemoteMessage::from(msg))
                .await;
        }
        Ok(())
    }

    async fn send_messages(&self, msgs: Vec<(u32, LocalMessage<T>)>) -> Result<()> {
        let mut futures: FuturesUnordered<_> = FuturesUnordered::new();

        let (local_msg, remote_msg): (_, Vec<_>) =
            msgs.into_iter().partition(|(to, _)| self.is_local(to));

        // Send local messages
        futures.extend(
            local_msg
                .into_iter()
                .map(|(to, msg)| Self::local_send(to, Arc::clone(&self.local_send_receive), msg))
                .map(tokio::spawn),
        );

        // Send remote messages sequentially. Chunked concurrent connection setup
        // hangs in CloudLab with Redis-backed collectives.
        for (to, msg) in remote_msg {
            let remote_message = RemoteMessage::from(msg);
            if self.enable_message_chunking {
                let chunked_messages = chunk_message(&remote_message, self.message_chunk_size);
                log::debug!("Chunked message in {} parts", chunked_messages.len());
                for msg in chunked_messages {
                    Self::remote_send(to, Arc::clone(&self.remote_send_receive), msg).await?;
                }
            } else {
                Self::remote_send(to, Arc::clone(&self.remote_send_receive), remote_message)
                    .await?;
            }
        }

        futures::future::try_join_all(futures).await?;
        Ok(())
    }

    async fn get_message(
        &mut self,
        from: u32,
        collective: &CollectiveType,
        counter: u32,
    ) -> Result<RemoteMessage> {
        if from >= self.options.burst_size {
            return Err("worker with id {} does not exist".into());
        }
        log::debug!(
            "[Worker {:?}] get_message: from => {:?} collective => {:?} counter => {:?}",
            self.worker_id,
            from,
            collective,
            counter
        );

        if self.is_local(&from) {
            loop {
                // Check if complete message is in buffer
                if let Some(msg) = self.local_message_buffer.get(from, *collective, counter) {
                    return Ok(RemoteMessage::from(msg));
                };

                let msg = self.local_send_receive.local_recv(from).await?;
                log::debug!("[Worker {}] received message {:?}", self.worker_id, msg);

                // Check if this is the message we are waiting for
                if msg.metadata.counter == counter
                    && msg.metadata.collective == *collective
                    && msg.metadata.sender_id == from
                {
                    return Ok(RemoteMessage::from(msg));
                } else {
                    self.local_message_buffer.insert(msg);
                    log::debug!(
                        "[Worker {}] Put message in buffer, get next message",
                        self.worker_id
                    )
                }
            }
        } else {
            loop {
                // Check if complete message is in buffer
                if let Some(msg) = self.remote_message_buffer.get(&from, collective, &counter) {
                    return Ok(msg);
                };

                let msg = self.remote_send_receive.remote_recv(from).await?;
                log::debug!("[Worker {}] received message {:?}", self.worker_id, msg);

                // Check if this is the message we are waiting for
                if msg.metadata.counter == counter
                    && msg.metadata.collective == *collective
                    && msg.metadata.sender_id == from
                {
                    if msg.metadata.num_chunks == 1 {
                        return Ok(msg);
                    } else {
                        // If message is chunked, we need to receive all chunks
                        let complete_msg = self.get_complete_message(msg).await?;
                        return Ok(complete_msg);
                    }
                } else {
                    // got a message with another collective or counter, loop until we receive what we want
                    self.remote_message_buffer.insert(msg);
                    log::debug!(
                        "[Worker {}] Put message in buffer, get next message",
                        self.worker_id
                    )
                }
            }
        }
    }

    async fn get_complete_message(&mut self, msg_chunk: RemoteMessage) -> Result<RemoteMessage> {
        if !self.enable_message_chunking || (msg_chunk.metadata.num_chunks) < 2 {
            panic!("get_complete_message called with non-chunked message")
        }
        assert!(!self.is_local(&msg_chunk.metadata.sender_id));

        let from = msg_chunk.metadata.sender_id;
        let collective = msg_chunk.metadata.collective;
        let counter = msg_chunk.metadata.counter;
        let num_chunks = msg_chunk.metadata.num_chunks;

        // put first chunk into buffer, we will put the rest as we receive them
        self.remote_message_buffer.insert(msg_chunk);

        // Check if we have some chunks already in the buffer
        let missing_chunks =
            match self
                .remote_message_buffer
                .num_chunks_stored(&from, &collective, &counter)
            {
                Some(n) => num_chunks - n, // we will have at least 1 chunk in the buffer
                None => panic!("Inserted first chunk into buffer but now it's gone"), // we should have at least the first chunk in the buffer
            };

        if missing_chunks == 0 {
            // we already have all chunks in the buffer, this was the last one
            if let Some(msg) = self.remote_message_buffer.get(&from, &collective, &counter) {
                return Ok(msg);
            } else {
                // something went wrong
                return Err("There are no missing chunks but the message is not complete".into());
            }
        }

        log::debug!(
            "[Worker {}] Waiting for {} missing chunks for message (collective={}, counter={}, sender={})",
            self.worker_id,
            missing_chunks,
            collective,
            counter,
            from
        );

        // we will expect, at least, N - 1 more messages, where N is the number of chunks
        let mut futures = (0..missing_chunks)
            .map(|_| Self::remote_recv(from, Arc::clone(&self.remote_send_receive)))
            .map(tokio::spawn)
            .collect::<FuturesUnordered<_>>();

        // Loop until all chunks are received
        while let Some(fut) = futures.next().await {
            match fut {
                Ok((id, fut_res)) => {
                    let msg = fut_res?;
                    log::debug!("[Worker {}] received message {:?}", self.worker_id, msg);

                    if msg.metadata.sender_id != id
                        || msg.metadata.counter != counter
                        || msg.metadata.collective != collective
                    {
                        // we got a message with another collective, counter or sender, we will need to receive yet another message
                        futures.push(tokio::spawn(Self::remote_recv(
                            id,
                            Arc::clone(&self.remote_send_receive),
                        )));
                    }

                    // Put msg into buffer, either if it's a chunk or another unrelated message
                    self.remote_message_buffer.insert(msg);
                }
                Err(e) => {
                    log::error!("Error receiving message {:?}", e);
                }
            }
        }

        log::debug!("[Worker {}] received all chunks", self.worker_id);

        // at this point, we should have all chunks in the buffer
        if let Some(msg) = self.remote_message_buffer.get(&from, &collective, &counter) {
            Ok(msg)
        } else {
            // something went wrong
            // TODO try receiving more messages?
            Err("Waited for all chunks but some are missing".into())
        }
    }

    async fn get_messages(
        &mut self,
        collective: &CollectiveType,
        counter: u32,
        sender_ids: HashSet<u32>,
    ) -> Result<Vec<LocalMessage<T>>> {
        let mut messages: Vec<LocalMessage<T>> = Vec::with_capacity(sender_ids.len());

        // Retrieve pending messages from buffer
        let mut sender_ids_found: HashSet<u32> = HashSet::new();
        for from in sender_ids.iter() {
            if self.is_local(from) {
                if let Some(msg) = self.local_message_buffer.get(*from, *collective, counter) {
                    messages.push(msg);
                    sender_ids_found.insert(*from);
                }
            } else if let Some(msg) = self.remote_message_buffer.get(from, collective, &counter) {
                messages.push(LocalMessage::from(msg));
                sender_ids_found.insert(*from);
            }
        }

        // Receive missing local messages
        let mut futures = sender_ids
            .difference(&sender_ids_found)
            .filter(|&id| self.is_local(id))
            .map(|id| {
                let proxy = Arc::clone(&self.local_send_receive);
                Self::local_recv(*id, proxy)
            })
            .map(tokio::spawn)
            .collect::<FuturesUnordered<_>>();

        while let Some(fut) = futures.next().await {
            match fut {
                Ok((id, fut_res)) => {
                    let msg = fut_res?;
                    if msg.metadata.counter == counter && msg.metadata.collective == *collective {
                        messages.push(msg);
                    } else {
                        self.local_message_buffer.insert(msg);
                        // spawn a new task to receive another message
                        futures.push(tokio::spawn(Self::local_recv(
                            id,
                            Arc::clone(&self.local_send_receive),
                        )));
                    }
                }
                Err(e) => {
                    log::error!("Error receiving message {:?}", e);
                }
            }
        }

        // Receive missing remote messages
        let mut futures = sender_ids
            .difference(&sender_ids_found)
            .filter(|&id| !self.is_local(id))
            .map(|id| {
                let proxy = Arc::clone(&self.remote_send_receive);
                Self::remote_recv(*id, proxy)
            })
            .map(tokio::spawn)
            .collect::<FuturesUnordered<_>>();

        // Loop until all messages are received
        while let Some(fut) = futures.next().await {
            match fut {
                Ok((id, fut_res)) => {
                    let msg = fut_res?;
                    if msg.metadata.num_chunks == 1
                        && msg.metadata.counter == counter
                        && msg.metadata.collective == *collective
                    {
                        messages.push(LocalMessage::from(msg));
                    } else {
                        let sender_id = msg.metadata.sender_id;

                        // Received either a partial message, or other collective message
                        // put it into buffer
                        self.remote_message_buffer.insert(msg);

                        // check if we have a complete message in the buffer
                        if let Some(msg) = self
                            .remote_message_buffer
                            .get(&sender_id, collective, &counter)
                        {
                            messages.push(LocalMessage::from(msg));
                        } else {
                            // if not, spawn a new task to receive another message
                            futures.push(tokio::spawn(Self::remote_recv(
                                id,
                                Arc::clone(&self.remote_send_receive),
                            )));
                        };
                    }
                }
                Err(e) => {
                    log::error!("Error receiving message {:?}", e);
                }
            }
        }

        Ok(messages)
    }

    async fn get_broadcast_message(&mut self, root: u32, counter: u32) -> Result<LocalMessage<T>> {
        // Check if complete message is in buffer
        if let Some(msg) =
            self.remote_message_buffer
                .get(&root, &CollectiveType::Broadcast, &counter)
        {
            return Ok(LocalMessage::from(msg));
        };

        // Loop until we receive a message with the counter we are waiting for
        loop {
            let msg = Self::remote_broadcast_recv(Arc::clone(&self.remote_broadcast)).await?;
            log::debug!(
                "[Worker {}] received broadcast message {:?}",
                self.worker_id,
                msg
            );

            // Check if this is the message we are waiting for
            if msg.metadata.counter == counter {
                if msg.metadata.num_chunks == 1 {
                    return Ok(LocalMessage::from(msg));
                }

                // If message is chunked, we need to receive all chunks
                if !self.enable_message_chunking || (msg.metadata.num_chunks) < 2 {
                    panic!("get_complete_message called with non-chunked message")
                }

                // put first chunk into buffer, we will put the rest as we receive them
                let num_chunks = msg.metadata.num_chunks;
                self.remote_message_buffer.insert(msg);

                // Check if we have some chunks already in the buffer
                let missing_chunks = match self.remote_message_buffer.num_chunks_stored(
                    &root,
                    &CollectiveType::Broadcast,
                    &counter,
                ) {
                    Some(n) => num_chunks - n, // we will have at least 1 chunk in the buffer
                    None => panic!("Inserted first chunk into buffer but now it's gone"), // we should have at least the first chunk in the buffer
                };

                if missing_chunks == 0 {
                    // we already have all chunks in the buffer, this was the last one
                    if let Some(msg) =
                        self.remote_message_buffer
                            .get(&root, &CollectiveType::Broadcast, &counter)
                    {
                        return Ok(LocalMessage::from(msg));
                    } else {
                        // something went wrong
                        return Err(
                            "There are no missing chunks but the message is not complete".into(),
                        );
                    }
                }

                log::debug!(
                    "Waiting for {} missing chunks for broadcast message (counter={})",
                    missing_chunks,
                    counter,
                );

                // we will expect, at least, N - 1 more messages, where N is the number of chunks
                let mut futures = (0..missing_chunks)
                    .map(|_| Self::remote_broadcast_recv(Arc::clone(&self.remote_broadcast)))
                    .map(tokio::spawn)
                    .collect::<FuturesUnordered<_>>();

                // Loop until all chunks are received
                while let Some(fut) = futures.next().await {
                    match fut {
                        Ok(fut_res) => {
                            let msg = fut_res?;
                            log::debug!("Received broadcast message {:?}", msg);

                            if msg.metadata.counter != counter {
                                // we got a message with another counter
                                // we will need to receive yet another message
                                futures.push(tokio::spawn(Self::remote_broadcast_recv(
                                    Arc::clone(&self.remote_broadcast),
                                )));
                            }

                            // Put msg into buffer, either if it's a chunk or another unrelated message
                            self.remote_message_buffer.insert(msg);
                        }
                        Err(e) => {
                            log::error!("Error receiving message {:?}", e);
                        }
                    }
                }

                log::debug!("Received all broadcast chunks for counter={}", counter);

                // at this point, we should have all chunks in the buffer
                if let Some(msg) =
                    self.remote_message_buffer
                        .get(&root, &CollectiveType::Broadcast, &counter)
                {
                    return Ok(LocalMessage::from(msg));
                } else {
                    // something went wrong
                    // TODO try receiving more messages?
                    return Err("Waited for all chunks but some are missing".into());
                }
            }
            // we got a message with another counter, put it into buffer and get next message
            self.remote_message_buffer.insert(msg);
        }
    }

    fn is_local(&self, dest: &u32) -> bool {
        self.group.contains(dest)
    }

    async fn remote_recv(
        from: u32,
        proxy: Arc<dyn RemoteSendReceiveProxy>,
    ) -> (u32, Result<RemoteMessage>) {
        (from, proxy.remote_recv(from).await)
    }

    async fn remote_send(
        to: u32,
        proxy: Arc<dyn RemoteSendReceiveProxy>,
        msg: RemoteMessage,
    ) -> Result<()> {
        proxy.remote_send(to, msg).await
    }

    async fn local_recv(
        from: u32,
        proxy: Arc<dyn LocalSendReceiveProxy<T>>,
    ) -> (u32, Result<LocalMessage<T>>) {
        (from, proxy.local_recv(from).await)
    }

    async fn local_send(
        to: u32,
        proxy: Arc<dyn LocalSendReceiveProxy<T>>,
        msg: LocalMessage<T>,
    ) -> Result<()> {
        proxy.local_send(to, msg).await
    }

    async fn remote_broadcast_send(
        proxy: Arc<dyn RemoteBroadcastProxy>,
        msg: RemoteMessage,
    ) -> Result<()> {
        proxy.remote_broadcast_send(msg).await
    }

    async fn remote_broadcast_recv(proxy: Arc<dyn RemoteBroadcastProxy>) -> Result<RemoteMessage> {
        proxy.remote_broadcast_recv().await
    }

    fn get_counter<U>(map: &HashMap<U, u32>, key: &U) -> Result<u32>
    where
        U: Eq + PartialEq + Hash + Debug,
    {
        map.get(key)
            .copied()
            .ok_or(format!("Counter not found for key {:?}", key).into())
    }

    fn increment_counter<U>(map: &mut HashMap<U, u32>, key: &U) -> Result<()>
    where
        U: Eq + PartialEq + Hash + Debug,
    {
        let v = map
            .get_mut(key)
            .ok_or(format!("Counter not found for key {:?}", key))?;
        *v += 1;
        Ok(())
    }
}
