use bytes::{Bytes, BytesMut};

use crate::{MessageMetadata, RemoteMessage};

pub struct BytesMutChunkedMessageBody {
    chunked_buffs: Vec<BytesMut>,
    buff: BytesMut,
    bytes_written: usize,
    num_chunks: u32,
    num_chunks_stored: u32,
}

impl BytesMutChunkedMessageBody {
    pub fn new(num_chunks: u32, chunk_size: usize) -> Self {
        let buff_size = (num_chunks as usize) * chunk_size;
        let mut buff = BytesMut::with_capacity(buff_size);
        unsafe {
            buff.set_len(buff_size);
        }
        let chunked_buffs: Vec<_> = (0..num_chunks).map(|_| buff.split_to(chunk_size)).collect();
        Self {
            chunked_buffs,
            buff,
            bytes_written: 0,
            num_chunks,
            num_chunks_stored: 0,
        }
    }

    pub fn insert(&mut self, chunk_id: u32, chunk: Bytes) {
        // log::debug!("Inserting chunk {} of {}", chunk_id, self.num_chunks);

        self.bytes_written += chunk.len();
        let chunk_buff = &mut self.chunked_buffs[chunk_id as usize];
        if chunk.len() != chunk_buff.len() {
            // Chunk is smaller than the buffer size and copy_from_slice requires both slices to be the same size
            // Copy the chunk into a new buffer with the desired size, with zero-paddin, and then copy that buffer into the chunk buffer
            let mut rechunk = BytesMut::with_capacity(chunk_buff.len());
            rechunk.extend_from_slice(chunk.as_ref());
            rechunk.resize(chunk_buff.len(), 0);
            chunk_buff.copy_from_slice(&rechunk);
        } else {
            chunk_buff.copy_from_slice(chunk.as_ref());
        }

        self.num_chunks_stored += 1;
        // log::debug!(
        //     "Received {} of {} chunks",
        //     self.num_chunks_stored,
        //     self.num_chunks
        // );
    }

    pub fn is_complete(&self) -> bool {
        self.num_chunks_stored == self.num_chunks
    }

    pub fn get_complete_body(mut self) -> Bytes {
        assert!(self.is_complete(), "Message is not complete");

        for chunk in self.chunked_buffs.into_iter() {
            self.buff.unsplit(chunk);
        }
        self.buff.truncate(self.bytes_written);
        self.buff.freeze()
    }

    pub fn get_num_chunks_stored(&self) -> u32 {
        self.num_chunks_stored
    }
}

pub fn chunk_message(msg: &RemoteMessage, max_chunk_size: usize) -> Vec<RemoteMessage> {
    let mut chunks = Vec::new();
    let mut body = msg.data.clone();
    while !body.is_empty() {
        let chunk = body.split_to(std::cmp::min(body.len(), max_chunk_size));
        chunks.push(chunk);
    }

    // Empty payload: emit one zero-byte chunk so receiver sees num_chunks=1
    // and can reassemble an empty body instead of blocking forever on BLPOP.
    if chunks.is_empty() {
        chunks.push(body);
    }

    let num_chunks = chunks.len();
    chunks
        .into_iter()
        .enumerate()
        .map(|(i, data)| RemoteMessage {
            metadata: MessageMetadata {
                sender_id: msg.metadata.sender_id,
                chunk_id: i as u32,
                num_chunks: num_chunks as u32,
                counter: msg.metadata.counter,
                collective: msg.metadata.collective,
            },
            data,
        })
        .collect()
}
