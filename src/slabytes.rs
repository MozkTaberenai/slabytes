use crate::DEFAULT_CHUNK_SIZE;
use sharded_slab::{Config, DefaultConfig};
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Slabytes<const CHUNK_SIZE: usize = DEFAULT_CHUNK_SIZE, C: Config = DefaultConfig> {
    pub(crate) chunks: VecDeque<Chunk<CHUNK_SIZE, C>>,
}

impl<const CHUNK_SIZE: usize, C: Config> Slabytes<CHUNK_SIZE, C> {
    pub(crate) fn new() -> Self {
        Self {
            chunks: Default::default(),
        }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            chunks: VecDeque::with_capacity(match capacity % CHUNK_SIZE {
                0 => capacity / CHUNK_SIZE,
                _ => capacity / CHUNK_SIZE + 1,
            }),
        }
    }

    pub(crate) fn append(&mut self, chunk: Chunk<CHUNK_SIZE, C>) {
        self.chunks.push_back(chunk);
    }
}

impl<const CHUNK_SIZE: usize, C: Config> bytes::Buf for Slabytes<CHUNK_SIZE, C> {
    fn remaining(&self) -> usize {
        self.chunks.iter().map(|slab| slab.remaining()).sum()
    }

    fn chunk(&self) -> &[u8] {
        if self.chunks.is_empty() {
            &[]
        } else {
            self.chunks[0].chunk()
        }
    }

    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 {
            let front = self.chunks.front_mut().expect("cnt too large");
            let front_remaining = front.remaining();
            if front_remaining > cnt {
                front.advance(cnt);
                cnt = 0;
            } else {
                cnt -= front_remaining;
                self.chunks.pop_front();
            }
        }
    }
}

#[cfg(feature = "tokio")]
impl<const CHUNK_SIZE: usize, C: Config> tokio::io::AsyncRead for Slabytes<CHUNK_SIZE, C> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use bytes::Buf;

        let mut len = std::cmp::min(self.remaining(), buf.remaining());
        while len > 0 {
            let chunk = self.chunk();
            let chunk_len = chunk.len();
            if len > chunk_len {
                buf.put_slice(chunk);
                self.advance(chunk_len);
                len -= chunk_len;
            } else {
                buf.put_slice(&chunk[..len]);
                self.advance(len);
                len = 0;
            }
        }
        std::task::Poll::Ready(Ok(()))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Chunk<const SIZE: usize, C: Config> {
    inner: Arc<crate::pool::StoredChunkRef<SIZE, C>>,
    pos: usize,
}

impl<const SIZE: usize, C: Config> From<Arc<crate::pool::StoredChunkRef<SIZE, C>>>
    for Chunk<SIZE, C>
{
    fn from(inner: Arc<crate::pool::StoredChunkRef<SIZE, C>>) -> Self {
        Self { inner, pos: 0 }
    }
}

impl<const SIZE: usize, C: Config> bytes::Buf for Chunk<SIZE, C> {
    fn remaining(&self) -> usize {
        self.inner.as_bytes().len() - self.pos
    }

    fn chunk(&self) -> &[u8] {
        &self.inner.as_bytes()[self.pos..]
    }

    fn advance(&mut self, cnt: usize) {
        assert!(self.pos + cnt <= self.inner.as_bytes().len());
        self.pos += cnt;
    }
}
