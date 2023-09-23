mod pool;

use bytes::{Buf, BytesMut};
pub use pool::{allocated_slab_count, SLAB_SIZE};
use std::collections::VecDeque;
use std::sync::{mpsc, Arc};

#[derive(Debug, Default, Clone)]
pub struct Slabytes {
    slabs: VecDeque<Slab>,
}

impl Slabytes {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slabs: VecDeque::with_capacity(match capacity % SLAB_SIZE {
                0 => capacity / SLAB_SIZE,
                _ => capacity / SLAB_SIZE + 1,
            }),
        }
    }

    pub fn append(&mut self, slab: Slab) {
        self.slabs.push_back(slab);
    }

    pub fn from_buf_blocking(buf: &mut impl bytes::Buf) -> Self {
        let mut slabytes = Slabytes::with_capacity(buf.remaining());
        while buf.has_remaining() {
            slabytes.append(Slab::from_buf_blocking(buf));
        }
        slabytes
    }

    pub fn from_std_reader(reader: &mut impl std::io::Read) -> std::io::Result<Self> {
        let mut slabytes = Slabytes::new();
        let mut eof = false;

        loop {
            let mut bytes_mut = BytesMut::zeroed(SLAB_SIZE);

            let mut buf = bytes_mut.as_mut();
            while !buf.is_empty() {
                match reader.read(buf) {
                    Ok(0) => {
                        eof = true;
                        break;
                    }
                    Ok(n) => {
                        let tmp = buf;
                        buf = &mut tmp[n..];
                    }
                    Err(ref err) if err.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            }
            let len = SLAB_SIZE - buf.len();

            if len > 0 {
                let bytes = bytes_mut.freeze().split_to(len);
                slabytes.append(Slab::from_bytes_blocking(bytes));
            }

            if eof {
                break;
            }
        }

        Ok(slabytes)
    }
}

impl From<bytes::Bytes> for Slabytes {
    fn from(mut bytes: bytes::Bytes) -> Self {
        Self::from_buf_blocking(&mut bytes)
    }
}

impl bytes::Buf for Slabytes {
    fn remaining(&self) -> usize {
        self.slabs.iter().map(|slab| slab.remaining()).sum()
    }

    fn chunk(&self) -> &[u8] {
        if self.slabs.is_empty() {
            &[]
        } else {
            self.slabs[0].chunk()
        }
    }

    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 {
            let front = self.slabs.front_mut().expect("cnt too large");
            let front_remaining = front.remaining();
            if front_remaining > cnt {
                front.advance(cnt);
                cnt = 0;
            } else {
                cnt -= front_remaining;
                self.slabs.pop_front();
            }
        }
    }
}

#[cfg(feature = "tokio")]
impl Slabytes {
    pub async fn from_buf(buf: &mut impl bytes::Buf) -> Self {
        let mut slabytes = Slabytes::with_capacity(buf.remaining());
        while buf.has_remaining() {
            slabytes.append(Slab::from_buf(buf).await);
        }
        slabytes
    }

    pub async fn from_bytes(mut bytes: bytes::Bytes) -> Self {
        Self::from_buf(&mut bytes).await
    }

    pub async fn from_async_reader(
        reader: &mut (impl tokio::io::AsyncRead + Unpin),
    ) -> std::io::Result<Slabytes> {
        use std::future::poll_fn;
        use std::pin::Pin;
        use tokio::io::ReadBuf;

        let mut slabytes = Slabytes::new();
        let mut eof = false;

        loop {
            let mut bytes_mut = BytesMut::zeroed(SLAB_SIZE);

            let mut buf = ReadBuf::new(bytes_mut.as_mut());
            while buf.remaining() > 0 {
                let remaining = buf.remaining();
                poll_fn(|cx| Pin::new(&mut *reader).poll_read(cx, &mut buf)).await?;
                if buf.remaining() == remaining {
                    eof = true;
                    break;
                }
            }
            let len = SLAB_SIZE - buf.remaining();

            if len > 0 {
                let bytes = bytes_mut.freeze().split_to(len);
                slabytes.append(Slab::from_bytes(bytes).await);
            }

            if eof {
                break;
            }
        }

        Ok(slabytes)
    }
}

#[cfg(feature = "tokio")]
impl tokio::io::AsyncRead for Slabytes {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
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
pub struct Slab {
    inner: Arc<pool::SlabRef>,
    pos: usize,
}

impl bytes::Buf for Slab {
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

impl Slab {
    pub fn from_buf_blocking(buf: &mut impl bytes::Buf) -> Self {
        assert!(buf.has_remaining());
        let len = std::cmp::min(buf.remaining(), SLAB_SIZE);
        let bytes = buf.copy_to_bytes(len);
        Slab::from_bytes_blocking(bytes)
    }

    fn from_bytes_blocking(bytes: bytes::Bytes) -> Self {
        debug_assert!(!bytes.is_empty());
        debug_assert!(bytes.len() <= SLAB_SIZE);

        let (result_tx, result_rx) = mpsc::channel();

        pool::actor()
            .send(pool::Message::Store { bytes, result_tx })
            .expect("fail to send");

        Self {
            inner: result_rx.recv().expect("fail to recv").unwrap(),
            pos: 0,
        }
    }
}

#[cfg(feature = "tokio")]
impl Slab {
    pub async fn from_buf(buf: &mut impl bytes::Buf) -> Self {
        assert!(buf.has_remaining());
        let len = std::cmp::min(buf.remaining(), SLAB_SIZE);
        let bytes = buf.copy_to_bytes(len);
        Slab::from_bytes(bytes).await
    }

    async fn from_bytes(bytes: bytes::Bytes) -> Self {
        debug_assert!(!bytes.is_empty());
        debug_assert!(bytes.len() <= SLAB_SIZE);

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        pool::actor()
            .send(pool::Message::TokioStore { bytes, result_tx })
            .unwrap();

        Slab {
            inner: result_rx.await.unwrap().unwrap(),
            pos: 0,
        }
    }
}

#[cfg(test)]
mod test;
