use crate::{Slabytes, DEFAULT_CHUNK_SIZE};
use rustc_hash::FxHashMap;
use sharded_slab::pool::OwnedRef;
use sharded_slab::{Config, DefaultConfig};
use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};

#[derive(Debug)]
pub struct Pool<const CHUNK_SIZE: usize = DEFAULT_CHUNK_SIZE, C: Config = DefaultConfig> {
    inner: Arc<sharded_slab::Pool<Chunk<CHUNK_SIZE>, C>>,
    crc_map: Arc<RwLock<FxHashMap<u32, WeakChunkRefs<CHUNK_SIZE, C>>>>,
    chunk_count: AtomicUsize,
}

#[derive(Debug)]
enum WeakChunkRefs<const SIZE: usize, C: Config> {
    One(Weak<StoredChunkRef<SIZE, C>>),
    Many(Vec<Weak<StoredChunkRef<SIZE, C>>>),
}

impl<const CHUNK_SIZE: usize> Pool<CHUNK_SIZE> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(sharded_slab::Pool::<Chunk<CHUNK_SIZE>>::new()),
            crc_map: Default::default(),
            chunk_count: Default::default(),
        })
    }

    pub fn new_with_config<C: Config>() -> Arc<Pool<CHUNK_SIZE, C>> {
        Arc::new(Pool {
            inner: Arc::new(sharded_slab::Pool::<Chunk<CHUNK_SIZE>>::new_with_config::<C>()),
            crc_map: Default::default(),
            chunk_count: Default::default(),
        })
    }
}

impl<const CHUNK_SIZE: usize, C: Config> Pool<CHUNK_SIZE, C> {
    pub const fn chunk_size(&self) -> usize {
        CHUNK_SIZE
    }

    pub fn stored_chunk_count(&self) -> usize {
        self.chunk_count.load(Ordering::Relaxed)
    }

    pub fn stored_bytes(&self) -> usize {
        self.stored_chunk_count() * CHUNK_SIZE
    }

    pub(crate) fn store_chunk(
        self: &Arc<Self>,
        chunk: &[u8],
    ) -> Arc<StoredChunkRef<CHUNK_SIZE, C>> {
        assert!(chunk.len() <= CHUNK_SIZE);

        let crc = crc32fast::hash(chunk);
        let mut crc_map = self.crc_map.write().unwrap();

        // check crc
        if let Some(chunk_ref) = crc_map.get(&crc) {
            match chunk_ref {
                WeakChunkRefs::One(weak_ref) => {
                    if let Some(stored_chunk) = weak_ref.upgrade() {
                        if stored_chunk.as_bytes() == chunk {
                            // debug!(id = %stored_chunk.id(), "dup slab found");
                            return stored_chunk;
                        }
                    }
                }
                WeakChunkRefs::Many(chunk_refs) => {
                    for chunk_ref in chunk_refs {
                        if let Some(stored_chunk) = chunk_ref.upgrade() {
                            if stored_chunk.as_bytes() == chunk {
                                // debug!(id = %stored_chunk.id(), "dup slab found");
                                return stored_chunk;
                            }
                        }
                    }
                }
            }
        }

        // store new slab
        let mut chunk_mut = self
            .inner
            .clone()
            .create_owned()
            .expect("fail to create_owned");
        chunk_mut.copy_from_slice(chunk);
        let chunk = chunk_mut.downgrade();
        self.inner.clear(chunk.key());
        let chunk = Arc::new(StoredChunkRef {
            inner: Some(chunk),
            pool: self.clone(),
        });
        self.chunk_count.fetch_add(1, Ordering::Relaxed);

        // store weak ref into crc map
        match crc_map.entry(crc) {
            Entry::Occupied(mut entry) => {
                let value = entry.get_mut();
                match value {
                    WeakChunkRefs::One(chunk_ref) => {
                        *value =
                            WeakChunkRefs::Many(vec![chunk_ref.clone(), Arc::downgrade(&chunk)]);
                    }
                    WeakChunkRefs::Many(chunk_refs) => {
                        chunk_refs.push(Arc::downgrade(&chunk));
                    }
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(WeakChunkRefs::One(Arc::downgrade(&chunk)));
            }
        }

        chunk
    }

    pub(crate) fn clear_crc(self: &Arc<Self>, crc: u32) {
        let mut crc_map = self.crc_map.write().unwrap();
        let Entry::Occupied(mut entry) = crc_map.entry(crc) else {
            unreachable!()
        };

        let mut remove_entry = false;

        match entry.get_mut() {
            WeakChunkRefs::One(slab_ref) => {
                assert!(slab_ref.upgrade().is_none());
                remove_entry = true;
            }
            WeakChunkRefs::Many(slab_refs) => {
                let mut remove_index = None;
                for (i, slab_ref) in slab_refs.iter().enumerate() {
                    if slab_ref.upgrade().is_none() {
                        remove_index.replace(i);
                        break;
                    }
                }
                slab_refs.swap_remove(remove_index.unwrap());
                if slab_refs.is_empty() {
                    remove_entry = true;
                }
            }
        }

        if remove_entry {
            entry.remove();
        }
    }

    pub fn store_from_buf(self: &Arc<Self>, buf: &mut impl bytes::Buf) -> Slabytes<CHUNK_SIZE, C> {
        if !buf.has_remaining() {
            return Slabytes::new();
        }
        let mut slabytes = Slabytes::with_capacity(buf.remaining());
        while buf.has_remaining() {
            let len = std::cmp::min(buf.remaining(), CHUNK_SIZE);
            let bytes = buf.copy_to_bytes(len);
            let inner = self.store_chunk(&bytes);
            slabytes.append(crate::slabytes::Chunk::from(inner));
        }
        slabytes
    }

    pub fn store_from_bytes(self: &Arc<Self>, mut bytes: bytes::Bytes) -> Slabytes<CHUNK_SIZE, C> {
        self.store_from_buf(&mut bytes)
    }

    pub fn store_from_std_reader(
        self: &Arc<Self>,
        reader: &mut impl std::io::Read,
    ) -> std::io::Result<Slabytes<CHUNK_SIZE, C>> {
        use bytes::BytesMut;

        let mut slabytes = Slabytes::new();
        let mut eof = false;

        loop {
            let mut bytes_mut = BytesMut::zeroed(CHUNK_SIZE);

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
            let len = CHUNK_SIZE - buf.len();

            if len > 0 {
                let bytes = bytes_mut.freeze().split_to(len);
                let inner = self.clone().store_chunk(&bytes);
                slabytes.append(crate::slabytes::Chunk::from(inner));
            }

            if eof {
                break;
            }
        }

        Ok(slabytes)
    }

    #[cfg(feature = "tokio")]
    pub async fn store_from_async_reader(
        self: &Arc<Self>,
        reader: &mut (impl tokio::io::AsyncRead + Unpin),
    ) -> std::io::Result<Slabytes<CHUNK_SIZE, C>> {
        use bytes::BytesMut;
        use std::future::poll_fn;
        use std::pin::Pin;
        use tokio::io::ReadBuf;

        let mut slabytes = Slabytes::new();
        let mut eof = false;

        loop {
            let mut bytes_mut = BytesMut::zeroed(CHUNK_SIZE);

            let mut buf = ReadBuf::new(bytes_mut.as_mut());
            while buf.remaining() > 0 {
                let remaining = buf.remaining();
                poll_fn(|cx| Pin::new(&mut *reader).poll_read(cx, &mut buf)).await?;
                if buf.remaining() == remaining {
                    eof = true;
                    break;
                }
            }
            let len = CHUNK_SIZE - buf.remaining();

            if len > 0 {
                let bytes = bytes_mut.freeze().split_to(len);
                let inner = self.clone().store_chunk(&bytes);
                slabytes.append(crate::slabytes::Chunk::from(inner));
            }

            if eof {
                break;
            }
        }

        Ok(slabytes)
    }
}

#[derive(Debug)]
struct Chunk<const SIZE: usize> {
    data: [u8; SIZE],
    len: usize,
}

impl<const SIZE: usize> Default for Chunk<SIZE> {
    fn default() -> Self {
        Self {
            data: [0; SIZE],
            len: 0,
        }
    }
}

impl<const SIZE: usize> sharded_slab::Clear for Chunk<SIZE> {
    fn clear(&mut self) {
        self.len = 0;
    }
}

impl<const SIZE: usize> Chunk<SIZE> {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.data[0..self.len]
    }

    #[inline]
    fn as_mut_bytes(&mut self) -> &mut [u8] {
        &mut self.data[0..self.len]
    }

    fn copy_from_slice(&mut self, slice: &[u8]) {
        assert!(slice.len() <= SIZE);
        self.len = slice.len();
        self.as_mut_bytes().copy_from_slice(slice);
    }
}

#[derive(Debug)]
pub(crate) struct StoredChunkRef<const SIZE: usize, C: Config> {
    inner: Option<OwnedRef<Chunk<SIZE>, C>>,
    pool: Arc<Pool<SIZE, C>>,
}

impl<const SIZE: usize, C: Config> Drop for StoredChunkRef<SIZE, C> {
    fn drop(&mut self) {
        let slab_ref = self.inner.take().unwrap();
        let crc = crc32fast::hash(slab_ref.as_bytes());
        self.pool.clear_crc(crc);
        self.pool.chunk_count.fetch_sub(1, Ordering::Relaxed);
    }
}

impl<const SIZE: usize, C: Config> StoredChunkRef<SIZE, C> {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_ref().unwrap().as_bytes()
    }
}
