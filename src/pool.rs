use rustc_hash::FxHasher;
use sharded_slab::pool::OwnedRef;
use std::collections::{hash_map::Entry, HashMap};
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Weak};
use tracing::{debug, error};

pub const SLAB_SIZE: usize = 512;
static ALLOCATED_SLAB_COUNT: AtomicUsize = AtomicUsize::new(0);

pub fn allocated_slab_count() -> usize {
    ALLOCATED_SLAB_COUNT.load(Ordering::Relaxed)
}

pub(crate) enum Message {
    Store {
        bytes: bytes::Bytes,
        result_tx: std::sync::mpsc::Sender<Result<Arc<SlabRef>, Error>>,
    },

    #[cfg(feature = "tokio")]
    TokioStore {
        bytes: bytes::Bytes,
        result_tx: ::tokio::sync::oneshot::Sender<Result<Arc<SlabRef>, Error>>,
    },

    Drop {
        #[allow(dead_code)]
        slab_ref: OwnedRef<Slab>,
        crc: u32,
    },
}

#[derive(Debug)]
pub(crate) enum Error {
    SlabTooLarge,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::SlabTooLarge => write!(f, "Slab must be less than {SLAB_SIZE}"),
        }
    }
}

impl std::error::Error for Error {}

pub(crate) fn actor() -> &'static mpsc::Sender<Message> {
    use std::sync::OnceLock;
    static TX: OnceLock<mpsc::Sender<Message>> = OnceLock::new();
    TX.get_or_init(spawn_actor)
}

fn spawn_actor() -> mpsc::Sender<Message> {
    let (tx, rx) = mpsc::channel();
    let mut pool = Pool::new(tx.clone());
    let tx_clone = tx.clone();
    std::thread::Builder::new()
        .name("slab-pool-actor".into())
        .spawn(move || {
            debug!("pool actor started");
            for msg in rx {
                match msg {
                    Message::Store { bytes, result_tx } => {
                        if result_tx.send(pool.store_slab(&bytes)).is_err() {
                            error!("fail to send");
                        }
                    }

                    #[cfg(feature = "tokio")]
                    Message::TokioStore { bytes, result_tx } => {
                        if result_tx.send(pool.store_slab(&bytes)).is_err() {
                            error!("fail to send");
                        }
                    }

                    Message::Drop { crc, .. } => pool.clear_crc(crc),
                }
            }
            debug!("pool actor shutdown");
        })
        .unwrap();
    tx_clone
}

struct Pool {
    inner: Arc<sharded_slab::Pool<Slab>>,
    crc: HashMap<u32, WeakSlabRefs, BuildHasherDefault<FxHasher>>,
    tx: mpsc::Sender<Message>,
}

enum WeakSlabRefs {
    One(Weak<SlabRef>),
    Many(Vec<Weak<SlabRef>>),
}

impl Pool {
    pub fn new(tx: mpsc::Sender<Message>) -> Self {
        Self {
            inner: Arc::new(sharded_slab::Pool::<Slab>::new()),
            crc: HashMap::default(),
            tx,
        }
    }

    pub fn store_slab(&mut self, slab: &[u8]) -> Result<Arc<SlabRef>, Error> {
        if slab.len() > SLAB_SIZE {
            return Err(Error::SlabTooLarge);
        }

        let crc = crc32fast::hash(slab);

        // check crc
        if let Some(slab_ref) = self.crc.get(&crc) {
            match slab_ref {
                WeakSlabRefs::One(weak_ref) => {
                    if let Some(stored_slab) = weak_ref.upgrade() {
                        if stored_slab.as_bytes() == slab {
                            debug!(id = %stored_slab.id(), "dup slab found");
                            return Ok(stored_slab);
                        }
                    }
                }
                WeakSlabRefs::Many(slab_refs) => {
                    for slab_ref in slab_refs {
                        if let Some(stored_slab) = slab_ref.upgrade() {
                            if stored_slab.as_bytes() == slab {
                                debug!(id = %stored_slab.id(), "dup slab found");
                                return Ok(stored_slab);
                            }
                        }
                    }
                }
            }
        }

        // store new slab
        let mut slab_mut = self
            .inner
            .clone()
            .create_owned()
            .expect("fail to create_owned");
        slab_mut.store(slab);
        let slab = slab_mut.downgrade();
        self.inner.clear(slab.key());
        let slab = Arc::new(SlabRef {
            inner: Some(slab),
            tx: self.tx.clone(),
        });

        // store weak ref into crc map
        match self.crc.entry(crc) {
            Entry::Occupied(mut entry) => {
                let value = entry.get_mut();
                match value {
                    WeakSlabRefs::One(slab_ref) => {
                        *value = WeakSlabRefs::Many(vec![slab_ref.clone(), Arc::downgrade(&slab)]);
                    }
                    WeakSlabRefs::Many(slab_refs) => {
                        slab_refs.push(Arc::downgrade(&slab));
                    }
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(WeakSlabRefs::One(Arc::downgrade(&slab)));
            }
        }

        Ok(slab)
    }

    pub fn clear_crc(&mut self, crc: u32) {
        let Entry::Occupied(mut entry) = self.crc.entry(crc) else {
            unreachable!()
        };

        let mut remove_entry = false;

        match entry.get_mut() {
            WeakSlabRefs::One(slab_ref) => {
                assert!(slab_ref.upgrade().is_none());
                remove_entry = true;
            }
            WeakSlabRefs::Many(ref mut slab_refs) => {
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
}

#[derive(Debug)]
pub(crate) struct Slab {
    data: Box<[u8; SLAB_SIZE]>,
    len: usize,
    id: usize,
}

impl Default for Slab {
    fn default() -> Self {
        let id = ALLOCATED_SLAB_COUNT.fetch_add(1, Ordering::Relaxed);
        debug!(%id, "slab allocated");
        Self {
            data: Box::new([0; SLAB_SIZE]),
            len: 0,
            id,
        }
    }
}

impl sharded_slab::Clear for Slab {
    fn clear(&mut self) {
        debug!(id=%self.id, "clear slab");
        self.len = 0;
    }
}

impl Slab {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data[0..self.len]
    }

    #[inline]
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        &mut self.data[0..self.len]
    }

    pub fn store(&mut self, slice: &[u8]) {
        assert!(slice.len() <= SLAB_SIZE);
        self.len = slice.len();
        self.as_mut_bytes().copy_from_slice(slice);
    }
}

#[derive(Debug)]
pub(crate) struct SlabRef {
    inner: Option<OwnedRef<Slab>>,
    tx: mpsc::Sender<Message>,
}

impl Drop for SlabRef {
    fn drop(&mut self) {
        let slab_ref = self.inner.take().unwrap();
        let crc = crc32fast::hash(slab_ref.as_bytes());
        self.tx
            .send(Message::Drop { slab_ref, crc })
            .expect("fail to send Message::Drop");
    }
}

impl SlabRef {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_ref().unwrap().as_bytes()
    }

    fn id(&self) -> usize {
        self.inner.as_ref().unwrap().id
    }
}
