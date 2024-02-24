use super::*;
use bytes::{Buf, Bytes};
use std::io::Write;

fn gen_uniq_chunk<const SIZE: usize>() -> [u8; SIZE] {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static COUNT: AtomicUsize = AtomicUsize::new(0);
    let id = COUNT.fetch_add(1, Ordering::Relaxed);
    let id_buf = id.to_be_bytes();
    let mut chunk = [0; SIZE];
    chunk[SIZE - std::mem::size_of::<usize>()..].copy_from_slice(&id_buf);
    chunk
}

fn gen_uniq_chunk_bytes<const SIZE: usize>(chunk_count: usize) -> bytes::Bytes {
    let mut vec = Vec::with_capacity(SIZE * chunk_count);
    for _ in 0..chunk_count {
        vec.write_all(&gen_uniq_chunk::<SIZE>()).unwrap();
    }
    assert_eq!(vec.len(), SIZE * chunk_count);
    vec.into()
}

#[test]
fn test() {
    const CHUNK_SIZE: usize = 512;
    let pool = Pool::<CHUNK_SIZE>::new();
    assert_eq!(pool.chunk_size(), CHUNK_SIZE);

    let bytes1 = gen_uniq_chunk_bytes::<CHUNK_SIZE>(3);
    let slabytes1 = pool.store_from_bytes(bytes1.clone());
    // 3 chunks stored
    assert_eq!(pool.stored_chunk_count(), 3);

    assert_eq!(slabytes1.chunks.len(), 3);
    for chunk in slabytes1.clone().chunks {
        assert_eq!(chunk.remaining(), pool.chunk_size());
    }
    assert_eq!(
        slabytes1.clone().copy_to_bytes(slabytes1.remaining()),
        bytes1
    );

    let bytes2 = Bytes::from(vec![2; 300]);
    let slabytes2 = pool.store_from_bytes(bytes2.clone());
    // store 1 chunk (total 4 chunks used)
    assert_eq!(pool.stored_chunk_count(), 4);

    assert_eq!(slabytes2.chunks.len(), 1);
    assert_eq!(slabytes2.chunks[0].chunk(), &[2; 300]);
    assert_eq!(
        slabytes2.clone().copy_to_bytes(slabytes2.remaining()),
        bytes2
    );

    drop(slabytes1); // clear 3 chunks (total 1 chunk used)
    assert_eq!(pool.stored_chunk_count(), 1);

    let bytes3 = gen_uniq_chunk_bytes::<CHUNK_SIZE>(31);
    let slabytes3 = pool.store_from_bytes(bytes3.clone());
    // store 31 chunks (3 chunks reused, total 32 chunks used)
    assert_eq!(pool.stored_chunk_count(), 32);

    {
        let mut obj = slabytes3.clone();
        while obj.has_remaining() {
            let chunk = obj.chunk();
            assert_eq!(chunk.len(), pool.chunk_size());
            obj.advance(chunk.len());
        }
    }
    assert_eq!(
        slabytes3.clone().copy_to_bytes(slabytes3.remaining()),
        bytes3
    );

    let slabytes = pool.store_from_buf(&mut &[0; CHUNK_SIZE * 16][..]);
    assert_eq!(slabytes.remaining(), CHUNK_SIZE * 16);
    {
        let mut obj = slabytes.clone();
        while obj.has_remaining() {
            let chunk = obj.chunk();
            assert_eq!(chunk.len(), CHUNK_SIZE);
            assert_eq!(chunk, &[0; CHUNK_SIZE]);
            obj.advance(chunk.len());
        }
    }
    assert_eq!(pool.stored_chunk_count(), 33);

    let mut slabytes = pool
        .store_from_std_reader(&mut &[0; CHUNK_SIZE * 16][..])
        .unwrap();
    assert_eq!(
        &slabytes.copy_to_bytes(slabytes.remaining())[..],
        &[0; CHUNK_SIZE * 16][..]
    );
    assert_eq!(pool.stored_chunk_count(), 33);
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn async_read() {
    use tokio::io::AsyncReadExt;

    const CHUNK_SIZE: usize = 512;
    let pool = Pool::<CHUNK_SIZE>::new();
    assert_eq!(pool.chunk_size(), CHUNK_SIZE);

    let vec_in = vec![3; CHUNK_SIZE * 3];
    let mut obj = pool
        .store_from_async_reader(&mut &vec_in[..])
        .await
        .unwrap();
    assert_eq!(obj.chunks.len(), 3);
    assert_eq!(obj.chunks[0].chunk(), &[3; CHUNK_SIZE]);
    assert_eq!(obj.chunks[1].chunk(), &[3; CHUNK_SIZE]);
    assert_eq!(obj.chunks[2].chunk(), &[3; CHUNK_SIZE]);
    assert_eq!(pool.stored_chunk_count(), 1);
    let mut vec_out = vec![];
    obj.read_to_end(&mut vec_out).await.unwrap();
    assert_eq!(vec_out, vec_in);
    assert_eq!(pool.stored_chunk_count(), 0);
}
