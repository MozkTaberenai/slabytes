use super::*;
use bytes::{Buf, Bytes};
use std::io::Write;

fn gen_uniq_chunk() -> [u8; SLAB_SIZE] {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static COUNT: AtomicUsize = AtomicUsize::new(0);
    let id = COUNT.fetch_add(1, Ordering::Relaxed);
    let id_buf = id.to_be_bytes();
    let mut chunk = [0; SLAB_SIZE];
    chunk[SLAB_SIZE - std::mem::size_of::<usize>()..].copy_from_slice(&id_buf);
    chunk
}

fn gen_uniq_chunk_bytes(chunk_count: usize) -> bytes::Bytes {
    let mut vec = Vec::with_capacity(SLAB_SIZE * chunk_count);
    for _ in 0..chunk_count {
        vec.write_all(&gen_uniq_chunk()).unwrap();
    }
    assert_eq!(vec.len(), SLAB_SIZE * chunk_count);
    vec.into()
}

#[test]
fn test() {
    use sharded_slab::{Config, DefaultConfig};
    const INITIAL_PAGE_SIZE: usize = DefaultConfig::INITIAL_PAGE_SIZE;

    let bytes1 = gen_uniq_chunk_bytes(3);
    let slabytes1 = Slabytes::from(bytes1.clone());
    // 3 chunks stored
    assert_eq!(crate::allocated_slab_count(), INITIAL_PAGE_SIZE);

    assert_eq!(slabytes1.slabs.len(), 3);
    for slab in slabytes1.clone().slabs {
        assert_eq!(slab.remaining(), SLAB_SIZE);
    }
    assert_eq!(
        slabytes1.clone().copy_to_bytes(slabytes1.remaining()),
        bytes1
    );

    let bytes2 = Bytes::from(vec![2; 300]);
    let slabytes2 = Slabytes::from(bytes2.clone());
    // store 1 chunk (total 4 chunks used)
    assert_eq!(slabytes2.slabs.len(), 1);
    assert_eq!(slabytes2.slabs[0].chunk(), &[2; 300]);
    assert_eq!(
        slabytes2.clone().copy_to_bytes(slabytes2.remaining()),
        bytes2
    );

    drop(slabytes1); // clear 3 chunks (total 1 chunk used)

    let bytes3 = gen_uniq_chunk_bytes(31);
    let slabytes3 = Slabytes::from(bytes3.clone());
    // store 31 chunks (3 chunks reused, total 32 chunks used)
    assert_eq!(crate::allocated_slab_count(), INITIAL_PAGE_SIZE);
    {
        let mut obj = slabytes3.clone();
        while obj.has_remaining() {
            let chunk = obj.chunk();
            assert_eq!(chunk.len(), SLAB_SIZE);
            obj.advance(chunk.len());
        }
    }
    assert_eq!(
        slabytes3.clone().copy_to_bytes(slabytes3.remaining()),
        bytes3
    );

    // https://docs.rs/sharded-slab/latest/sharded_slab/implementation/index.html

    let _slabytes4 = Slabytes::from(gen_uniq_chunk_bytes(INITIAL_PAGE_SIZE * 2));
    assert_eq!(
        crate::allocated_slab_count(),
        INITIAL_PAGE_SIZE + INITIAL_PAGE_SIZE * 2
    );

    let slabytes5 = Slabytes::from_buf_blocking(&mut vec![5; SLAB_SIZE].as_slice());
    assert_eq!(
        crate::allocated_slab_count(),
        INITIAL_PAGE_SIZE + INITIAL_PAGE_SIZE * 2 + INITIAL_PAGE_SIZE * 4
    );
    drop(slabytes5);

    let slabytes6 = Slabytes::from(gen_uniq_chunk_bytes(INITIAL_PAGE_SIZE * 4));
    assert_eq!(
        crate::allocated_slab_count(),
        INITIAL_PAGE_SIZE + INITIAL_PAGE_SIZE * 2 + INITIAL_PAGE_SIZE * 4
    );
    drop(slabytes6);

    let slabytes = Slabytes::from_buf_blocking(&mut vec![0; SLAB_SIZE * 4].as_slice());
    assert_eq!(slabytes.remaining(), SLAB_SIZE * 4);
    {
        let mut obj = slabytes.clone();
        while obj.has_remaining() {
            let chunk = obj.chunk();
            assert_eq!(chunk.len(), SLAB_SIZE);
            assert_eq!(chunk, &[0; SLAB_SIZE]);
            obj.advance(chunk.len());
        }
    }

    let slabytes = Slabytes::from_buf_blocking(&mut &[0; SLAB_SIZE * 16][..]);
    assert_eq!(slabytes.remaining(), SLAB_SIZE * 16);
    {
        let mut obj = slabytes.clone();
        while obj.has_remaining() {
            let chunk = obj.chunk();
            assert_eq!(chunk.len(), SLAB_SIZE);
            assert_eq!(chunk, &[0; SLAB_SIZE]);
            obj.advance(chunk.len());
        }
    }
    let slabytes = Slabytes::from_buf_blocking(&mut &b"01234"[..]);
    assert_eq!(slabytes.chunk(), b"01234");
    drop(slabytes);

    let mut slabytes =
        Slabytes::from_std_reader(&mut &[0; SLAB_SIZE * INITIAL_PAGE_SIZE * 8][..]).unwrap();
    assert_eq!(
        &slabytes.copy_to_bytes(slabytes.remaining())[..],
        &[0; SLAB_SIZE * INITIAL_PAGE_SIZE * 8][..]
    );

    assert_eq!(
        crate::allocated_slab_count(),
        INITIAL_PAGE_SIZE + INITIAL_PAGE_SIZE * 2 + INITIAL_PAGE_SIZE * 4
    );

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            use tokio::io::AsyncReadExt;

            let vec1_in = vec![1; SLAB_SIZE];
            let mut obj1 = Slabytes::from_buf(&mut &vec1_in[..]).await;
            assert_eq!(obj1.slabs.len(), 1);
            assert_eq!(obj1.slabs[0].chunk(), &[1; SLAB_SIZE]);
            let mut vec1_out = vec![];
            obj1.read_to_end(&mut vec1_out).await.unwrap();
            assert_eq!(vec1_out, vec1_in);

            let vec2_in = vec![2; SLAB_SIZE * 2];
            let mut obj2 = Slabytes::from_buf(&mut &vec2_in[..]).await;
            assert_eq!(obj2.slabs.len(), 2);
            assert_eq!(obj2.slabs[0].chunk(), &[2; SLAB_SIZE]);
            assert_eq!(obj2.slabs[1].chunk(), &[2; SLAB_SIZE]);
            let mut vec2_out = vec![];
            obj2.read_to_end(&mut vec2_out).await.unwrap();
            assert_eq!(vec2_out, vec2_in);

            let vec3_in = vec![3; SLAB_SIZE * 3];
            let mut obj3 = Slabytes::from_async_reader(&mut &vec3_in[..])
                .await
                .unwrap();
            assert_eq!(obj3.slabs.len(), 3);
            assert_eq!(obj3.slabs[0].chunk(), &[3; SLAB_SIZE]);
            assert_eq!(obj3.slabs[1].chunk(), &[3; SLAB_SIZE]);
            assert_eq!(obj3.slabs[2].chunk(), &[3; SLAB_SIZE]);
            let mut vec3_out = vec![];
            obj3.read_to_end(&mut vec3_out).await.unwrap();
            assert_eq!(vec3_out, vec3_in);
        })
}
