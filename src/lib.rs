mod pool;
mod slabytes;

const DEFAULT_CHUNK_SIZE: usize = 512;

pub use pool::Pool;
pub use slabytes::Slabytes;

#[cfg(test)]
mod test;
