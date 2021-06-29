//! Key-value store utilities.
//!
//! In this module we define the *system keyspace*, a special keyspace prefixed by `\xff`. In storage plans we never
//! have this kind of keys (8925 years!) so this works.
//!
//! - System subspace for k8 dedup: `\xff\x00`.

use anyhow::Result;

use crate::util::rand_kn_stateless;

use super::kv::KvTransaction;
use thiserror::Error;

#[derive(Error, Debug)]
enum KvUtilError {
  #[error("failed to get a deduplicated key after retries")]
  FailedToGetDeduplicatedKey,
}

pub async fn global_k8_deduplicated(kv: &dyn KvTransaction) -> Result<[u8; 8]> {
  for _ in 0..10 {
    let k8 = rand_kn_stateless::<8>();
    let mut key = vec![0xffu8, 0x00u8];
    key.extend_from_slice(&k8);
    if kv.get(&key).await?.is_none() {
      kv.put(&key, &[]).await?;
      return Ok(k8);
    }
  }
  Err(KvUtilError::FailedToGetDeduplicatedKey.into())
}
