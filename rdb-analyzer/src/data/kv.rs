use anyhow::Result;
use async_trait::async_trait;
use thiserror::Error;

#[async_trait]
pub trait KeyValueStore {
  async fn begin_transaction(&self) -> Result<Box<dyn KvTransaction>>;
}

#[async_trait]
pub trait KvTransaction {
  async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;
  async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
  async fn delete(&mut self, key: &[u8]) -> Result<()>;
  async fn scan_keys(&mut self, start: &[u8], end: &[u8]) -> Result<Box<dyn KvKeyIterator>>;
  async fn commit(self: Box<Self>) -> Result<(), KvError>;
}

#[async_trait]
pub trait KvKeyIterator {
  async fn next(&self) -> Result<Option<Vec<u8>>>;
}

#[derive(Error, Debug)]
pub enum KvError {
  #[error("conflict")]
  Conflict,

  #[error("commit state unknown")]
  CommitStateUnknown,
}
