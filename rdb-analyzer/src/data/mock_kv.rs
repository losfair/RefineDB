use std::{
  collections::HashMap,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
};

use async_trait::async_trait;
use rpds::RedBlackTreeMapSync;
use tokio::sync::Mutex;

use super::kv::{KeyValueStore, KvError, KvKeyIterator, KvTransaction};
use anyhow::Result;

/// A mocked KV store that simulates MVCC with snapshot isolation.
pub struct MockKv {
  store: MockStore,
}

pub struct MockTransaction {
  id: u64,
  store: MockStore,
  read_buffer: RedBlackTreeMapSync<Vec<u8>, (Option<Vec<u8>>, u64)>,
  buffer: Mutex<RedBlackTreeMapSync<Vec<u8>, (Option<Vec<u8>>, u64)>>,
  modified: Mutex<HashMap<Vec<u8>, u64>>,
}

#[derive(Clone)]
struct MockStore {
  data: Arc<Mutex<RedBlackTreeMapSync<Vec<u8>, (Option<Vec<u8>>, u64)>>>,
  txn_count: Arc<AtomicU64>,
}

struct MockIterator {
  map: RedBlackTreeMapSync<Vec<u8>, (Option<Vec<u8>>, u64)>,
  current: Vec<u8>,
  end: Vec<u8>,
}

impl MockKv {
  pub fn new() -> Self {
    MockKv {
      store: MockStore {
        data: Arc::new(Mutex::new(RedBlackTreeMapSync::new_sync())),
        txn_count: Arc::new(AtomicU64::new(0)),
      },
    }
  }
}

impl MockKv {
  pub async fn dump(&self) -> RedBlackTreeMapSync<Vec<u8>, (Option<Vec<u8>>, u64)> {
    self.store.data.lock().await.clone()
  }
}

#[async_trait]
impl KeyValueStore for MockKv {
  async fn begin_transaction(&self) -> Result<Box<dyn KvTransaction>> {
    let buffer = self.store.data.lock().await.clone();
    Ok(Box::new(MockTransaction {
      id: self.store.txn_count.fetch_add(1, Ordering::SeqCst) + 1,
      store: self.store.clone(),
      read_buffer: buffer.clone(),
      buffer: Mutex::new(buffer),
      modified: Mutex::new(HashMap::new()),
    }))
  }
}

#[async_trait]
impl KvTransaction for MockTransaction {
  async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    log::trace!("[txn {}] get {}", self.id, base64::encode(key));
    Ok(
      self
        .read_buffer
        .get(key)
        .and_then(|x| x.0.as_ref())
        .cloned(),
    )
  }

  async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
    log::trace!(
      "[txn {}] put {} {}",
      self.id,
      base64::encode(key),
      base64::encode(value)
    );
    let mut buffer = self.buffer.lock().await;
    let mut modified = self.modified.lock().await;
    let version = buffer.get(key).map(|x| x.1).unwrap_or_default();
    buffer.insert_mut(key.to_vec(), (Some(value.to_vec()), version + 1));
    if !modified.contains_key(key) {
      modified.insert(key.to_vec(), version);
    }
    Ok(())
  }

  async fn delete(&self, key: &[u8]) -> Result<()> {
    log::trace!("[txn {}] delete {}", self.id, base64::encode(key));
    let mut buffer = self.buffer.lock().await;
    let mut modified = self.modified.lock().await;
    let version = buffer.get(key).map(|x| x.1).unwrap_or_default();
    buffer.insert_mut(key.to_vec(), (None, version + 1));
    if !modified.contains_key(key) {
      modified.insert(key.to_vec(), version);
    }
    Ok(())
  }

  async fn scan_keys(&self, start: &[u8], end: &[u8]) -> Result<Box<dyn KvKeyIterator>> {
    Ok(Box::new(MockIterator {
      map: self.buffer.lock().await.clone(),
      current: start.to_vec(),
      end: end.to_vec(),
    }))
  }

  async fn commit(self: Box<Self>) -> Result<(), KvError> {
    let buffer = self.buffer.into_inner();
    let modified = self.modified.into_inner();

    let mut data = self.store.data.lock().await;
    for (k, initial_version) in &modified {
      if data.get(k).map(|x| x.1).unwrap_or_default() != *initial_version {
        log::trace!("[txn {}] commit CONFLICT", self.id);
        return Err(KvError::Conflict);
      }
    }

    for (k, _) in modified {
      let value = buffer.get(&k).unwrap().clone();
      data.insert_mut(k, value);
    }
    log::trace!("[txn {}] commit OK", self.id);
    Ok(())
  }

  async fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()> {
    log::trace!(
      "[txn {}] delete_range {} {}",
      self.id,
      base64::encode(start),
      base64::encode(end)
    );
    let mut buffer = self.buffer.lock().await;
    let mut modified = self.modified.lock().await;

    let mut to_delete = vec![];
    for (k, _) in buffer.range(start.to_vec()..end.to_vec()) {
      to_delete.push(k.clone());
    }

    log::trace!(
      "[txn {}] deleted {} keys in range",
      self.id,
      to_delete.len()
    );

    for key in to_delete {
      let version = buffer.get(&key).map(|x| x.1).unwrap_or_default();
      buffer.insert_mut(key.clone(), (None, version + 1));
      if !modified.contains_key(&key) {
        modified.insert(key, version);
      }
    }
    Ok(())
  }
}

#[async_trait]
impl KvKeyIterator for MockIterator {
  async fn next(&mut self) -> Result<Option<Vec<u8>>> {
    let mut range = self.map.range(self.current.clone()..self.end.clone());
    loop {
      if let Some((k, v)) = range.next() {
        // Move to next
        self.current = k.iter().copied().chain(std::iter::once(0x00u8)).collect();
        match &v.0 {
          Some(x) => break Ok(Some(x.clone())),
          None => {}
        }
      } else {
        break Ok(None);
      }
    }
  }
}
