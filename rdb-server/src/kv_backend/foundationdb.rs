use std::{collections::BTreeMap, ops::Range, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use foundationdb::{future::FdbValues, Database, KeySelector, RangeOption, Transaction};
use rdb_analyzer::data::kv::{KeyValueStore, KvError, KvKeyIterator, KvTransaction};
use tokio::sync::Mutex;

pub struct FdbKvStore {
  db: Arc<Database>,
  prefix: Arc<[u8]>,
}

pub struct FdbTxn {
  inner: Arc<Transaction>,
  prefix: Arc<[u8]>,
  write_buffer: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
  range_deletion_buffer: Mutex<Vec<Range<Vec<u8>>>>,
}

impl FdbKvStore {
  pub fn new(db: Arc<Database>, prefix: &[u8]) -> Self {
    Self {
      db,
      prefix: Arc::from(prefix),
    }
  }
}

#[async_trait]
impl KeyValueStore for FdbKvStore {
  async fn begin_transaction(&self) -> Result<Box<dyn KvTransaction>> {
    let txn = self.db.create_trx()?;
    Ok(Box::new(FdbTxn {
      inner: Arc::new(txn),
      prefix: self.prefix.clone(),
      write_buffer: Mutex::new(BTreeMap::new()),
      range_deletion_buffer: Mutex::new(Vec::new()),
    }))
  }
}

#[async_trait]
impl KvTransaction for FdbTxn {
  async fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
    let k = self
      .prefix
      .iter()
      .chain(k.iter())
      .copied()
      .collect::<Vec<_>>();
    let res = self.inner.get(&k, false).await?;
    Ok(res.map(|x| x.to_vec()))
  }

  async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
    // Defer writes to commit
    self
      .write_buffer
      .lock()
      .await
      .insert(key.to_vec(), Some(value.to_vec()));
    Ok(())
  }

  async fn delete(&self, key: &[u8]) -> Result<()> {
    self.write_buffer.lock().await.insert(key.to_vec(), None);
    Ok(())
  }

  async fn scan_keys(&self, start: &[u8], end: &[u8]) -> Result<Box<dyn KvKeyIterator>> {
    let start = self
      .prefix
      .iter()
      .chain(start.iter())
      .copied()
      .collect::<Vec<_>>();
    let end = self
      .prefix
      .iter()
      .chain(end.iter())
      .copied()
      .collect::<Vec<_>>();

    let range: RangeOption = (start..end).into();
    Ok(Box::new(FdbIterator {
      txn: self.inner.clone(),
      prefix: self.prefix.clone(),
      values: None,
      range,
      iteration: 1,
    }))
  }

  async fn commit(self: Box<Self>) -> Result<(), KvError> {
    for (k, v) in self.write_buffer.into_inner() {
      let k = self
        .prefix
        .iter()
        .chain(k.iter())
        .copied()
        .collect::<Vec<_>>();
      if let Some(v) = v {
        self.inner.set(&k, &v);
      } else {
        self.inner.clear(&k);
      }
    }
    for x in self.range_deletion_buffer.into_inner() {
      self.inner.clear_range(&x.start, &x.end);
    }
    Arc::try_unwrap(self.inner)
      .map_err(|_| {
        log::error!("some iterators are not dropped at commit time");
        KvError::CommitStateUnknown
      })?
      .commit()
      .await
      .map_err(|e| {
        log::error!("txn commit error: {:?}", e);
        if e.is_retryable() {
          KvError::Conflict
        } else {
          KvError::CommitStateUnknown
        }
      })
      .map(|_| ())
  }

  async fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()> {
    self
      .range_deletion_buffer
      .lock()
      .await
      .push(start.to_vec()..end.to_vec());
    Ok(())
  }
}

pub struct FdbIterator {
  txn: Arc<Transaction>,
  prefix: Arc<[u8]>,
  values: Option<(FdbValues, usize)>,
  range: RangeOption<'static>,
  iteration: usize,
}

#[async_trait]
impl KvKeyIterator for FdbIterator {
  async fn next(&mut self) -> Result<Option<Vec<u8>>> {
    if self.values.is_none() {
      log::trace!("get_range iteration {}", self.iteration);
      let values = self
        .txn
        .get_range(&self.range, self.iteration, false)
        .await?;
      if values.len() == 0 {
        return Ok(None);
      }
      self.iteration += 1;
      self.values = Some((values, 0));
    }

    let (values, value_index) = self.values.as_mut().unwrap();
    let raw_key = values[*value_index].key();
    let key = raw_key.strip_prefix(&*self.prefix).unwrap().to_vec();
    if *value_index + 1 == values.len() {
      self.range.begin = KeySelector::first_greater_than(raw_key.to_vec());
      self.values = None;
    } else {
      *value_index += 1;
    }

    log::trace!("got key: {}", base64::encode(&key));

    Ok(Some(key))
  }
}
