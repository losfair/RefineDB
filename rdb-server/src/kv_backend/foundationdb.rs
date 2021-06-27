use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use foundationdb::{
  future::FdbValues, options::TransactionOption, Database, KeySelector, RangeOption, Transaction,
};
use rdb_analyzer::data::kv::{KeyValueStore, KvError, KvKeyIterator, KvTransaction};

pub struct FdbKvStore {
  db: Arc<Database>,
  prefix: Arc<[u8]>,
}

pub struct FdbTxn {
  inner: Arc<Transaction>,
  prefix: Arc<[u8]>,
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

    // Required for RefineDB execution semantics
    txn.set_option(TransactionOption::ReadYourWritesDisable)?;

    Ok(Box::new(FdbTxn {
      inner: Arc::new(txn),
      prefix: self.prefix.clone(),
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
    log::trace!("get {}", base64::encode(&k));
    let res = self.inner.get(&k, false).await?;
    Ok(res.map(|x| x.to_vec()))
  }

  async fn put(&self, k: &[u8], v: &[u8]) -> Result<()> {
    let k = self
      .prefix
      .iter()
      .chain(k.iter())
      .copied()
      .collect::<Vec<_>>();
    log::trace!("put {} {}", base64::encode(&k), base64::encode(&v));
    self.inner.set(&k, &v);
    Ok(())
  }

  async fn delete(&self, k: &[u8]) -> Result<()> {
    let k = self
      .prefix
      .iter()
      .chain(k.iter())
      .copied()
      .collect::<Vec<_>>();
    log::trace!("clear {}", base64::encode(&k));
    self.inner.clear(&k);
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

  async fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()> {
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
    log::trace!(
      "clear_range {} {}",
      base64::encode(&start),
      base64::encode(&end)
    );
    self.inner.clear_range(&start, &end);
    Ok(())
  }

  async fn commit(self: Box<Self>) -> Result<(), KvError> {
    Arc::try_unwrap(self.inner)
      .map_err(|_| {
        log::error!("some iterators are not dropped at commit time");
        KvError::CommitStateUnknown
      })?
      .commit()
      .await
      .map_err(|e| {
        log::error!("txn commit error: {:?}", e);

        // XXX: Is this correct?
        if e.is_retryable_not_committed() {
          KvError::Conflict
        } else {
          KvError::CommitStateUnknown
        }
      })
      .map(|_| ())
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
