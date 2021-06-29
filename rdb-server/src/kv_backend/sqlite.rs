use std::{pin::Pin, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rdb_analyzer::data::kv::{KeyValueStore, KvError, KvKeyIterator, KvTransaction};
use rusqlite::{named_params, OptionalExtension, Transaction};
use std::future::Future;
use thiserror::Error;
use tokio::{
  runtime::Builder,
  sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot, Mutex,
  },
  task::{block_in_place, spawn_local, LocalSet},
};

pub struct SqliteKvStore {
  global: Arc<GlobalSqliteStore>,
  table: Arc<str>,
  prefix: Arc<[u8]>,
}

#[derive(Error, Debug)]
pub enum SqliteKvError {
  #[error("interrupted")]
  Interrupted,
}

pub struct GlobalSqliteStore {
  conn_pool: Pool<SqliteConnectionManager>,
  task_tx: UnboundedSender<Task>,
}

type Task = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()>>> + Send>;

impl GlobalSqliteStore {
  pub fn open_leaky(path: &str) -> Result<Arc<Self>> {
    let manager = SqliteConnectionManager::file(path).with_init(|c| {
      c.execute_batch(
        r#"
      PRAGMA journal_mode=WAL;
      create table if not exists system (k blob primary key, v blob);
      create table if not exists system_meta (k blob primary key, v blob);
      create table if not exists user_data (k blob primary key, v blob);
      "#,
      )
    });

    let (task_tx, task_rx) = unbounded_channel();
    let me = Arc::new(Self {
      conn_pool: Pool::new(manager)?,
      task_tx,
    });

    let me2 = me.clone();

    // Isolate SQLite work onto its own thread
    std::thread::spawn(move || {
      let rt = Builder::new_current_thread().enable_all().build().unwrap();
      LocalSet::new().block_on(&rt, me2.run_worker(task_rx))
    });
    Ok(me)
  }

  async fn run_worker(self: Arc<Self>, mut task_rx: UnboundedReceiver<Task>) -> ! {
    loop {
      let task = task_rx.recv().await.unwrap();
      spawn_local(task());
    }
  }
}

impl SqliteKvStore {
  pub fn new(global: Arc<GlobalSqliteStore>, table: &str, prefix: &[u8]) -> Self {
    Self {
      global,
      table: Arc::from(table),
      prefix: Arc::from(prefix),
    }
  }
}

#[async_trait]
impl KeyValueStore for SqliteKvStore {
  async fn begin_transaction(&self) -> Result<Box<dyn KvTransaction>> {
    let conn = block_in_place(|| self.global.conn_pool.get())?;

    let (work_tx, work_rx) = unbounded_channel();
    self
      .global
      .task_tx
      .send(Box::new(|| {
        Box::pin(async move { txn_worker(conn, work_rx).await })
      }))
      .unwrap_or_else(|_| unreachable!());
    Ok(Box::new(SqliteKvTxn {
      work_tx,
      log: Mutex::new(vec![]),
      table: self.table.clone(),
      prefix: self.prefix.clone(),
    }))
  }
}

type Work = Box<
  dyn for<'a> FnOnce(&'a mut Option<Transaction>) -> Pin<Box<dyn Future<Output = ()> + 'a>> + Send,
>;

pub struct SqliteKvTxn {
  work_tx: UnboundedSender<Work>,
  log: Mutex<Vec<ModOp>>,
  table: Arc<str>,
  prefix: Arc<[u8]>,
}

enum ModOp {
  Put(Vec<u8>, Vec<u8>),
  Delete(Vec<u8>),
  DeleteRange(Vec<u8>, Vec<u8>),
}

async fn txn_worker(
  mut conn: PooledConnection<SqliteConnectionManager>,
  mut work_rx: UnboundedReceiver<Work>,
) {
  let mut txn = Some(match conn.transaction() {
    Ok(x) => x,
    Err(e) => {
      log::error!("txn_worker: transaction creation error: {:?}", e);
      return;
    }
  });

  loop {
    let work = match work_rx.recv().await {
      Some(x) => x,
      None => {
        log::debug!("txn_worker: ending transaction");
        return;
      }
    };
    work(&mut txn).await;
  }
}

impl SqliteKvTxn {
  async fn run<
    G: FnOnce(&mut Option<Transaction>) -> Result<R> + Send + 'static,
    R: Send + 'static,
  >(
    &self,
    f: G,
  ) -> Result<R> {
    let (tx, rx) = oneshot::channel();
    let res = self.work_tx.send(Box::new(move |txn| {
      Box::pin(async move {
        // Don't check the error here in case of asynchronous cancellation on `rx`.
        let _ = tx.send(f(txn));
      })
    }));
    let res = match res {
      Ok(_) => rx.await.unwrap_or_else(|e| Err(anyhow::Error::from(e))),
      Err(_) => Err(anyhow::Error::from(SqliteKvError::Interrupted)),
    };
    res
  }
}

#[async_trait]
impl KvTransaction for SqliteKvTxn {
  async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let key = self
      .prefix
      .iter()
      .copied()
      .chain(key.iter().copied())
      .collect::<Vec<_>>();
    let table = self.table.clone();
    self
      .run(move |txn| {
        let mut stmt = txn
          .as_mut()
          .unwrap()
          .prepare_cached(&format!("select v from {} where k = ?", table))?;
        let value: Option<Vec<u8>> = stmt.query_row(&[&key], |x| x.get(0)).optional()?;
        Ok(value)
      })
      .await
  }

  async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
    let key = self
      .prefix
      .iter()
      .copied()
      .chain(key.iter().copied())
      .collect::<Vec<_>>();
    let value = value.to_vec();
    self.log.lock().await.push(ModOp::Put(key, value));
    Ok(())
  }

  async fn delete(&self, key: &[u8]) -> Result<()> {
    let key = self
      .prefix
      .iter()
      .copied()
      .chain(key.iter().copied())
      .collect::<Vec<_>>();
    self.log.lock().await.push(ModOp::Delete(key));
    Ok(())
  }

  async fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()> {
    let start = self
      .prefix
      .iter()
      .copied()
      .chain(start.iter().copied())
      .collect::<Vec<_>>();
    let end = self
      .prefix
      .iter()
      .copied()
      .chain(end.iter().copied())
      .collect::<Vec<_>>();
    self.log.lock().await.push(ModOp::DeleteRange(start, end));
    Ok(())
  }

  async fn scan_keys(&self, start: &[u8], end: &[u8]) -> Result<Box<dyn KvKeyIterator>> {
    let start = self
      .prefix
      .iter()
      .copied()
      .chain(start.iter().copied())
      .collect::<Vec<_>>();
    let end = self
      .prefix
      .iter()
      .copied()
      .chain(end.iter().copied())
      .collect::<Vec<_>>();
    let table = self.table.clone();
    let prefix_len = self.prefix.len();
    self
      .run(move |txn| {
        let mut stmt = txn.as_mut().unwrap().prepare_cached(&format!(
          "select k from {} where k >= ? and k < ? order by k desc",
          table
        ))?;
        let keys: Vec<Vec<u8>> = stmt
          .query_map(&[&start, &end], |x| x.get(0))?
          .map(|x| x.map_err(anyhow::Error::from))
          .collect::<Result<_>>()?;
        Ok(Box::new(SqliteKvIterator {
          keys: keys.into_iter().map(|x| x[prefix_len..].to_vec()).collect(),
        }) as Box<dyn KvKeyIterator>)
      })
      .await
  }

  async fn commit(self: Box<Self>) -> Result<(), KvError> {
    let log = std::mem::replace(&mut *self.log.try_lock().unwrap(), vec![]);
    let table = self.table.clone();
    self
      .run(move |txn| {
        let txn = txn.take().unwrap();
        for op in log {
          match op {
            ModOp::Put(key, value) => {
              let mut stmt = txn.prepare_cached(&format!(
                "insert into {} (k, v) values(:k, :v) on conflict(k) do update set v = :v",
                table
              ))?;
              stmt.execute(named_params! { ":k": &key, ":v": &value })?;
            }
            ModOp::Delete(key) => {
              let mut stmt = txn.prepare_cached(&format!("delete from {} where k = ?", table))?;
              stmt.execute(&[&key])?;
            }
            ModOp::DeleteRange(start, end) => {
              let mut stmt =
                txn.prepare_cached(&format!("delete from {} where k >= ? and k < ?", table))?;
              stmt.execute(&[&start, &end])?;
            }
          }
        }
        txn.commit()?;
        Ok(())
      })
      .await
      .map_err(|e| {
        if let Some(x) = e.downcast_ref::<rusqlite::Error>() {
          match x {
            rusqlite::Error::SqliteFailure(_, reason) => {
              if let Some(reason) = reason {
                if reason == "database is locked" {
                  return KvError::Conflict;
                }
              }
            }
            _ => {}
          }
        }
        log::error!("sqlite commit error: {:?}", e);
        KvError::CommitStateUnknown
      })
  }
}

pub struct SqliteKvIterator {
  keys: Vec<Vec<u8>>,
}

#[async_trait]
impl KvKeyIterator for SqliteKvIterator {
  async fn next(&mut self) -> Result<Option<Vec<u8>>> {
    Ok(self.keys.pop())
  }
}
