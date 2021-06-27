use std::{
  sync::{Arc, Weak},
  time::Duration,
};

use lru::LruCache;
use sysinfo::{get_current_pid, ProcessExt, System, SystemExt};
use tokio::{sync::Mutex, time::sleep};

use crate::exec_core::ExecContext;

/// The minimum threshold to shrink query cache to.
const MIN_QUERY_CACHE_SIZE: usize = 64;

/// Items per step when shrinking the query cache due to memory threshold metrics.
const QUERY_CACHE_SHRINK_STEP_SIZE: usize = 16;

pub struct QueryCache {
  items: Mutex<LruCache<QueryCacheKey, Arc<ExecContext>>>,
  params: QueryCacheParams,
}

#[derive(Clone, Debug)]
pub struct QueryCacheParams {
  pub process_memory_threshold_kb: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct QueryCacheKey {
  /// Namespace id.
  pub namespace_id: String,

  /// Generated deployment id.
  pub deployment_id: String,

  /// User-provided query script id.
  pub query_script_id: String,

  /// In case the query script is updated.
  pub query_script_create_time: i64,
}

impl QueryCache {
  pub fn new(params: QueryCacheParams) -> Arc<Self> {
    let me = Arc::new(Self {
      items: Mutex::new(LruCache::unbounded()),
      params,
    });
    let me_weak = Arc::downgrade(&me);
    tokio::spawn(async move { Self::gc(me_weak) });
    me
  }

  pub async fn get(&self, key: &QueryCacheKey) -> Option<Arc<ExecContext>> {
    let items = self.items.lock().await;
    items.peek(key).cloned()
  }

  pub async fn put(&self, key: QueryCacheKey, value: Arc<ExecContext>) {
    self.items.lock().await.put(key, value);
  }

  async fn gc(me: Weak<Self>) {
    let system = System::new_all();
    loop {
      sleep(Duration::from_secs(1)).await;
      let me = match me.upgrade() {
        Some(x) => x,
        None => break,
      };

      // Process memory threshold.
      let process = system
        .get_process(get_current_pid().unwrap())
        .expect("cannot get current process");
      let memory_usage_kb = process.memory();
      if memory_usage_kb > me.params.process_memory_threshold_kb {
        let mut items = me.items.lock().await;
        if items.len() > MIN_QUERY_CACHE_SIZE {
          log::warn!(
            "Memory usage ({} KiB) exceeds threshold ({} KiB) and the query cache contains {} items. Shrinking query cache by {} items.",
            memory_usage_kb,
            me.params.process_memory_threshold_kb,
            items.len(),
            QUERY_CACHE_SHRINK_STEP_SIZE,
          );
          for _ in 0..QUERY_CACHE_SHRINK_STEP_SIZE {
            items.pop_lru();
          }
        }
      }
    }
  }
}
