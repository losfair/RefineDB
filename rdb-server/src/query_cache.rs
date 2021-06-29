use std::{
  sync::{Arc, Weak},
  time::{Duration, Instant},
};

use lru::LruCache;
use sysinfo::{get_current_pid, ProcessExt, System, SystemExt};
use tokio::{sync::Mutex, time::sleep};

use crate::exec_core::ExecContext;

/// The minimum threshold to shrink query cache to.
const MIN_QUERY_CACHE_SIZE: usize = 64;

/// Items per step when shrinking the query cache due to memory threshold metrics.
const QUERY_CACHE_SHRINK_STEP_SIZE: usize = 16;

const HOT_ITEM_TTL: Duration = Duration::from_secs(3);

pub struct QueryCache {
  items: Mutex<LruCache<QueryCacheKey, Arc<ExecContext>>>,
  hot_items: Mutex<LruCache<(String, String), HotItem>>,
  params: QueryCacheParams,
}

struct HotItem {
  exec_ctx: Arc<ExecContext>,
  create_time: Instant,
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
      hot_items: Mutex::new(LruCache::unbounded()),
      params,
    });
    let me_weak = Arc::downgrade(&me);
    tokio::spawn(async move {
      Self::gc(me_weak).await;
    });
    me
  }

  pub async fn get_hot(
    &self,
    namespace_id: &str,
    query_script_id: &str,
  ) -> Option<Arc<ExecContext>> {
    let hot_items = self.hot_items.lock().await;

    // Peek. Don't update LRU state.
    if let Some(x) = hot_items.peek(&(namespace_id.to_string(), query_script_id.to_string())) {
      Some(x.exec_ctx.clone())
    } else {
      None
    }
  }

  pub async fn get(&self, key: &QueryCacheKey) -> Option<Arc<ExecContext>> {
    let items = self.items.lock().await;
    let item = items.peek(key).cloned();
    drop(items);

    // Insert into hot cache.
    if let Some(item) = &item {
      self.hot_items.lock().await.put(
        (key.namespace_id.clone(), key.query_script_id.clone()),
        HotItem {
          exec_ctx: item.clone(),
          create_time: Instant::now(),
        },
      );
    }

    item
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
        None => {
          log::warn!("gc: exiting");
          break;
        }
      };

      // Step 1: Hot item expiration.
      {
        let mut hot_items = me.hot_items.lock().await;

        // Get current time after locking hot_items to ensure: forall x in hot_items, now >= x.create_time.
        let now = Instant::now();

        let mut pop_count = 0usize;

        while let Some((_, v)) = hot_items.peek_lru() {
          let dur = now.duration_since(v.create_time);
          if dur > HOT_ITEM_TTL {
            hot_items.pop_lru().unwrap();
            pop_count += 1;
          } else {
            break;
          }
        }

        if pop_count > 0 {
          log::info!("gc: Removed {} hot item(s) from cache.", pop_count);
        }
      }

      // Step 2: Process memory threshold.
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
