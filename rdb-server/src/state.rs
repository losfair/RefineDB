use std::sync::Arc;

use once_cell::sync::OnceCell;
use rdb_analyzer::data::kv::KeyValueStore;

use crate::{query_cache::QueryCache, system::SystemSchema};

pub type DataStoreGenerator = Box<dyn Fn(&[u8]) -> Box<dyn KeyValueStore> + Send + Sync>;

pub struct ServerState {
  pub data_store_generator: DataStoreGenerator,
  pub system_store: Box<dyn KeyValueStore>,
  pub system_schema: SystemSchema,
  pub query_cache: Arc<QueryCache>,
}

static STATE: OnceCell<ServerState> = OnceCell::new();

pub fn set_state(st: ServerState) {
  STATE
    .set(st)
    .unwrap_or_else(|_| panic!("set_state: attempting to set state twice"));
}

pub fn get_state() -> &'static ServerState {
  STATE.get().expect("get_state: not initialized")
}
