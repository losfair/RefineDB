use std::sync::Arc;

use anyhow::Result;
use foundationdb::{tuple::Subspace, Database};
use rdb_analyzer::data::kv::KeyValueStore;
use rdb_proto::{proto::rdb_control_server::RdbControlServer, tonic::transport::Server};
use structopt::StructOpt;
use tokio::runtime::Runtime;

use crate::{
  httpapi::run_http_server,
  kv_backend::{
    foundationdb::FdbKvStore,
    sqlite::{GlobalSqliteStore, SqliteKvStore},
  },
  opt::Opt,
  query_cache::{QueryCache, QueryCacheParams},
  server::ControlServer,
  state::{set_state, DataStoreGenerator, ServerState},
  system::SystemSchema,
};
mod exec;
mod exec_core;
mod httpapi;
mod kv_backend;
mod opt;
mod query_cache;
mod server;
mod state;
mod sysquery;
mod system;
mod util;

fn main() {
  pretty_env_logger::init_timed();
  let network = unsafe { foundationdb::boot() };

  Runtime::new()
    .unwrap()
    .block_on(async move { run().await })
    .unwrap();

  // Required for safety
  drop(network);
}

async fn run() -> Result<()> {
  let opt = Opt::from_args();

  let data_store_generator: DataStoreGenerator;
  let system_store: Box<dyn KeyValueStore>;
  let system_metadata_store: Box<dyn KeyValueStore>;
  if let Some(x) = &opt.fdb_cluster {
    if opt.sqlite_db.is_some() {
      panic!("cannot select multiple kv backends");
    }
    let db = Arc::new(Database::new(Some(x))?);
    let keyspace = Subspace::from_bytes(
      opt
        .fdb_keyspace
        .as_ref()
        .expect("missing fdb-keyspace")
        .as_bytes(),
    );

    system_store = Box::new(FdbKvStore::new(
      db.clone(),
      keyspace.subspace(&"System").bytes(),
    ));
    system_metadata_store = Box::new(FdbKvStore::new(
      db.clone(),
      keyspace.subspace(&"SystemMeta").bytes(),
    ));
    data_store_generator = Box::new(move |namespace| {
      Box::new(FdbKvStore::new(
        db.clone(),
        &keyspace
          .subspace(&"D")
          .bytes()
          .iter()
          .copied()
          .chain(namespace.iter().copied())
          .collect::<Vec<u8>>(),
      ))
    });
  } else if let Some(x) = &opt.sqlite_db {
    if opt.fdb_cluster.is_some() || opt.fdb_keyspace.is_some() {
      panic!("cannot select multiple kv backends");
    }
    let backend = GlobalSqliteStore::open_leaky(x)?;
    system_store = Box::new(SqliteKvStore::new(backend.clone(), "system", b""));
    system_metadata_store = Box::new(SqliteKvStore::new(backend.clone(), "system_meta", b""));
    data_store_generator = Box::new(move |namespace| {
      Box::new(SqliteKvStore::new(backend.clone(), "user_data", namespace))
    });
  } else {
    panic!("no kv backend selected");
  }

  let system_schema = SystemSchema::new(
    opt.migration_hash.clone(),
    &*system_store,
    &*system_metadata_store,
  )
  .await;
  let query_cache = QueryCache::new(QueryCacheParams {
    process_memory_threshold_kb: opt.process_memory_threshold_kb,
  });

  set_state(ServerState {
    data_store_generator,
    system_store,
    system_schema,
    query_cache,
  });

  log::info!("RefineDB started.");

  let http_listen = opt.http_listen.clone();
  tokio::spawn(async move { run_http_server(http_listen).await });

  Server::builder()
    .add_service(RdbControlServer::new(ControlServer))
    .serve(opt.grpc_listen.parse()?)
    .await?;

  Ok(())
}
