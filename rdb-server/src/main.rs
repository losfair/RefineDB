use std::sync::Arc;

use anyhow::Result;
use foundationdb::{tuple::Subspace, Database};
use rdb_analyzer::data::kv::KeyValueStore;
use rdb_proto::{proto::rdb_control_server::RdbControlServer, tonic::transport::Server};
use structopt::StructOpt;
use tokio::runtime::Runtime;

use crate::{
  kv_backend::foundationdb::FdbKvStore,
  opt::Opt,
  server::ControlServer,
  state::{set_state, DataStoreGenerator, ServerState},
  system::SystemSchema,
};
mod exec;
mod exec_core;
mod kv_backend;
mod opt;
mod server;
mod state;
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
        keyspace.subspace(&"D").subspace(&namespace).bytes(),
      ))
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

  set_state(ServerState {
    data_store_generator,
    system_store,
    system_schema,
  });

  log::info!("RefineDB started.");

  Server::builder()
    .add_service(RdbControlServer::new(ControlServer))
    .serve(opt.listen.parse()?)
    .await?;

  Ok(())
}
