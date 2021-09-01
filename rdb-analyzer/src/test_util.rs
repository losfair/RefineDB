use crate::data::kv::KeyValueStore;

#[cfg(feature = "test-with-fdb")]
fn ensure_fdb_ready() {
  use foundationdb::{tuple::Subspace, Database};
  use std::sync::Once;
  static FDB_BOOT: Once = Once::new();
  FDB_BOOT.call_once(|| {
    let network = unsafe { foundationdb::boot() };
    std::mem::forget(network);

    std::thread::spawn(|| {
      tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
          let db = Database::default().unwrap();
          let txn = db.create_trx().unwrap();
          txn.clear_subspace_range(&Subspace::all().subspace(&"rdbtest"));
          txn.commit().await.unwrap();
        });
    })
    .join()
    .unwrap_or_else(|_| panic!("db init failed"))
  });
}

#[cfg(not(any(feature = "test-with-fdb", feature = "test-with-sqlite")))]
pub fn create_kv() -> Box<dyn KeyValueStore> {
  use crate::kv_backend::mock_kv::MockKv;
  Box::new(MockKv::new())
}

#[cfg(feature = "test-with-fdb")]
pub fn create_kv() -> Box<dyn KeyValueStore> {
  use crate::kv_backend::foundationdb::FdbKvStore;
  use foundationdb::{tuple::Subspace, Database};
  use rand::RngCore;
  use std::sync::Arc;
  ensure_fdb_ready();

  let mut isolation_id = [0u8; 16];
  rand::thread_rng().fill_bytes(&mut isolation_id[..]);
  let isolation_id = hex::encode(&isolation_id);

  Box::new(FdbKvStore::new(
    Arc::new(Database::default().unwrap()),
    Subspace::all()
      .subspace(&format!("rdbtest"))
      .subspace(&isolation_id)
      .bytes(),
  ))
}
