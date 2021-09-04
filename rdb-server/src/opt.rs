use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "rdb-server", about = "RefineDB server.")]
pub struct Opt {
  /// FoundationDB cluster file.
  #[structopt(long, env = "RDB_FDB_CLUSTER")]
  pub fdb_cluster: Option<String>,

  /// FoundationDB root keyspace.
  #[structopt(long, env = "RDB_FDB_KEYSPACE")]
  pub fdb_keyspace: Option<String>,

  /// Path to the SQLite database.
  #[structopt(long, env = "RDB_SQLITE_DB")]
  pub sqlite_db: Option<String>,

  /// GRPC listen address.
  #[structopt(long, env = "RDB_GRPC_LISTEN")]
  pub grpc_listen: String,

  /// HTTP API listen address.
  #[structopt(long, env = "RDB_HTTP_LISTEN")]
  pub http_listen: String,

  /// Migration hash.
  #[structopt(long, env = "RDB_MIGRATION_HASH")]
  pub migration_hash: Option<String>,

  /// Process memory threshold (in KiB) for query cache.
  #[structopt(
    long,
    default_value = "524288",
    env = "RDB_PROCESS_MEMORY_THRESHOLD_KB"
  )]
  pub process_memory_threshold_kb: u64,
}
