use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "rdb-server", about = "RefineDB server.")]
pub struct Opt {
  /// FoundationDB cluster file.
  #[structopt(long)]
  pub fdb_cluster: Option<String>,

  /// FoundationDB root keyspace.
  #[structopt(long)]
  pub fdb_keyspace: Option<String>,

  /// GRPC listen address.
  #[structopt(long)]
  pub grpc_listen: String,

  /// HTTP API listen address.
  #[structopt(long)]
  pub http_listen: String,

  /// Migration hash.
  #[structopt(long)]
  pub migration_hash: Option<String>,

  /// Process memory threshold (in KiB) for query cache.
  #[structopt(long, default_value = "524288")]
  pub process_memory_threshold_kb: u64,
}
