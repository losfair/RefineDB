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
  #[structopt(short, long)]
  pub listen: String,

  /// Migration hash.
  #[structopt(long)]
  pub migration_hash: Option<String>,
}
