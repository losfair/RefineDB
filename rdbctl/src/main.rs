use anyhow::Result;

use clap::{AppSettings, Clap};
use rdb_proto::{proto::{CreateNamespaceRequest, DeleteNamespaceRequest, ListNamespaceRequest, rdb_control_client::RdbControlClient}, tonic::Request};

/// RefineDB CLI.
#[derive(Clap)]
#[clap(version = "0.1", author = "Heyang Zhou <zhy20000919@hotmail.com>")]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
  /// Server URL.
  #[clap(short, long)]
  server: String,
  #[clap(subcommand)]
  subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
  /// Create a namespace.
  CreateNamespace(CreateNamespace),

  /// List namespaces.
  ListNamespace(ListNamespace),

  /// Delete a namespace.
  DeleteNamespace(DeleteNamespace),
}

#[derive(Clap)]
struct CreateNamespace {
  namespace_id: String,
}

#[derive(Clap)]
struct ListNamespace {
}

#[derive(Clap)]
struct DeleteNamespace {
  namespace_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
  if std::env::var("RUST_LOG").is_err() {
    std::env::set_var("RUST_LOG", "info");
  }
  let opts: Opts = Opts::parse();
  let mut client = RdbControlClient::connect(opts.server.clone()).await?;

  match &opts.subcmd {
    SubCommand::CreateNamespace(x) => {
      let req = Request::new(CreateNamespaceRequest {
        id: x.namespace_id.clone(),
      });
      let res = client.create_namespace(req).await?;
      println!("{:?}", res);
    }
    SubCommand::ListNamespace(_) => {
      let req = Request::new(ListNamespaceRequest {});
      let res = client.list_namespace(req).await?;
      println!("{:?}", res);
    }
    SubCommand::DeleteNamespace(x) => {
      let req = Request::new(DeleteNamespaceRequest {
        id: x.namespace_id.clone(),
      });
      let res = client.delete_namespace(req).await?;
      println!("{:?}", res);
    }
  }

  Ok(())
}