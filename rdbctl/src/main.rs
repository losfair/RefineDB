use std::convert::TryFrom;

use anyhow::Result;

use bumpalo::Bump;
use clap::{AppSettings, Clap};
use rdb_analyzer::{
  schema::{compile::compile, grammar::parse},
  storage_plan::{planner::generate_plan_for_schema, StorageKey, StoragePlan},
};
use rdb_proto::{
  proto::{
    rdb_control_client::RdbControlClient, CreateDeploymentRequest, CreateNamespaceRequest,
    DeleteNamespaceRequest, GetDeploymentRequest, ListDeploymentRequest, ListNamespaceRequest,
  },
  tonic::Request,
};
use thiserror::Error;

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

  /// Create a deployment.
  CreateDeployment(CreateDeployment),

  /// List deployments.
  ListDeployment(ListDeployment),
}

#[derive(Clap)]
struct CreateNamespace {
  namespace_id: String,
}

#[derive(Clap)]
struct ListNamespace {}

#[derive(Clap)]
struct DeleteNamespace {
  namespace_id: String,
}

#[derive(Clap)]
struct CreateDeployment {
  /// The source deployment to migrate from.
  #[clap(short, long)]
  migrate_from: Option<String>,

  /// Path to the new schema.
  #[clap(short, long)]
  schema: String,

  /// Deployment description.
  #[clap(short, long)]
  description: Option<String>,

  /// Namespace id.
  #[clap(short, long)]
  namespace: String,
}

#[derive(Clap)]
struct ListDeployment {
  namespace_id: String,
}

#[derive(Error, Debug)]
enum CliError {
  #[error("reference deployment not found")]
  ReferenceDeploymentNotFound,

  #[error("deployment not created")]
  DeploymentNotCreated,
}

#[tokio::main]
async fn main() -> Result<()> {
  if std::env::var("RUST_LOG").is_err() {
    std::env::set_var("RUST_LOG", "info");
  }
  pretty_env_logger::init_timed();
  let opts: Opts = Opts::parse();
  let mut client = RdbControlClient::connect(opts.server.clone()).await?;

  match &opts.subcmd {
    SubCommand::CreateNamespace(x) => {
      let req = Request::new(CreateNamespaceRequest {
        id: x.namespace_id.clone(),
      });
      let res = client.create_namespace(req).await?;
      println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
          "created": res.get_ref().created,
        }))?
      );
    }
    SubCommand::ListNamespace(_) => {
      let req = Request::new(ListNamespaceRequest {});
      let res = client.list_namespace(req).await?;
      println!(
        "{}",
        serde_json::to_string(
          &res
            .get_ref()
            .namespaces
            .iter()
            .map(|x| serde_json::json!({
              "id": x.id,
              "create_time": x.create_time,
            }))
            .collect::<Vec<_>>()
        )?
      );
    }
    SubCommand::DeleteNamespace(x) => {
      let req = Request::new(DeleteNamespaceRequest {
        id: x.namespace_id.clone(),
      });
      let res = client.delete_namespace(req).await?;
      println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
          "deleted": res.get_ref().deleted,
        }))?
      );
    }
    SubCommand::CreateDeployment(subopts) => {
      let schema_text = std::fs::read_to_string(&subopts.schema)?;

      let new_schema = compile(&parse(&Bump::new(), &schema_text)?)?;
      let new_plan = if let Some(reference) = &subopts.migrate_from {
        let reference_deployment = client
          .get_deployment(Request::new(GetDeploymentRequest {
            namespace_id: subopts.namespace.clone(),
            deployment_id: reference.clone(),
          }))
          .await?;
        let info = reference_deployment
          .get_ref()
          .info
          .as_ref()
          .ok_or_else(|| CliError::ReferenceDeploymentNotFound)?;
        let reference_schema = compile(&parse(&Bump::new(), &info.schema)?)?;
        let reference_plan: StoragePlan<String> = serde_yaml::from_str(&info.plan)?;
        let reference_plan = StoragePlan::<StorageKey>::try_from(&reference_plan)?;
        let new_plan = generate_plan_for_schema(&reference_plan, &reference_schema, &new_schema)?;
        log::info!("Storage plan migrated from reference deployment.");
        new_plan
      } else {
        generate_plan_for_schema(&Default::default(), &Default::default(), &new_schema)?
      };

      let res = client
        .create_deployment(Request::new(CreateDeploymentRequest {
          namespace_id: subopts.namespace.clone(),
          schema: schema_text,
          plan: serde_yaml::to_string(&StoragePlan::<String>::from(&new_plan))?,
          description: subopts.description.clone().unwrap_or_default(),
        }))
        .await?;
      let deployment_id = res
        .get_ref()
        .deployment_id
        .as_ref()
        .ok_or_else(|| CliError::DeploymentNotCreated)?;
      println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
          "id": deployment_id.id,
        }))?
      );
    }
    SubCommand::ListDeployment(subopts) => {
      let req = Request::new(ListDeploymentRequest {
        namespace_id: subopts.namespace_id.clone(),
      });
      let res = client.list_deployment(req).await?;
      println!(
        "{}",
        serde_json::to_string(
          &res
            .get_ref()
            .deployments
            .iter()
            .map(|x| serde_json::json!({
              "id": x.id,
              "create_time": x.create_time,
              "description": x.description,
            }))
            .collect::<Vec<_>>()
        )?
      );
    }
  }

  Ok(())
}
