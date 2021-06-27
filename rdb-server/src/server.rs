use std::convert::TryFrom;

use async_trait::async_trait;
use bumpalo::Bump;
use maplit::btreemap;
use rand::RngCore;
use rdb_analyzer::data::fixup::migrate_schema;
use rdb_analyzer::data::treewalker::serialize::SerializedVmValue;
use rdb_analyzer::schema::compile::compile;
use rdb_analyzer::schema::grammar::parse;
use rdb_analyzer::storage_plan::planner::generate_plan_for_schema;
use rdb_analyzer::storage_plan::{StorageKey, StoragePlan};
use rdb_control_server::RdbControl;
use rdb_proto::proto::*;
use rdb_proto::tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::state::get_state;
use crate::sysquery::ns_to_kv_prefix;
use crate::util::current_millis;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
  #[error("invalid storage plan")]
  InvalidStoragePlan,

  #[error("namespace does not exist")]
  NonExistingNamespace,
}

pub struct ControlServer;

#[async_trait]
impl RdbControl for ControlServer {
  async fn create_namespace(
    &self,
    request: Request<CreateNamespaceRequest>,
  ) -> Result<Response<CreateNamespaceReply>, Status> {
    let r = request.get_ref();
    let st = get_state();

    let mut kv_prefix: [u8; 16] = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut kv_prefix);

    let res = st
      .system_schema
      .exec_ctx
      .run_exported_graph(
        &*st.system_store,
        "add_namespace",
        &[
          SerializedVmValue::Null(None),
          SerializedVmValue::String(r.id.clone()),
          SerializedVmValue::String(base64::encode(&kv_prefix)),
          SerializedVmValue::String(format!("{}", current_millis())),
        ],
      )
      .await
      .translate_err()?;
    res.check_nonnull().translate_err()?;
    let ok = res.try_unwrap_bool().translate_err()?;
    Ok(Response::new(CreateNamespaceReply { created: ok }))
  }

  async fn list_namespace(
    &self,
    _request: Request<ListNamespaceRequest>,
  ) -> Result<Response<ListNamespaceReply>, Status> {
    let st = get_state();
    let res = st
      .system_schema
      .exec_ctx
      .run_exported_graph(
        &*st.system_store,
        "list_namespaces",
        &[SerializedVmValue::Null(None)],
      )
      .await
      .translate_err()?;
    res.check_nonnull().translate_err()?;
    let res = res.try_unwrap_list().translate_err()?;
    let mut namespaces: Vec<NamespaceBasicInfo> = Vec::new();
    for x in res {
      let m = x.try_unwrap_map(&["id", "create_time"]).translate_err()?;
      let id = m.get("id").unwrap().try_unwrap_string().translate_err()?;
      let create_time: i64 = m
        .get("create_time")
        .unwrap()
        .try_unwrap_string()
        .translate_err()?
        .parse::<i64>()
        .translate_err()?;
      namespaces.push(NamespaceBasicInfo {
        id: id.clone(),
        create_time,
      });
    }
    Ok(Response::new(ListNamespaceReply { namespaces }))
  }

  async fn delete_namespace(
    &self,
    request: Request<DeleteNamespaceRequest>,
  ) -> Result<Response<DeleteNamespaceReply>, Status> {
    let r = request.get_ref();
    let st = get_state();
    let res = st
      .system_schema
      .exec_ctx
      .run_exported_graph(
        &*st.system_store,
        "delete_namespace",
        &[
          SerializedVmValue::Null(None),
          SerializedVmValue::String(r.id.clone()),
        ],
      )
      .await
      .translate_err()?;
    res.check_nonnull().translate_err()?;
    let ok = res.try_unwrap_bool().translate_err()?;
    Ok(Response::new(DeleteNamespaceReply { deleted: ok }))
  }

  async fn create_deployment(
    &self,
    request: Request<CreateDeploymentRequest>,
  ) -> Result<Response<CreateDeploymentReply>, Status> {
    let r = request.get_ref();
    let st = get_state();

    let id = Uuid::new_v4().to_string();
    let now = current_millis();

    let new_schema = compile(&parse(&Bump::new(), &r.schema).translate_err()?).translate_err()?;
    let new_plan: StoragePlan<String> = serde_yaml::from_str(&r.plan).translate_err()?;
    let new_plan = StoragePlan::<StorageKey>::try_from(&new_plan).translate_err()?;

    // Integrity check
    let generated_plan =
      generate_plan_for_schema(&new_plan, &new_schema, &new_schema).translate_err()?;
    if rmp_serde::to_vec_named(&generated_plan).translate_err()?
      != rmp_serde::to_vec_named(&new_plan).translate_err()?
    {
      Err(ServerError::InvalidStoragePlan).translate_err()?;
    }

    // Try migration
    let kv_prefix = ns_to_kv_prefix(&r.namespace_id).await.translate_err()?;
    let kv = (st.data_store_generator)(&kv_prefix);
    migrate_schema(&new_schema, &generated_plan, &*kv)
      .await
      .translate_err()?;

    // And finally, update our system schema.
    let res = st
      .system_schema
      .exec_ctx
      .run_exported_graph(
        &*st.system_store,
        "add_or_update_deployment",
        &[
          SerializedVmValue::Null(None),
          SerializedVmValue::String(r.namespace_id.clone()),
          SerializedVmValue::Map(btreemap! {
            "id".to_string() => SerializedVmValue::String(id.clone()),
            "description".to_string() => SerializedVmValue::String(r.description.clone()),
            "schema".to_string() => SerializedVmValue::String(r.schema.clone()),
            "plan".to_string() => SerializedVmValue::String(base64::encode(&generated_plan.serialize_compressed().translate_err()?)),
            "create_time".to_string() => SerializedVmValue::String(format!("{}", now)),
          }),
        ],
      )
      .await
      .translate_err()?;
    res.check_nonnull().translate_err()?;
    let ok = res.try_unwrap_bool().translate_err()?;
    Ok(Response::new(CreateDeploymentReply {
      deployment_id: ok.then(|| DeploymentId { id }),
    }))
  }

  async fn get_deployment(
    &self,
    request: Request<GetDeploymentRequest>,
  ) -> Result<Response<GetDeploymentReply>, Status> {
    let r = request.get_ref();
    let st = get_state();
    let res = st
      .system_schema
      .exec_ctx
      .run_exported_graph(
        &*st.system_store,
        "get_deployment",
        &[
          SerializedVmValue::Null(None),
          SerializedVmValue::String(r.namespace_id.clone()),
          SerializedVmValue::String(r.deployment_id.clone()),
        ],
      )
      .await
      .translate_err()?;
    let res = match res {
      SerializedVmValue::Null(_) => GetDeploymentReply { info: None },
      _ => {
        let res = res
          .try_unwrap_map(&["id", "create_time", "description", "schema", "plan"])
          .translate_err()?;
        GetDeploymentReply {
          info: Some(DeploymentFullInfo {
            id: res
              .get("id")
              .unwrap()
              .try_unwrap_string()
              .translate_err()?
              .clone(),
            create_time: res
              .get("create_time")
              .unwrap()
              .try_unwrap_string()
              .translate_err()?
              .parse()
              .translate_err()?,
            description: res
              .get("description")
              .unwrap()
              .try_unwrap_string()
              .translate_err()?
              .clone(),
            schema: res
              .get("schema")
              .unwrap()
              .try_unwrap_string()
              .translate_err()?
              .clone(),
            plan: serde_yaml::to_string(&StoragePlan::<String>::from(
              &StoragePlan::deserialize_compressed(
                &base64::decode(
                  res
                    .get("plan")
                    .unwrap()
                    .try_unwrap_string()
                    .translate_err()?,
                )
                .translate_err()?,
              )
              .translate_err()?,
            ))
            .translate_err()?,
          }),
        }
      }
    };
    Ok(Response::new(res))
  }

  async fn list_deployment(
    &self,
    request: Request<ListDeploymentRequest>,
  ) -> Result<Response<ListDeploymentReply>, Status> {
    let r = request.get_ref();
    let st = get_state();
    let res = st
      .system_schema
      .exec_ctx
      .run_exported_graph(
        &*st.system_store,
        "list_deployment",
        &[
          SerializedVmValue::Null(None),
          SerializedVmValue::String(r.namespace_id.clone()),
        ],
      )
      .await
      .translate_err()?;
    res.check_nonnull().translate_err()?;
    let res = res.try_unwrap_list().translate_err()?;
    let mut deployments: Vec<DeploymentBasicInfo> = Vec::new();
    for x in res {
      let m = x
        .try_unwrap_map(&["id", "create_time", "description"])
        .translate_err()?;
      let id = m.get("id").unwrap().try_unwrap_string().translate_err()?;
      let create_time: i64 = m
        .get("create_time")
        .unwrap()
        .try_unwrap_string()
        .translate_err()?
        .parse::<i64>()
        .translate_err()?;
      let description = m
        .get("description")
        .unwrap()
        .try_unwrap_string()
        .translate_err()?;
      deployments.push(DeploymentBasicInfo {
        id: id.clone(),
        create_time,
        description: description.clone(),
      });
    }
    Ok(Response::new(ListDeploymentReply { deployments }))
  }

  async fn create_query_script(
    &self,
    request: Request<CreateQueryScriptRequest>,
  ) -> Result<Response<CreateQueryScriptReply>, Status> {
    todo!()
  }

  async fn delete_query_script(
    &self,
    request: Request<DeleteQueryScriptRequest>,
  ) -> Result<Response<DeleteQueryScriptReply>, Status> {
    todo!()
  }
}

trait ErrorTranslate {
  type Output;
  fn translate_err(self) -> Result<Self::Output, Status>;
}

impl<T, E> ErrorTranslate for Result<T, E>
where
  anyhow::Error: From<E>,
{
  type Output = T;

  fn translate_err(self) -> Result<Self::Output, Status> {
    self.map_err(|x| {
      let x = anyhow::Error::from(x);
      log::error!("request error: {:?}", x);
      Status::internal(format!("{:?}", x))
    })
  }
}
