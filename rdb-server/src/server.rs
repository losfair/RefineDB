use std::convert::TryFrom;
use std::sync::Arc;

use async_trait::async_trait;
use bumpalo::Bump;
use maplit::btreemap;
use rand::RngCore;
use rdb_analyzer::data::fixup::migrate_schema;
use rdb_analyzer::data::treewalker::serialize::{SerializedVmValue, TaggedVmValue};
use rdb_analyzer::schema::compile::compile;
use rdb_analyzer::schema::grammar::parse;
use rdb_analyzer::storage_plan::planner::generate_plan_for_schema;
use rdb_analyzer::storage_plan::{StorageKey, StoragePlan};
use rdb_control_server::RdbControl;
use rdb_proto::proto::*;
use rdb_proto::tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::exec_core::{ExecContext, SchemaContext};
use crate::state::get_state;
use crate::sysquery::{lookup_deployment, lookup_query_script, ns_to_kv_prefix_with_appended_zero};
use crate::util::current_millis;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
  #[error("invalid storage plan")]
  InvalidStoragePlan,
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

    // Delete all data in this namespace
    if let Ok(mut kv_prefix) = ns_to_kv_prefix_with_appended_zero(&r.id).await {
      // Remove trailing zero
      let popped = kv_prefix.pop().unwrap();
      assert_eq!(popped, 0);

      let full_range = (st.data_store_generator)(&kv_prefix);
      let txn = full_range.begin_transaction().await.translate_err()?;
      txn.delete_range(&[0x00], &[0x01]).await.translate_err()?;
      txn.commit().await.translate_err()?;
    }

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
    let kv_prefix = ns_to_kv_prefix_with_appended_zero(&r.namespace_id)
      .await
      .translate_err()?;
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
        "add_deployment",
        &[
          SerializedVmValue::Null(None),
          SerializedVmValue::String(r.namespace_id.clone()),
          SerializedVmValue::Tagged(TaggedVmValue::M(btreemap! {
            "id".to_string() => SerializedVmValue::String(id.clone()),
            "description".to_string() => SerializedVmValue::String(r.description.clone()),
            "schema".to_string() => SerializedVmValue::String(r.schema.clone()),
            "plan".to_string() => SerializedVmValue::String(base64::encode(&generated_plan.serialize_compressed().translate_err()?)),
            "create_time".to_string() => SerializedVmValue::String(format!("{}", now)),
          })),
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

  async fn delete_deployment(
    &self,
    request: Request<DeleteDeploymentRequest>,
  ) -> Result<Response<DeleteDeploymentReply>, Status> {
    let r = request.get_ref();
    let st = get_state();
    let res = st
      .system_schema
      .exec_ctx
      .run_exported_graph(
        &*st.system_store,
        "delete_deployment",
        &[
          SerializedVmValue::Null(None),
          SerializedVmValue::String(r.namespace_id.clone()),
          SerializedVmValue::String(r.id.clone()),
        ],
      )
      .await
      .translate_err()?;
    res.check_nonnull().translate_err()?;
    let deleted = res.try_unwrap_bool().translate_err()?;
    Ok(Response::new(DeleteDeploymentReply { deleted }))
  }

  async fn create_query_script(
    &self,
    request: Request<CreateQueryScriptRequest>,
  ) -> Result<Response<CreateQueryScriptReply>, Status> {
    let r = request.get_ref();
    let st = get_state();

    let depl = lookup_deployment(&r.namespace_id, &r.associated_deployment)
      .await
      .translate_err()?;

    // Validation
    let schema = compile(&parse(&Bump::new(), &depl.schema).translate_err()?).translate_err()?;
    let plan = StoragePlan::deserialize_compressed(&depl.plan).translate_err()?;
    let schema_ctx = Arc::new(SchemaContext { schema, plan });
    ExecContext::load(schema_ctx, &r.script).translate_err()?;

    let res = st
      .system_schema
      .exec_ctx
      .run_exported_graph(
        &*st.system_store,
        "add_or_update_query_script",
        &[
          SerializedVmValue::Null(None),
          SerializedVmValue::String(r.namespace_id.clone()),
          SerializedVmValue::Tagged(TaggedVmValue::M(btreemap! {
            "id".to_string() => SerializedVmValue::String(r.id.clone()),
            "associated_deployment".to_string() => SerializedVmValue::String(r.associated_deployment.clone()),
            "script".to_string() => SerializedVmValue::String(r.script.clone()),
            "create_time".to_string() => SerializedVmValue::String(format!("{}", current_millis())),
          })),
        ],
      )
      .await
      .translate_err()?;
    res.check_nonnull().translate_err()?;
    let created = res.try_unwrap_bool().translate_err()?;
    Ok(Response::new(CreateQueryScriptReply { created }))
  }

  async fn delete_query_script(
    &self,
    request: Request<DeleteQueryScriptRequest>,
  ) -> Result<Response<DeleteQueryScriptReply>, Status> {
    let r = request.get_ref();
    let st = get_state();
    let res = st
      .system_schema
      .exec_ctx
      .run_exported_graph(
        &*st.system_store,
        "delete_query_script",
        &[
          SerializedVmValue::Null(None),
          SerializedVmValue::String(r.namespace_id.clone()),
          SerializedVmValue::String(r.id.clone()),
        ],
      )
      .await
      .translate_err()?;
    res.check_nonnull().translate_err()?;
    let deleted = res.try_unwrap_bool().translate_err()?;
    Ok(Response::new(DeleteQueryScriptReply { deleted }))
  }

  async fn get_query_script(
    &self,
    request: Request<GetQueryScriptRequest>,
  ) -> Result<Response<GetQueryScriptReply>, Status> {
    let r = request.get_ref();
    let qs = lookup_query_script(&r.namespace_id, &r.query_script_id)
      .await
      .translate_err()?;
    Ok(Response::new(GetQueryScriptReply {
      info: Some(QueryScriptFullInfo {
        id: qs.id,
        associated_deployment: qs.associated_deployment,
        script: qs.script,
        create_time: qs.create_time,
      }),
    }))
  }

  async fn list_query_script(
    &self,
    request: Request<ListQueryScriptRequest>,
  ) -> Result<Response<ListQueryScriptReply>, Status> {
    let r = request.get_ref();
    let st = get_state();
    let res = st
      .system_schema
      .exec_ctx
      .run_exported_graph(
        &*st.system_store,
        "list_query_script",
        &[
          SerializedVmValue::Null(None),
          SerializedVmValue::String(r.namespace_id.clone()),
        ],
      )
      .await
      .translate_err()?;
    res.check_nonnull().translate_err()?;
    let res = res.try_unwrap_list().translate_err()?;
    let mut query_scripts: Vec<QueryScriptBasicInfo> = Vec::new();
    for x in res {
      let m = x
        .try_unwrap_map(&["id", "associated_deployment", "create_time"])
        .translate_err()?;
      let id = m.get("id").unwrap().try_unwrap_string().translate_err()?;
      let associated_deployment = m
        .get("associated_deployment")
        .unwrap()
        .try_unwrap_string()
        .translate_err()?;
      let create_time: i64 = m
        .get("create_time")
        .unwrap()
        .try_unwrap_string()
        .translate_err()?
        .parse::<i64>()
        .translate_err()?;
      query_scripts.push(QueryScriptBasicInfo {
        id: id.clone(),
        associated_deployment: associated_deployment.clone(),
        create_time,
      });
    }
    Ok(Response::new(ListQueryScriptReply { query_scripts }))
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
