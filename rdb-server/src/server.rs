use async_trait::async_trait;
use rdb_analyzer::data::treewalker::serialize::SerializedVmValue;
use rdb_control_server::RdbControl;
use rdb_proto::proto::*;
use rdb_proto::tonic::{Request, Response, Status};

use crate::state::get_state;
use crate::util::current_millis;

pub struct ControlServer;

#[async_trait]
impl RdbControl for ControlServer {
  async fn create_namespace(
    &self,
    request: Request<CreateNamespaceRequest>,
  ) -> Result<Response<CreateNamespaceReply>, Status> {
    let r = request.get_ref();
    let st = get_state();
    let res = st
      .system_schema
      .exec_ctx
      .run_exported_graph(
        &*st.system_store,
        "add_namespace",
        &[
          SerializedVmValue::Null(None),
          SerializedVmValue::String(r.id.clone()),
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
    todo!()
  }

  async fn get_deployment(
    &self,
    request: Request<GetDeploymentRequest>,
  ) -> Result<Response<GetDeploymentReply>, Status> {
    todo!()
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
        "list_deployments",
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
      let m = x.try_unwrap_map(&["id", "create_time"]).translate_err()?;
      let id = m.get("id").unwrap().try_unwrap_string().translate_err()?;
      let create_time: i64 = m
        .get("create_time")
        .unwrap()
        .try_unwrap_string()
        .translate_err()?
        .parse::<i64>()
        .translate_err()?;
      deployments.push(DeploymentBasicInfo {
        id: id.clone(),
        create_time,
      });
    }
    Ok(Response::new(ListDeploymentReply { deployments }))
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
