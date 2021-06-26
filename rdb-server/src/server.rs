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
    let ok = res.try_unwrap_bool().translate_err()?;
    Ok(Response::new(CreateNamespaceReply { created: ok }))
  }

  async fn delete_namespace(
    &self,
    request: Request<DeleteNamespaceRequest>,
  ) -> Result<Response<DeleteNamespaceReply>, Status> {
    todo!()
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

  async fn list_deployments(
    &self,
    request: Request<ListDeploymentsRequest>,
  ) -> Result<Response<ListDeploymentsReply>, Status> {
    todo!()
  }
}

trait ErrorTranslate {
  type Output;
  fn translate_err(self) -> Result<Self::Output, Status>;
}

impl<T> ErrorTranslate for Result<T, anyhow::Error> {
  type Output = T;

  fn translate_err(self) -> Result<Self::Output, Status> {
    self.map_err(|x| {
      log::error!("request error: {:?}", x);
      Status::internal(format!("{:?}", x))
    })
  }
}
