use anyhow::Result;
use rdb_analyzer::data::treewalker::serialize::SerializedVmValue;

use crate::state::get_state;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SysQueryError {
  #[error("namespace not found")]
  NamespaceNotFound,

  #[error("query script not found")]
  QueryScriptNotFound,
}

pub struct QueryScript {
  pub id: String,
  pub create_time: i64,
  pub associated_deployment: String,
  pub script: String,
}

pub struct Deployment {
  pub id: String,
  pub description: String,
  pub schema: String,
  pub plan: Vec<u8>,
  pub create_time: i64,
}

pub async fn ns_to_kv_prefix_with_appended_zero(ns_id: &str) -> Result<Vec<u8>> {
  let st = get_state();
  let res = st
    .system_schema
    .exec_ctx
    .run_exported_graph(
      &*st.system_store,
      "ns_to_kv_prefix",
      &[
        SerializedVmValue::Null(None),
        SerializedVmValue::String(ns_id.into()),
      ],
    )
    .await?;
  match res {
    SerializedVmValue::Null(_) => Err(SysQueryError::NamespaceNotFound.into()),
    _ => Ok({
      let mut x = base64::decode(res.try_unwrap_string()?)?;
      x.push(0);
      x
    }),
  }
}

pub async fn lookup_query_script(ns_id: &str, qs_id: &str) -> Result<QueryScript> {
  let st = get_state();
  let res = st
    .system_schema
    .exec_ctx
    .run_exported_graph(
      &*st.system_store,
      "get_query_script",
      &[
        SerializedVmValue::Null(None),
        SerializedVmValue::String(ns_id.into()),
        SerializedVmValue::String(qs_id.into()),
      ],
    )
    .await?;
  match res {
    SerializedVmValue::Null(_) => Err(SysQueryError::QueryScriptNotFound.into()),
    _ => {
      let m = res.try_unwrap_map(&["id", "create_time", "associated_deployment", "script"])?;
      Ok(QueryScript {
        id: m.get("id").unwrap().try_unwrap_string()?.clone(),
        create_time: m.get("id").unwrap().try_unwrap_string()?.parse()?,
        associated_deployment: m
          .get("associated_deployment")
          .unwrap()
          .try_unwrap_string()?
          .clone(),
        script: m.get("script").unwrap().try_unwrap_string()?.clone(),
      })
    }
  }
}

pub async fn lookup_deployment(namespace_id: &str, deployment_id: &str) -> Result<Deployment> {
  let st = get_state();
  let res = st
    .system_schema
    .exec_ctx
    .run_exported_graph(
      &*st.system_store,
      "get_deployment",
      &[
        SerializedVmValue::Null(None),
        SerializedVmValue::String(namespace_id.into()),
        SerializedVmValue::String(deployment_id.into()),
      ],
    )
    .await?;
  let res = res.try_unwrap_map(&["id", "create_time", "description", "schema", "plan"])?;
  let depl = Deployment {
    id: res.get("id").unwrap().try_unwrap_string()?.clone(),
    description: res.get("description").unwrap().try_unwrap_string()?.clone(),
    schema: res.get("schema").unwrap().try_unwrap_string()?.clone(),
    plan: base64::decode(res.get("plan").unwrap().try_unwrap_string()?)?,
    create_time: res
      .get("create_time")
      .unwrap()
      .try_unwrap_string()?
      .parse()?,
  };
  Ok(depl)
}
