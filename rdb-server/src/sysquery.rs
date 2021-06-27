use anyhow::Result;
use rdb_analyzer::data::treewalker::serialize::SerializedVmValue;

use crate::state::get_state;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SysQueryError {
  #[error("namespace not found")]
  NamespaceNotFound,
}

pub async fn ns_to_kv_prefix(ns_id: &str) -> Result<Vec<u8>> {
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
    _ => Ok(base64::decode(res.try_unwrap_string()?)?),
  }
}
