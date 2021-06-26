use std::{panic::AssertUnwindSafe, sync::Arc};

use anyhow::Result;
use futures::FutureExt;
use rdb_analyzer::data::{
  kv::KeyValueStore,
  treewalker::{exec::Executor, serialize::SerializedVmValue, vm_value::VmType},
};

use crate::exec_core::ExecContext;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecError {
  #[error("graph executor panicked")]
  GraphExecutorPanic,

  #[error("param count mismatch: expected {0}, got {1}")]
  ParamCountMismatch(usize, usize),
}

impl ExecContext {
  pub async fn run_exported_graph(
    &self,
    kv: &dyn KeyValueStore,
    name: &str,
    params: &[SerializedVmValue],
  ) -> Result<SerializedVmValue> {
    let graph_index = self.vm().lookup_exported_graph_by_name(name)?;
    let param_types = &self.type_info().graphs[graph_index].params;
    if param_types.len() != params.len() {
      return Err(ExecError::ParamCountMismatch(param_types.len(), params.len()).into());
    }
    let executor = Executor::new(self.vm(), kv, self.type_info());
    let params = params
      .iter()
      .zip(param_types)
      .map(|(v, ty)| match ty {
        VmType::Schema => Ok(self.root_map().clone()),
        _ => v.decode(ty).map(Arc::new),
      })
      .collect::<Result<Vec<_>>>()?;
    let output = AssertUnwindSafe(executor.run_graph(graph_index, &params))
      .catch_unwind()
      .await
      .unwrap_or_else(|_| Err(ExecError::GraphExecutorPanic.into()))?
      .map(|x| SerializedVmValue::encode(&*x))
      .transpose()?;
    Ok(output.unwrap_or_else(|| SerializedVmValue::Null(None)))
  }
}
