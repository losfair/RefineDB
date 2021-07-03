use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

use anyhow::Result;
use futures::FutureExt;
use rdb_analyzer::data::{
  kv::KeyValueStore,
  treewalker::{exec::Executor, serialize::SerializedVmValue, vm_value::VmType},
};
use tokio::{task::yield_now, time::sleep};

use crate::exec_core::ExecContext;
use thiserror::Error;

const QUERY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Error, Debug)]
pub enum ExecError {
  #[error("graph executor panicked")]
  GraphExecutorPanic,

  #[error("param count mismatch: expected {0}, got {1}")]
  ParamCountMismatch(usize, usize),

  #[error("query timeout")]
  Timeout,
}

impl ExecContext {
  pub async fn run_exported_graph(
    &self,
    kv: &dyn KeyValueStore,
    name: &str,
    params: &[SerializedVmValue],
  ) -> Result<SerializedVmValue> {
    let run_fut = AssertUnwindSafe(self.run_exported_graph_inner(kv, name, params)).catch_unwind();
    let timeout_fut = sleep(QUERY_TIMEOUT);
    tokio::select! {
      res = run_fut => {
        res.unwrap_or_else(|_| Err(ExecError::GraphExecutorPanic.into()))
      }
      _ = timeout_fut => Err(ExecError::Timeout.into()),
    }
  }

  async fn run_exported_graph_inner(
    &self,
    kv: &dyn KeyValueStore,
    name: &str,
    params: &[SerializedVmValue],
  ) -> Result<SerializedVmValue> {
    let graph_index = self.vm().lookup_exported_graph_by_name(name)?;
    let param_types = &self.type_info().graphs[graph_index].params;

    // We also need raw types because we need a way to detect the `Schema` pseudo-type.
    let raw_param_types = self.vm().script.graphs[graph_index]
      .param_types
      .iter()
      .map(|x| &self.vm().types[*x as usize])
      .collect::<Vec<_>>();
    assert_eq!(param_types.len(), raw_param_types.len());
    if param_types.len() != params.len() {
      return Err(ExecError::ParamCountMismatch(param_types.len(), params.len()).into());
    }
    let mut executor = Executor::new(self.vm(), kv, self.type_info());
    executor.set_yield_fn(|| Box::pin(yield_now()));
    executor.set_sleep_fn(|x| Box::pin(sleep(x)));
    let params = params
      .iter()
      .zip(param_types)
      .zip(raw_param_types)
      .map(|((v, ty), raw_ty)| match raw_ty {
        VmType::Schema => Ok(self.root_map().clone()),
        _ => v.decode(ty).map(Arc::new),
      })
      .collect::<Result<Vec<_>>>()?;
    let output = executor
      .run_graph(graph_index, &params)
      .await?
      .map(|x| SerializedVmValue::encode(&*x, &Default::default()))
      .transpose()?;
    Ok(output.unwrap_or_else(|| SerializedVmValue::Null(None)))
  }
}
