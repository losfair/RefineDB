use std::{collections::HashMap, sync::Arc};

use anyhow::Result;

use crate::{schema::compile::CompiledSchema, storage_plan::StoragePlan};

use super::{
  bytecode::TwScript,
  vm_value::{VmType, VmValue},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum VmError {
  #[error("exported graph not found: `{0}`")]
  ExportedGraphNotFound(String),
}

pub struct TwVm<'a> {
  pub schema: &'a CompiledSchema,
  pub storage_plan: &'a StoragePlan,
  pub script: &'a TwScript,
  pub consts: Vec<Arc<VmValue<'a>>>,
  pub types: Vec<VmType<&'a str>>,
  pub exported_graph_name_index: HashMap<&'a str, usize>,
}

impl<'a> TwVm<'a> {
  pub fn new(
    schema: &'a CompiledSchema,
    storage_plan: &'a StoragePlan,
    script: &'a TwScript,
  ) -> Result<Self> {
    let consts = script
      .consts
      .iter()
      .map(|x| VmValue::from_const(schema, x).map(Arc::new))
      .collect::<Result<Vec<_>>>()?;
    let types = script
      .types
      .iter()
      .map(|x| VmType::<&'a str>::from(x))
      .collect::<Vec<_>>();

    let mut exported_graph_name_index = HashMap::new();
    for (i, g) in script.graphs.iter().enumerate() {
      if g.exported {
        exported_graph_name_index.insert(g.name.as_str(), i);
      }
    }

    Ok(Self {
      schema,
      storage_plan,
      script,
      consts,
      types,
      exported_graph_name_index,
    })
  }

  pub fn lookup_exported_graph_by_name(&self, name: &str) -> Result<usize> {
    Ok(
      self
        .exported_graph_name_index
        .get(name)
        .copied()
        .ok_or_else(|| VmError::ExportedGraphNotFound(name.into()))?,
    )
  }
}
