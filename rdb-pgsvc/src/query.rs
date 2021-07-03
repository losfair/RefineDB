use std::sync::Arc;

use anyhow::Result;
use rdb_analyzer::{
  data::{
    fixup::migrate_schema,
    kv::KeyValueStore,
    treewalker::{
      bytecode::TwGraph,
      exec::{generate_root_map, Executor},
      serialize::{SerializedVmValue, TaggedVmValue},
      typeck::GlobalTypeInfo,
      vm::TwVm,
      vm_value::VmType,
    },
  },
  schema::compile::PrimitiveType,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Serialize, Default)]
pub struct VmGlobalGraphInfo {
  pub graphs: Vec<VmGraphInfo>,
}

#[derive(Serialize)]
pub struct VmGraphInfo {
  pub name: String,
  pub query_template: String,
  pub params: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct VmGraphQuery {
  pub graph: String,
  pub params: Vec<SerializedVmValue>,
}

#[derive(Error, Debug)]
pub enum QueryError {
  #[error("graph not found")]
  GraphNotFound,

  #[error("param count mismatch")]
  ParamCountMismatch,
}

pub fn get_vm_graphs(vm: &TwVm) -> VmGlobalGraphInfo {
  let mut res = VmGlobalGraphInfo::default();
  for g in &vm.script.graphs {
    if !g.exported {
      continue;
    }

    let query_template = match generate_example_query(vm, g) {
      Ok(x) => serde_json::to_string_pretty(&x).unwrap(),
      Err(e) => {
        log::error!("generate_example_query error: {:?}", e);
        "// example query generation failed".into()
      }
    };

    res.graphs.push(VmGraphInfo {
      name: g.name.clone(),
      query_template,
      params: g
        .param_types
        .iter()
        .map(|x| format!("{}", vm.types[*x as usize]))
        .collect(),
    });
  }
  res
}

pub fn run_vm_query<'a>(
  vm: &TwVm<'a>,
  kv: &dyn KeyValueStore,
  type_info: &GlobalTypeInfo<'a>,
  query: &VmGraphQuery,
) -> Result<Option<SerializedVmValue>> {
  futures::executor::block_on(migrate_schema(&vm.schema, &vm.storage_plan, kv))?;
  let mut executor = Executor::new(vm, kv, type_info);
  let (i, g) = vm
    .script
    .graphs
    .iter()
    .enumerate()
    .find(|(_, x)| x.name == query.graph)
    .ok_or_else(|| QueryError::GraphNotFound)?;
  if query.params.len() != g.param_types.len() {
    return Err(QueryError::ParamCountMismatch.into());
  }
  let param_types = g
    .param_types
    .iter()
    .map(|x| &vm.types[*x as usize])
    .collect::<Vec<_>>();
  let params = query
    .params
    .iter()
    .zip(param_types.iter())
    .map(|(x, ty)| match ty {
      VmType::Schema => generate_root_map(vm.schema, vm.storage_plan).map(Arc::new),
      _ => x.decode(ty).map(Arc::new),
    })
    .collect::<Result<Vec<_>>>()?;
  let res = futures::executor::block_on(executor.run_graph(i, &params))?;
  Ok(
    res
      .map(|x| SerializedVmValue::encode(&*x, &Default::default()))
      .transpose()?,
  )
}

fn generate_example_query(vm: &TwVm, g: &TwGraph) -> Result<VmGraphQuery> {
  let params = g
    .param_types
    .iter()
    .map(|x| &vm.types[*x as usize])
    .map(|x| generate_example_param(x))
    .collect::<Result<Vec<_>>>()?;
  Ok(VmGraphQuery {
    graph: g.name.clone(),
    params,
  })
}

fn generate_example_param(ty: &VmType<&str>) -> Result<SerializedVmValue> {
  Ok(match ty {
    VmType::Bool => SerializedVmValue::Bool(false),
    VmType::Map(x) => SerializedVmValue::Tagged(TaggedVmValue::M(
      x.iter()
        .map(|(k, v)| generate_example_param(v).map(|x| (k.to_string(), x)))
        .collect::<Result<_>>()?,
    )),
    VmType::Primitive(x) => match x {
      PrimitiveType::Bytes => SerializedVmValue::String("".into()),
      PrimitiveType::String => SerializedVmValue::String("".into()),
      PrimitiveType::Int64 => SerializedVmValue::String("0".into()),
      PrimitiveType::Double => SerializedVmValue::String("0.0".into()),
    },
    _ => SerializedVmValue::Null(None),
  })
}
