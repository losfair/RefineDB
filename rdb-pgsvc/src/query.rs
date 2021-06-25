use rdb_analyzer::data::treewalker::vm::TwVm;
use serde::Serialize;

#[derive(Serialize, Default)]
pub struct VmGlobalGraphInfo {
  pub graphs: Vec<VmGraphInfo>,
}

#[derive(Serialize)]
pub struct VmGraphInfo {
  pub name: String,
  pub params: Vec<String>,
}

pub fn get_vm_graphs(vm: &TwVm) -> VmGlobalGraphInfo {
  let mut res = VmGlobalGraphInfo::default();
  for g in &vm.script.graphs {
    if !g.exported {
      continue;
    }
    res.graphs.push(VmGraphInfo {
      name: g.name.clone(),
      params: g
        .param_types
        .iter()
        .map(|x| format!("{}", vm.types[*x as usize]))
        .collect(),
    });
  }
  res
}
