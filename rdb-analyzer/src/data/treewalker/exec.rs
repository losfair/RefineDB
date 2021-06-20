use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use anyhow::Result;
use rpds::RedBlackTreeMapSync;

use crate::data::{
  kv::KeyValueStore,
  treewalker::vm_value::{VmMapValue, VmTableValue, VmTableValueKind, VmValue},
};
use thiserror::Error;

use super::{
  bytecode::{TwGraph, TwGraphNode},
  vm::TwVm,
};

pub struct ExecConfig {
  pub concurrency: usize,
}

pub struct Executor<'a, 'b> {
  vm: &'b TwVm<'a>,
  kv: &'b dyn KeyValueStore,
}

#[derive(Clone)]
struct FireRuleItem {
  target_node: u32,
  param_position: u32,
}

#[derive(Error, Debug)]
pub enum ExecError {
  #[error("not yet implemented")]
  NotImplemented,
}

impl<'a, 'b> Executor<'a, 'b> {
  pub fn new_assume_typechecked(vm: &'b TwVm<'a>, kv: &'b dyn KeyValueStore) -> Self {
    Self { vm, kv }
  }

  pub async fn run_graph(&self, graph_index: usize) -> Result<Option<Arc<VmValue<'a>>>> {
    let g = &self.vm.script.graphs[graph_index];
    let fire_rules = generate_fire_rules(g);
    let mut deps_satisfied: Vec<Vec<Option<Arc<VmValue<'a>>>>> =
      g.nodes.iter().map(|(_, x)| vec![None; x.len()]).collect();

    // The initial batch
    let mut futures: Vec<Pin<Box<dyn Future<Output = (u32, Result<Option<Arc<VmValue<'a>>>>)>>>> =
      vec![];
    for (i, (n, in_edges)) in g.nodes.iter().enumerate() {
      if in_edges.is_empty() {
        futures.push(Box::pin(async move {
          (i as u32, self.run_node(n, vec![]).await)
        }));
      }
    }

    let mut ret: Option<Arc<VmValue<'a>>> = None;

    loop {
      if futures.is_empty() {
        break;
      }
      let ((node_index, result), _, remaining) = futures::future::select_all(futures).await;
      let result = result?;
      futures = remaining;

      if Some(node_index) == g.output {
        ret = result.clone();
      }

      if let Some(to_fire) = fire_rules.get(&node_index) {
        let result = result.unwrap_or_else(|| {
          panic!(
            "run_graph: node {} should fire some other nodes but does not produce a value",
            node_index
          )
        });
        for item in to_fire {
          deps_satisfied[item.target_node as usize][item.param_position as usize] =
            Some(result.clone());
        }

        // Do this in another iteration in case that a single source node is connect to a single target node's
        // multiple parameters.
        for item in to_fire {
          // If all deps are satisfied...
          if deps_satisfied[item.target_node as usize]
            .iter()
            .find(|x| x.is_none())
            .is_none()
          {
            let params = std::mem::replace(&mut deps_satisfied[item.target_node as usize], vec![])
              .into_iter()
              .map(|x| x.unwrap())
              .collect::<Vec<_>>();
            let target_node = item.target_node as usize;
            futures.push(Box::pin(async move {
              (
                target_node as u32,
                self.run_node(&g.nodes[target_node].0, params).await,
              )
            }))
          }
        }
      }
    }
    Ok(ret)
  }

  async fn run_node(
    &self,
    n: &TwGraphNode,
    params: Vec<Arc<VmValue<'a>>>,
  ) -> Result<Option<Arc<VmValue<'a>>>> {
    Ok(match n {
      TwGraphNode::BuildSet => unimplemented!(),
      TwGraphNode::BuildTable(table_ty) => {
        let map = match &*params[0] {
          VmValue::Map(x) => &x.elements,
          _ => unreachable!(),
        };
        let ty = self.vm.script.idents[*table_ty as usize].as_str();
        Some(Arc::new(VmValue::Table(VmTableValue {
          ty,
          kind: VmTableValueKind::Fresh(map.iter().map(|(k, v)| (*k, v.clone())).collect()),
        })))
      }
      TwGraphNode::CreateMap => Some(Arc::new(VmValue::Map(VmMapValue {
        elements: RedBlackTreeMapSync::new_sync(),
      }))),
      TwGraphNode::DeleteFromMap(key_index) => {
        let mut elements = match &*params[0] {
          VmValue::Map(x) => x.elements.clone(),
          _ => unreachable!(),
        };
        let key = self.vm.script.idents.get(*key_index as usize).unwrap();
        elements.remove_mut(key.as_str());
        Some(Arc::new(VmValue::Map(VmMapValue { elements })))
      }
      TwGraphNode::DeleteFromTable(_key_index) => None,
      /*
      TwGraphNode::GetMapField(key_index) => {
        let elements = match &*params[0] {
          VmValue::Map(x) => x.elements.clone(),
          _ => unreachable!(),
        };
        let key = self.vm.script.idents.get(*key_index as usize).unwrap();
        Some(
          elements
            .get(key.as_str())
            .cloned()
            .unwrap_or_else(|| Arc::new(VmValue::Null)),
        )
      }
      */
      _ => todo!(),
    })
  }
}

fn generate_fire_rules(g: &TwGraph) -> HashMap<u32, Vec<FireRuleItem>> {
  let mut m: HashMap<u32, Vec<FireRuleItem>> = HashMap::new();
  for (target_node, (_, in_edges)) in g.nodes.iter().enumerate() {
    for (param_position, source_node) in in_edges.iter().enumerate() {
      m.entry(*source_node).or_default().push(FireRuleItem {
        target_node: target_node as u32,
        param_position: param_position as u32,
      });
    }
  }
  m
}
