use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use anyhow::Result;
use rpds::RedBlackTreeMapSync;

use crate::{
  data::{
    kv::{KeyValueStore, KvTransaction},
    treewalker::vm_value::{
      VmMapValue, VmSetValue, VmSetValueKind, VmTableValue, VmTableValueKind, VmType, VmValue,
    },
    value::PrimitiveValue,
  },
  schema::compile::FieldType,
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
  #[error("not yet implemented: {0}")]
  NotImplemented(String),

  #[error("null value unwrappped")]
  NullUnwrapped,
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
    let txn = self.kv.begin_transaction().await?;

    // The initial batch
    let mut futures: Vec<Pin<Box<dyn Future<Output = (u32, Result<Option<Arc<VmValue<'a>>>>)>>>> =
      vec![];
    for (i, (n, in_edges)) in g.nodes.iter().enumerate() {
      if in_edges.is_empty() {
        let txn = &*txn;
        futures.push(Box::pin(async move {
          (i as u32, self.run_node(n, vec![], txn).await)
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
            let txn = &*txn;
            futures.push(Box::pin(async move {
              (
                target_node as u32,
                self.run_node(&g.nodes[target_node].0, params, txn).await,
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
    txn: &dyn KvTransaction,
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
      TwGraphNode::DeleteFromTable(_key_index) => {
        // Effect node
        None
      }
      TwGraphNode::GetField(key_index) => {
        let key = self.vm.script.idents.get(*key_index as usize).unwrap();
        match &*params[0] {
          VmValue::Map(map) => Some(
            map
              .elements
              .get(key.as_str())
              .cloned()
              .unwrap_or_else(|| Arc::new(VmValue::Null)),
          ),
          VmValue::Table(table) => {
            let specialized_ty = self.vm.schema.types.get(table.ty).unwrap();
            let (field, _) = specialized_ty.fields.get(key.as_str()).unwrap();
            match &table.kind {
              VmTableValueKind::Resident(walker) => {
                let walker = walker.enter_field(key.as_str()).unwrap();

                match field.optional_unwrapped() {
                  FieldType::Primitive(_) => {
                    // This is a primitive type - we cannot defer any more.
                    // Let's load from the database.
                    let key = walker.generate_key();
                    let raw_data: Option<PrimitiveValue> = txn
                      .get(&key)
                      .await?
                      .map(|x| rmp_serde::from_slice(&x))
                      .transpose()?;
                    Some(Arc::new(
                      raw_data
                        .map(VmValue::Primitive)
                        .unwrap_or_else(|| VmValue::Null),
                    ))
                  }
                  FieldType::Set(member_ty) => Some(Arc::new(VmValue::Set(VmSetValue {
                    member_ty: VmType::from(&**member_ty),
                    kind: VmSetValueKind::Resident(walker),
                  }))),
                  FieldType::Table(x) => Some(Arc::new(VmValue::Table(VmTableValue {
                    ty: &**x,
                    kind: VmTableValueKind::Resident(walker),
                  }))),
                  _ => unreachable!(),
                }
              }
              VmTableValueKind::Fresh(_) => {
                return Err(
                  ExecError::NotImplemented("cannot get field on a fresh table".into()).into(),
                )
              }
            }
          }
          _ => unreachable!(),
        }
      }
      TwGraphNode::GetSetElement(_) => {
        let primary_key_value = match &*params[0] {
          VmValue::Primitive(x) => x,
          _ => unreachable!(),
        };
        let set = match &*params[1] {
          VmValue::Set(x) => x,
          _ => unreachable!(),
        };
        let member_ty = match &set.member_ty {
          VmType::Table(x) => x.name,
          _ => unreachable!(),
        };
        match &set.kind {
          VmSetValueKind::Resident(walker) => {
            let walker = walker.enter_set(primary_key_value).unwrap();
            Some(Arc::new(VmValue::Table(VmTableValue {
              ty: member_ty,
              kind: VmTableValueKind::Resident(walker),
            })))
          }
          VmSetValueKind::Fresh(_) => {
            return Err(
              ExecError::NotImplemented("cannot get element on a fresh set".into()).into(),
            )
          }
        }
      }
      TwGraphNode::InsertIntoMap(key_index) => {
        let value = &params[0];
        let mut elements = match &*params[1] {
          VmValue::Map(x) => x.elements.clone(),
          _ => unreachable!(),
        };
        let key = self.vm.script.idents.get(*key_index as usize).unwrap();
        elements.insert_mut(key.as_str(), value.clone());
        Some(Arc::new(VmValue::Map(VmMapValue { elements })))
      }
      TwGraphNode::InsertIntoSet => {
        // Effect node
        None
      }
      TwGraphNode::InsertIntoTable(_) => {
        // Effect node
        None
      }
      TwGraphNode::LoadConst(const_index) => {
        let value = self.vm.consts[*const_index as usize].clone();
        Some(value)
      }
      TwGraphNode::LoadParam(param_index) => Some(params[*param_index as usize].clone()),
      TwGraphNode::UnwrapOptional => match &*params[0] {
        VmValue::Null => return Err(ExecError::NullUnwrapped.into()),
        _ => Some(params[0].clone()),
      },
      _ => return Err(ExecError::NotImplemented(format!("{:?}", n)).into()),
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
