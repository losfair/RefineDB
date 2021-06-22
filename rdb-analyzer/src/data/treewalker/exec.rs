use std::{
  collections::{BTreeMap, HashMap},
  future::Future,
  pin::Pin,
  sync::Arc,
};

use anyhow::Result;
use async_recursion::async_recursion;
use rpds::RedBlackTreeMapSync;

use crate::{
  data::{
    kv::{KeyValueStore, KvTransaction},
    pathwalker::PathWalker,
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

  #[error("operation is not supported on fresh tables or sets")]
  FreshTableOrSetNotSupported,
}

impl<'a, 'b> Executor<'a, 'b> {
  pub fn new_assume_typechecked(vm: &'b TwVm<'a>, kv: &'b dyn KeyValueStore) -> Self {
    Self { vm, kv }
  }

  pub async fn run_graph(
    &self,
    graph_index: usize,
    graph_params: &[Arc<VmValue<'a>>],
  ) -> Result<Option<Arc<VmValue<'a>>>> {
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
          (i as u32, self.run_node(n, vec![], txn, graph_params).await)
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
                self
                  .run_node(&g.nodes[target_node].0, params, txn, graph_params)
                  .await,
              )
            }))
          }
        }
      }
    }

    drop(futures);
    txn.commit().await?;
    Ok(ret)
  }

  async fn run_node(
    &self,
    n: &TwGraphNode,
    params: Vec<Arc<VmValue<'a>>>,
    txn: &dyn KvTransaction,
    graph_params: &[Arc<VmValue<'a>>],
  ) -> Result<Option<Arc<VmValue<'a>>>> {
    Ok(match n {
      TwGraphNode::BuildSet => unimplemented!(),
      TwGraphNode::BuildTable(table_ty) => {
        let map = match &*params[0] {
          VmValue::Map(x) => &x.elements,
          _ => unreachable!(),
        };
        let ty = self.vm.script.idents[*table_ty as usize].as_str();
        let mut table: BTreeMap<&'a str, Arc<VmValue<'a>>> = BTreeMap::new();
        let specialized_ty = self.vm.schema.types.get(ty).unwrap();
        for (field, _) in &specialized_ty.fields {
          table.insert(
            &**field,
            map
              .get(&**field)
              .cloned()
              .unwrap_or_else(|| Arc::new(VmValue::Null)),
          );
        }
        Some(Arc::new(VmValue::Table(VmTableValue {
          ty,
          kind: VmTableValueKind::Fresh(table),
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
          VmValue::Table(table) => Some(self.read_table_element(txn, table, key).await?),
          _ => unreachable!(),
        }
      }
      TwGraphNode::GetSetElement => {
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
          VmSetValueKind::Fresh(_) => return Err(ExecError::FreshTableOrSetNotSupported.into()),
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
        let value = params[0].clone();
        let (primary_key, _) = VmType::<&'a str>::from(&*params[1])
          .primary_key(self.vm.schema)
          .expect("inconsistency: primary key not found for set member");
        let primary_key_value = self
          .read_table_element(txn, value.unwrap_table(), primary_key)
          .await?;
        let set = params[1].unwrap_set();
        let primary_key_value = primary_key_value
          .unwrap_primitive()
          .serialize_for_key_component();

        match &set.kind {
          VmSetValueKind::Resident(walker) => {
            let walker = walker.enter_set_raw(&primary_key_value).unwrap();
            self.walk_and_insert(txn, walker, value).await?;
          }
          VmSetValueKind::Fresh(_) => {
            return Err(ExecError::FreshTableOrSetNotSupported.into());
          }
        }

        None
      }
      TwGraphNode::InsertIntoTable(key_index) => {
        // Effect node
        let key = self.vm.script.idents.get(*key_index as usize).unwrap();
        let value = params[0].clone();
        let table = params[1].unwrap_table();
        match &table.kind {
          VmTableValueKind::Resident(walker) => {
            let walker = walker.enter_field(key.as_str()).unwrap();
            self.walk_and_insert(txn, walker, value).await?;
          }
          VmTableValueKind::Fresh(_) => {
            return Err(ExecError::FreshTableOrSetNotSupported.into());
          }
        }
        None
      }
      TwGraphNode::LoadConst(const_index) => {
        let value = self.vm.consts[*const_index as usize].clone();
        Some(value)
      }
      TwGraphNode::LoadParam(param_index) => Some(graph_params[*param_index as usize].clone()),
      TwGraphNode::UnwrapOptional => match &*params[0] {
        VmValue::Null => return Err(ExecError::NullUnwrapped.into()),
        _ => Some(params[0].clone()),
      },
      TwGraphNode::DeleteFromSet => {
        let primary_key_value = match &*params[0] {
          VmValue::Primitive(x) => x,
          _ => unreachable!(),
        };
        let set = match &*params[1] {
          VmValue::Set(x) => x,
          _ => unreachable!(),
        };
        match &set.kind {
          VmSetValueKind::Resident(walker) => {
            let primary_key_value_raw = primary_key_value.serialize_for_key_component();
            let mut fast_scan_key = walker.set_fast_scan_prefix().unwrap();
            fast_scan_key.extend_from_slice(&primary_key_value_raw);

            let mut data_start_key = walker.set_data_prefix().unwrap();
            data_start_key.extend_from_slice(&primary_key_value_raw);
            data_start_key.push(0x00);

            let mut data_end_key = data_start_key.clone();
            *data_end_key.last_mut().unwrap() = 0x01;

            txn.delete(&fast_scan_key).await?;
            txn.delete_range(&data_start_key, &data_end_key).await?;
            None
          }
          VmSetValueKind::Fresh(_) => return Err(ExecError::FreshTableOrSetNotSupported.into()),
        }
      }
      _ => return Err(ExecError::NotImplemented(format!("{:?}", n)).into()),
    })
  }

  async fn read_table_element(
    &self,
    txn: &dyn KvTransaction,
    table: &VmTableValue<'a>,
    key: &str,
  ) -> Result<Arc<VmValue<'a>>> {
    Ok(match &table.kind {
      VmTableValueKind::Fresh(x) => x
        .get(key)
        .cloned()
        .unwrap_or_else(|| Arc::new(VmValue::Null)),
      VmTableValueKind::Resident(walker) => {
        let specialized_ty = self.vm.schema.types.get(table.ty).unwrap();
        let (field, _) = specialized_ty.fields.get(key).unwrap();
        let walker = walker
          .enter_field(key)
          .expect("inconsistency: field not found in table");

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
            Arc::new(
              raw_data
                .map(VmValue::Primitive)
                .unwrap_or_else(|| VmValue::Null),
            )
          }
          FieldType::Set(member_ty) => Arc::new(VmValue::Set(VmSetValue {
            member_ty: VmType::from(&**member_ty),
            kind: VmSetValueKind::Resident(walker),
          })),
          FieldType::Table(x) => Arc::new(VmValue::Table(VmTableValue {
            ty: &**x,
            kind: VmTableValueKind::Resident(walker),
          })),
          _ => unreachable!(),
        }
      }
    })
  }

  #[async_recursion]
  async fn walk_and_insert(
    &self,
    txn: &dyn KvTransaction,
    walker: Arc<PathWalker<'a>>,
    value: Arc<VmValue<'a>>,
  ) -> Result<()> {
    match &*value {
      VmValue::Null => {
        txn.delete(&walker.generate_key()).await?;
      }
      VmValue::Primitive(x) => {
        let value = rmp_serde::to_vec(x).unwrap();
        txn.put(&walker.generate_key(), &value).await?;
      }
      VmValue::Set(x) => {
        txn.put(&walker.generate_key(), &[]).await?;
        match &x.kind {
          VmSetValueKind::Fresh(members) => {
            // Need to clone this. Otherwise `async_recursion` errors
            let members = members.clone();
            for (primary_key_value, member) in members {
              let mut fast_scan_key = walker.set_fast_scan_prefix().unwrap();
              fast_scan_key.extend_from_slice(&primary_key_value);
              txn.put(&fast_scan_key, &[]).await?;

              let walker = walker.enter_set_raw(&primary_key_value).unwrap();
              self.walk_and_insert(txn, walker, member).await?;
            }
          }
          VmSetValueKind::Resident(_) => {
            return Err(ExecError::NotImplemented("set copy is not implemented".into()).into())
          }
        }
      }
      VmValue::Table(x) => {
        txn.put(&walker.generate_key(), &[]).await?;
        match &x.kind {
          VmTableValueKind::Fresh(fields) => {
            // Need to clone this. Otherwise `async_recursion` errors
            let fields = fields.clone();
            for (k, v) in fields {
              let walker = walker.enter_field(k).unwrap();
              let v = v.clone();
              self.walk_and_insert(txn, walker, v).await?;
            }
          }
          VmTableValueKind::Resident(_) => {
            return Err(ExecError::NotImplemented("table copy is not implemented".into()).into())
          }
        }
      }
      VmValue::Bool(_) | VmValue::Map(_) => {
        panic!(
          "inconsistency: walk_and_insert encountered non-storable type: {:?}",
          value
        );
      }
    }
    Ok(())
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
