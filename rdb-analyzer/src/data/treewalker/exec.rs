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
      VmListNode, VmListValue, VmMapValue, VmSetType, VmSetValue, VmSetValueKind, VmTableValue,
      VmTableValueKind, VmType, VmValue,
    },
    value::PrimitiveValue,
  },
  schema::compile::{CompiledSchema, FieldType},
  storage_plan::StoragePlan,
};
use thiserror::Error;

use super::{
  bytecode::{TwGraph, TwGraphNode},
  typeck::GlobalTypeInfo,
  vm::TwVm,
};

pub struct ExecConfig {
  pub concurrency: usize,
}

pub struct Executor<'a, 'b> {
  vm: &'b TwVm<'a>,
  kv: &'b dyn KeyValueStore,
  type_info: &'b GlobalTypeInfo<'a>,
}

#[derive(Clone)]
struct FireRuleItem {
  target_node: u32,
  kind: FireRuleKind,
}

#[derive(Clone)]
enum FireRuleKind {
  ParamDep(u32),
  Precondition,
}

#[derive(Error, Debug)]
pub enum ExecError {
  #[error("not yet implemented: {0}")]
  NotImplemented(String),

  #[error("null value unwrappped")]
  NullUnwrapped,

  #[error("operation is not supported on fresh tables or sets")]
  FreshTableOrSetNotSupported,

  #[error("export type not supported")]
  ExportTypeNotSupported,

  #[error("missing value for non-optional field `{0}` of table `{1}`")]
  MissingValueForNonOptionalField(String, Arc<str>),

  #[error("max recursion depth exceeded: {0}")]
  MaxRecursionDepthExceeded(usize),
}

const MAX_RECURSION_DEPTH: usize = 128;

impl<'a, 'b> Executor<'a, 'b> {
  pub fn new(
    vm: &'b TwVm<'a>,
    kv: &'b dyn KeyValueStore,
    type_info: &'b GlobalTypeInfo<'a>,
  ) -> Self {
    Self { vm, kv, type_info }
  }

  pub async fn run_graph(
    &self,
    graph_index: usize,
    graph_params: &[Arc<VmValue<'a>>],
  ) -> Result<Option<Arc<VmValue<'a>>>> {
    let txn = self.kv.begin_transaction().await?;
    let ret = self
      .recursively_run_graph(graph_index, graph_params, 0, &*txn)
      .await?;
    txn.commit().await?;
    Ok(ret)
  }

  async fn recursively_run_graph(
    &self,
    graph_index: usize,
    graph_params: &[Arc<VmValue<'a>>],
    recursion_depth: usize,
    txn: &dyn KvTransaction,
  ) -> Result<Option<Arc<VmValue<'a>>>> {
    if recursion_depth >= MAX_RECURSION_DEPTH {
      return Err(ExecError::MaxRecursionDepthExceeded(recursion_depth).into());
    }
    let recursion_depth = recursion_depth + 1;
    let g = &self.vm.script.graphs[graph_index];
    let type_info = &self.type_info.graphs[graph_index];
    let fire_rules = generate_fire_rules(g);
    let mut deps_satisfied: Vec<Vec<Option<Arc<VmValue<'a>>>>> = g
      .nodes
      .iter()
      .map(|(_, x, _)| vec![None; x.len()])
      .collect();
    let mut precondition_satisfied: Vec<bool> =
      g.nodes.iter().map(|(_, _, x)| x.is_none()).collect();

    // The initial batch
    let mut futures: Vec<Pin<Box<dyn Future<Output = (u32, Result<Option<Arc<VmValue<'a>>>>)>>>> =
      vec![];
    for (i, (n, in_edges, precondition)) in g.nodes.iter().enumerate() {
      if in_edges.is_empty() && precondition.is_none() {
        let txn = &*txn;
        futures.push(Box::pin(async move {
          (
            i as u32,
            self
              .run_node(
                n,
                vec![],
                txn,
                graph_params,
                type_info.nodes[i].as_ref(),
                recursion_depth,
              )
              .await,
          )
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
        for item in to_fire {
          match &item.kind {
            FireRuleKind::ParamDep(param_position) => {
              let result = result.as_ref().unwrap_or_else(|| {
                panic!(
                  "run_graph: node {} is a parameter dependency of some other nodes but does not produce a value",
                  node_index
                )
              });

              deps_satisfied[item.target_node as usize][*param_position as usize] =
                Some(result.clone());
            }
            FireRuleKind::Precondition => {
              precondition_satisfied[item.target_node as usize] =
                match result.as_ref().map(|x| &**x) {
                  Some(VmValue::Bool(x)) => *x,
                  None => true,
                  _ => panic!("inconsistency detected: invalid precondition: {:?}", result),
                };
            }
          }
        }

        // Do this in another iteration in case that a single source node is connect to a single target node's
        // multiple parameters.
        for item in to_fire {
          let target_node = item.target_node as usize;
          let node_info = &g.nodes[target_node].0;

          // If all deps and the precondition are satisfied...
          if precondition_satisfied[item.target_node as usize] {
            if node_info.is_select() {
              if deps_satisfied[item.target_node as usize].is_empty() {
                log::warn!("both select candidates are fired");
              }

              if let Some(x) = deps_satisfied[item.target_node as usize]
                .iter()
                .find_map(|x| x.as_ref())
              {
                let x = x.clone();

                // Fire only once!
                deps_satisfied[item.target_node as usize] = vec![];

                futures.push(Box::pin(async move { (target_node as u32, Ok(Some(x))) }))
              }
            } else {
              if deps_satisfied[item.target_node as usize]
                .iter()
                .find(|x| x.is_none())
                .is_none()
              {
                let params =
                  std::mem::replace(&mut deps_satisfied[item.target_node as usize], vec![])
                    .into_iter()
                    .map(|x| x.unwrap())
                    .collect::<Vec<_>>();
                let txn = &*txn;
                futures.push(Box::pin(async move {
                  (
                    target_node as u32,
                    self
                      .run_node(
                        node_info,
                        params,
                        txn,
                        graph_params,
                        type_info.nodes[target_node].as_ref(),
                        recursion_depth,
                      )
                      .await,
                  )
                }))
              }
            }
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
    graph_params: &[Arc<VmValue<'a>>],
    type_info: Option<&VmType<&'a str>>,
    recursion_depth: usize,
  ) -> Result<Option<Arc<VmValue<'a>>>> {
    // Optional chain
    if n.is_optional_chained() {
      for (i, p) in params.iter().enumerate() {
        if p.is_null() {
          log::trace!(
            "optional chaining node {:?} because parameter {} is null: {:?}",
            n,
            i,
            p
          );
          return Ok(type_info.map(|x| Arc::new(VmValue::Null(x.clone()))));
        }
      }
    }

    Ok(match n {
      TwGraphNode::BuildSet => {
        let list = match &*params[0] {
          VmValue::List(x) => x,
          _ => unreachable!(),
        };
        let mut node = list.node.as_ref();
        let mut members = BTreeMap::new();
        let (primary_key, _) = VmType::Set(VmSetType {
          ty: Box::new(list.member_ty.clone()),
        })
        .set_primary_key(self.vm.schema)
        .expect("inconsistency: primary key not found");
        while let Some(n) = node {
          let primary_key_value = match &n.value.unwrap_table().kind {
            VmTableValueKind::Fresh(x) => x
              .get(primary_key)
              .unwrap()
              .unwrap_primitive()
              .serialize_for_key_component(),
            _ => {
              return Err(ExecError::NotImplemented("table copy is not implemented".into()).into())
            }
          };
          members.insert(primary_key_value.to_vec(), n.value.clone());
          node = n.next.as_ref();
        }
        let set = VmSetValue {
          member_ty: list.member_ty.clone(),
          kind: VmSetValueKind::Fresh(members),
        };
        Some(Arc::new(VmValue::Set(set)))
      }
      TwGraphNode::BuildTable(table_ty) => {
        let map = match &*params[0] {
          VmValue::Map(x) => &x.elements,
          _ => unreachable!(),
        };
        let ty = self.vm.script.idents[*table_ty as usize].as_str();
        let mut table: BTreeMap<&'a str, Arc<VmValue<'a>>> = BTreeMap::new();
        let specialized_ty = self.vm.schema.types.get(ty).unwrap();
        for (field, (ty, _)) in &specialized_ty.fields {
          let field_value = map
            .get(&**field)
            .cloned()
            .unwrap_or_else(|| Arc::new(VmValue::Null(VmType::from(ty))));
          if field_value.is_null() && !ty.is_optional() {
            return Err(
              ExecError::MissingValueForNonOptionalField(
                field.to_string(),
                specialized_ty.name.clone(),
              )
              .into(),
            );
          }
          table.insert(&**field, field_value);
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
          VmValue::Null(_) => return Ok(Some(params[0].clone())),
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
              .unwrap_or_else(|| panic!("map field not found: {}", key)),
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
          VmValue::Null(_) => return Ok(Some(params[1].clone())),
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
          .set_primary_key(self.vm.schema)
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
            let mut fast_scan_key = walker.set_fast_scan_prefix().unwrap();
            fast_scan_key.extend_from_slice(&primary_key_value);
            txn.put(&fast_scan_key, &[]).await?;

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
            self
              .delete_entry_from_set(txn, walker, primary_key_value)
              .await?;
            None
          }
          VmSetValueKind::Fresh(_) => return Err(ExecError::FreshTableOrSetNotSupported.into()),
        }
      }
      TwGraphNode::Eq => Some(Arc::new(VmValue::Bool(params[0] == params[1]))),
      TwGraphNode::Ne => Some(Arc::new(VmValue::Bool(params[0] != params[1]))),
      TwGraphNode::And => Some(Arc::new(VmValue::Bool(
        params[0].unwrap_bool() & params[1].unwrap_bool(),
      ))),
      TwGraphNode::Or => Some(Arc::new(VmValue::Bool(
        params[0].unwrap_bool() | params[1].unwrap_bool(),
      ))),
      TwGraphNode::Not => Some(Arc::new(VmValue::Bool(!params[0].unwrap_bool()))),
      TwGraphNode::IsPresent => {
        let walker = match &*params[0] {
          VmValue::Set(x) => match &x.kind {
            VmSetValueKind::Fresh(_) => return Ok(Some(Arc::new(VmValue::Bool(true)))),
            VmSetValueKind::Resident(x) => x,
          },
          VmValue::Table(x) => match &x.kind {
            VmTableValueKind::Fresh(_) => return Ok(Some(Arc::new(VmValue::Bool(true)))),
            VmTableValueKind::Resident(x) => x,
          },
          _ => unreachable!(),
        };
        Some(Arc::new(VmValue::Bool(
          txn.get(&walker.generate_key()).await?.is_some(),
        )))
      }
      TwGraphNode::IsNull => Some(Arc::new(VmValue::Bool(params[0].is_null()))),
      TwGraphNode::Nop => Some(params[0].clone()),
      TwGraphNode::Call(subgraph_index) => {
        let output = self
          .recursively_run_graph(*subgraph_index as usize, &params, recursion_depth, txn)
          .await?;
        output
      }
      TwGraphNode::Add => Some(Arc::new(match (&*params[0], &*params[1]) {
        (
          VmValue::Primitive(PrimitiveValue::Int64(l)),
          VmValue::Primitive(PrimitiveValue::Int64(r)),
        ) => VmValue::Primitive(PrimitiveValue::Int64(l.wrapping_add(*r))),
        (
          VmValue::Primitive(PrimitiveValue::Double(l)),
          VmValue::Primitive(PrimitiveValue::Double(r)),
        ) => VmValue::Primitive(PrimitiveValue::Double(
          (f64::from_bits(*l) + f64::from_bits(*r)).to_bits(),
        )),
        (
          VmValue::Primitive(PrimitiveValue::String(l)),
          VmValue::Primitive(PrimitiveValue::String(r)),
        ) => VmValue::Primitive(PrimitiveValue::String(format!("{}{}", l, r))),
        _ => unreachable!(),
      })),
      TwGraphNode::Sub => Some(Arc::new(match (&*params[0], &*params[1]) {
        (
          VmValue::Primitive(PrimitiveValue::Int64(l)),
          VmValue::Primitive(PrimitiveValue::Int64(r)),
        ) => VmValue::Primitive(PrimitiveValue::Int64(l.wrapping_sub(*r))),
        (
          VmValue::Primitive(PrimitiveValue::Double(l)),
          VmValue::Primitive(PrimitiveValue::Double(r)),
        ) => VmValue::Primitive(PrimitiveValue::Double(
          (f64::from_bits(*l) - f64::from_bits(*r)).to_bits(),
        )),
        _ => unreachable!(),
      })),
      TwGraphNode::CreateList(member_ty) => {
        let member_ty = self.vm.types.get(*member_ty as usize).unwrap().clone();
        Some(Arc::new(VmValue::List(VmListValue {
          member_ty,
          node: None,
        })))
      }
      TwGraphNode::PrependToList => {
        let value = params[0].clone();
        let list = match &*params[1] {
          VmValue::List(x) => x,
          _ => unreachable!(),
        };
        Some(Arc::new(VmValue::List(VmListValue {
          member_ty: list.member_ty.clone(),
          node: Some(Arc::new(VmListNode {
            value,
            next: list.node.clone(),
          })),
        })))
      }
      TwGraphNode::PopFromList => {
        let list = match &*params[0] {
          VmValue::List(x) => x,
          _ => unreachable!(),
        };
        Some(Arc::new(match &list.node {
          Some(x) => VmValue::List(VmListValue {
            member_ty: list.member_ty.clone(),
            node: x.next.clone(),
          }),
          None => VmValue::Null(VmType::from(&*params[0])),
        }))
      }
      TwGraphNode::ListHead => {
        let list = match &*params[0] {
          VmValue::List(x) => x,
          _ => unreachable!(),
        };
        Some(match &list.node {
          Some(x) => x.value.clone(),
          None => Arc::new(VmValue::Null(list.member_ty.clone())),
        })
      }
      TwGraphNode::Select => panic!("inconsistency: got select in run_node"),
      TwGraphNode::FilterSet(_) => {
        return Err(ExecError::NotImplemented(format!("{:?}", n)).into())
      }
      TwGraphNode::Reduce(subgraph_index) => {
        let subgraph_param = &params[0];
        let reduce_init = &params[1];
        let list_or_set = &params[2];
        let mut subgraph_params = vec![
          subgraph_param.clone(),
          reduce_init.clone(),
          Arc::new(VmValue::Bool(false)), // placeholder
        ];
        match &**list_or_set {
          VmValue::List(list) => {
            let mut node = list.node.as_ref();
            while let Some(n) = node {
              subgraph_params[2] = n.value.clone();
              let output = self
                .recursively_run_graph(
                  *subgraph_index as usize,
                  &subgraph_params,
                  recursion_depth,
                  txn,
                )
                .await?
                .expect("inconsistency: ReduceList did not get an output from subgraph");
              subgraph_params[1] = output;
              node = n.next.as_ref();
            }
          }
          VmValue::Set(set) => {
            let walker = match &set.kind {
              VmSetValueKind::Resident(x) => x,
              _ => return Err(ExecError::FreshTableOrSetNotSupported.into()),
            };
            let specialized_ty = match &set.member_ty {
              VmType::Table(x) => self.vm.schema.types.get(x.name).unwrap(),
              _ => unreachable!(),
            };
            let range_start = walker.set_fast_scan_prefix().unwrap();
            let mut range_end = range_start.clone();
            *range_end.last_mut().unwrap() += 1;
            let range_end = range_end;

            log::trace!(
              "reduce set: scan keys: {} {}",
              base64::encode(&range_start),
              base64::encode(&range_end)
            );

            let mut it = txn.scan_keys(&range_start, &range_end).await?;
            while let Some(k) = it.next().await? {
              let k = k.strip_prefix(range_start.as_slice()).unwrap();
              let walker = walker.enter_set_raw(k).unwrap();
              subgraph_params[2] = Arc::new(VmValue::Table(VmTableValue {
                ty: &*specialized_ty.name,
                kind: VmTableValueKind::Resident(walker),
              }));
              let output = self
                .recursively_run_graph(
                  *subgraph_index as usize,
                  &subgraph_params,
                  recursion_depth,
                  txn,
                )
                .await?
                .expect("inconsistency: ReduceList did not get an output from subgraph");
              subgraph_params[1] = output;
            }
          }
          _ => unreachable!(),
        }
        Some(subgraph_params[1].clone())
      }
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
        .unwrap_or_else(|| panic!("read_table_element: key not found in table: {}", key)),
      VmTableValueKind::Resident(walker) => {
        let specialized_ty = self.vm.schema.types.get(table.ty).unwrap();
        let (field, _) = specialized_ty.fields.get(key).unwrap();
        let walker = walker
          .enter_field(key)
          .expect("inconsistency: field not found in table");

        match field.optional_unwrapped() {
          x @ FieldType::Primitive(_) => {
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
                .unwrap_or_else(|| VmValue::Null(VmType::from(x))),
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
      VmValue::Null(_) => {
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
            // Clear set
            self.delete_set(txn, &walker).await?;

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
      VmValue::Bool(_) | VmValue::Map(_) | VmValue::List(_) => {
        panic!(
          "inconsistency: walk_and_insert encountered non-storable type: {:?}",
          value
        );
      }
    }
    Ok(())
  }

  async fn delete_set(&self, txn: &dyn KvTransaction, walker: &Arc<PathWalker<'a>>) -> Result<()> {
    let fast_scan_start_key = walker.set_fast_scan_prefix().unwrap();
    let mut fast_scan_end_key = fast_scan_start_key.clone();
    *fast_scan_end_key.last_mut().unwrap() += 1;

    let data_start_key = walker.set_data_prefix().unwrap();
    let mut data_end_key = data_start_key.clone();
    *data_end_key.last_mut().unwrap() += 1;

    txn
      .delete_range(&fast_scan_start_key, &fast_scan_end_key)
      .await?;
    txn.delete_range(&data_start_key, &data_end_key).await?;
    Ok(())
  }

  async fn delete_entry_from_set(
    &self,
    txn: &dyn KvTransaction,
    walker: &Arc<PathWalker<'a>>,
    primary_key_value: &PrimitiveValue,
  ) -> Result<()> {
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
    Ok(())
  }
}

fn generate_fire_rules(g: &TwGraph) -> HashMap<u32, Vec<FireRuleItem>> {
  let mut m: HashMap<u32, Vec<FireRuleItem>> = HashMap::new();
  for (target_node, (_, in_edges, precondition)) in g.nodes.iter().enumerate() {
    for (param_position, source_node) in in_edges.iter().enumerate() {
      m.entry(*source_node).or_default().push(FireRuleItem {
        target_node: target_node as u32,
        kind: FireRuleKind::ParamDep(param_position as u32),
      });
    }
    if let Some(source_node) = precondition {
      m.entry(*source_node).or_default().push(FireRuleItem {
        target_node: target_node as u32,
        kind: FireRuleKind::Precondition,
      });
    }
  }
  m
}

pub fn generate_root_map<'a>(
  schema: &'a CompiledSchema,
  plan: &'a StoragePlan,
) -> Result<VmValue<'a>> {
  let mut m = RedBlackTreeMapSync::new_sync();
  for (field_name, field_ty) in &schema.exports {
    match field_ty {
      FieldType::Table(x) => {
        m.insert_mut(
          &**field_name,
          Arc::new(VmValue::Table(VmTableValue {
            ty: &**x,
            kind: VmTableValueKind::Resident(PathWalker::from_export(plan, &**field_name).unwrap()),
          })),
        );
      }
      FieldType::Set(x) => {
        m.insert_mut(
          &**field_name,
          Arc::new(VmValue::Set(VmSetValue {
            member_ty: VmType::from(&**x),
            kind: VmSetValueKind::Resident(PathWalker::from_export(plan, &**field_name).unwrap()),
          })),
        );
      }
      _ => return Err(ExecError::ExportTypeNotSupported.into()),
    }
  }
  Ok(VmValue::Map(VmMapValue { elements: m }))
}
