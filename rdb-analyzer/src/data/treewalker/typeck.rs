use std::{
  collections::{HashMap, HashSet},
  sync::Arc,
};

use anyhow::Result;
use petgraph::{algo::kosaraju_scc, graph::DiGraph};
use rpds::RedBlackTreeMapSync;
use thiserror::Error;

use crate::{
  data::treewalker::{
    bytecode::TwGraphNode,
    vm_value::{VmSetType, VmTableType},
  },
  schema::compile::FieldType,
};

use super::{bytecode::TwGraph, vm::TwVm, vm_value::VmType};

#[derive(Error, Debug)]
pub enum TypeckError {
  #[error("invalid in edge")]
  InvalidInEdge,
  #[error("const index out of bounds")]
  ConstIndexOob,
  #[error("ident index out of bounds")]
  IdentIndexOob,
  #[error("param index out of bounds")]
  ParamIndexOob,
  #[error("subgraph index out of bounds")]
  SubgraphIndexOob,
  #[error("expecting {0} in edges on node `{1}`, got {2}")]
  InEdgeCountMismatch(usize, String, usize),
  #[error("expecting a typed node")]
  ExpectingTypedNode,
  #[error("expecting list, got `{0}`")]
  ExpectingList(String),
  #[error("expecting set, got `{0}`")]
  ExpectingSet(String),
  #[error("type `{0}` is not covariant from `{1}`")]
  NonCovariantTypes(String, String),
  #[error("type `{0}` is not a map")]
  NotMap(String),
  #[error("type `{0}` is not a table")]
  NotTable(String),
  #[error("type `{0}` is not a map or table")]
  NotMapOrTable(String),
  #[error("type `{0}` is not a set")]
  NotSet(String),
  #[error("table type `{0}` not found")]
  TableTypeNotFound(String),
  #[error("map field `{0}` is not present in table `{1}`")]
  MapFieldNotPresentInTable(String, Arc<str>),
  #[error("non-optional table field `{0}` is not present in map `{1}`")]
  TableFieldNotPresentInMap(Arc<str>, String),
  #[error("graph output index out of bounds")]
  GraphOutputIndexOob,
  #[error("graph effect index out of bounds")]
  GraphEffectIndexOob,
  #[error("param type index out of bounds")]
  ParamTypeIndexOob,
  #[error("output type index out of bounds")]
  OutputTypeIndexOob,
  #[error("output node index out of bounds")]
  OutputNodeIndexOob,
  #[error("expected output type `{0}` mismatches with actual output type `{1}`")]
  OutputTypeMismatch(String, String),
  #[error("expecting bool output for filter subgraphs, got `{0}`")]
  ExpectingBoolOutputForFilterSubgraphs(String),
  #[error("field `{0}` is not present in table `{1}`")]
  FieldNotPresentInTable(String, Arc<str>),
  #[error("cannot unwrap non-optional type `{0}`")]
  CannotUnwrapNonOptional(String),
  #[error("field `{0}` of type `{1}` is not a primary key")]
  NotPrimaryKey(String, Arc<str>),
  #[error("deleting non-optional field `{0}` of table `{1}`")]
  DeletingNonOptionalTableField(String, Arc<str>),
  #[error("unknown type of param {0} is not resolved in subgraph {1}")]
  UnknownParamTypeNotResolved(u32, u32),
  #[error("multiple candidate types for param {0} in subgraph {1}: {2}")]
  MultipleParamTypeCandidates(u32, u32, String),
  #[error("param count mismatch in {0}: expected {1}, got {2}")]
  ParamCountMismatch(&'static str, u32, u32),
}

pub struct GlobalTyckContext<'a, 'b> {
  pub vm: &'b TwVm<'a>,
  pub scc_post_order: Vec<HashSet<u32>>,
  pub subgraph_expected_param_types: Vec<Vec<HashSet<VmType<&'a str>>>>,
}

impl<'a, 'b> GlobalTyckContext<'a, 'b> {
  pub fn new(vm: &'b TwVm<'a>) -> Result<Self> {
    let mut call_graph: DiGraph<u32, ()> = DiGraph::new();
    let subgraph_indices = vm
      .script
      .graphs
      .iter()
      .enumerate()
      .map(|(i, _)| call_graph.add_node(i as u32))
      .collect::<Vec<_>>();
    let subgraph_expected_param_types: Vec<Vec<HashSet<VmType<&'a str>>>> = vm
      .script
      .graphs
      .iter()
      .map(|g| (0..g.param_types.len()).map(|_| HashSet::new()).collect())
      .collect();

    // Build the call graph.
    for (i, g) in vm.script.graphs.iter().enumerate() {
      for (n, _) in &g.nodes {
        for r in n.subgraph_references() {
          vm.script
            .graphs
            .get(r as usize)
            .ok_or_else(|| TypeckError::SubgraphIndexOob)?;
          call_graph.add_edge(
            subgraph_indices[i as usize],
            subgraph_indices[r as usize],
            (),
          );
        }
      }
    }

    // Collect single external caller subgraphs
    let all_sccs: Vec<HashSet<u32>> = kosaraju_scc(&call_graph)
      .into_iter()
      .map(|x| x.into_iter().map(|i| call_graph[i]).collect())
      .collect();

    Ok(Self {
      vm,
      scc_post_order: all_sccs,
      subgraph_expected_param_types,
    })
  }

  pub fn analyze(&mut self) -> Result<()> {
    Ok(())
  }

  pub fn typeck(&mut self) -> Result<()> {
    // Typecheck subgraphs in reversed scc_post_order, to ensure param types can be inferred.
    for scc in self.scc_post_order.iter().rev() {
      let mut subgraph_expected_param_types_sink: HashMap<u32, Vec<HashSet<VmType<&'a str>>>> =
        HashMap::new();
      for i in scc {
        log::trace!("typeck: scc {:p}, subgraph {}", scc, i);
        self.typeck_graph(*i as usize, &mut subgraph_expected_param_types_sink)?;
      }

      for (i, x) in subgraph_expected_param_types_sink {
        let y = &mut self.subgraph_expected_param_types[i as usize];
        assert_eq!(x.len(), y.len());
        for (x, y) in x.into_iter().zip(y.iter_mut()) {
          for elem in x {
            y.insert(elem);
          }
        }
      }
    }
    Ok(())
  }

  fn typeck_graph(
    &self,
    graph_index: usize,
    subgraph_expected_param_types_sink: &mut HashMap<u32, Vec<HashSet<VmType<&'a str>>>>,
  ) -> Result<Vec<Option<VmType<&'a str>>>> {
    let vm = self.vm;
    let g = &self.vm.script.graphs[graph_index];
    if let Some(x) = g.output {
      if x as usize >= g.nodes.len() {
        return Err(TypeckError::GraphOutputIndexOob.into());
      }
    }

    for eff in &g.effects {
      if *eff as usize >= g.nodes.len() {
        return Err(TypeckError::GraphEffectIndexOob.into());
      }
    }

    let output_type = g
      .output_type
      .map(|x| {
        vm.types
          .get(x as usize)
          .ok_or_else(|| TypeckError::OutputTypeIndexOob)
      })
      .transpose()?;

    let mut params = g
      .param_types
      .iter()
      .map(|x| vm.types.get(*x as usize).cloned())
      .collect::<Option<Vec<_>>>()
      .ok_or_else(|| TypeckError::ParamTypeIndexOob)?;

    // Resolve param types
    for (i, p) in params.iter_mut().enumerate() {
      let expected = &self.subgraph_expected_param_types[graph_index][i];

      // Step 1: Param type inference
      match (&*p, expected.is_empty()) {
        (VmType::Unknown, true) => {
          return Err(
            TypeckError::UnknownParamTypeNotResolved(i as u32, graph_index as u32).into(),
          );
        }
        (_, true) => {}
        (VmType::Unknown, false) => {
          if expected.len() != 1 {
            return Err(
              TypeckError::MultipleParamTypeCandidates(
                i as u32,
                graph_index as u32,
                format!("{:?}", expected),
              )
              .into(),
            );
          }
          let ty = (*expected.iter().next().unwrap()).clone();
          log::trace!(
            "inferred type `{:?}` for subgraph {} param {}",
            ty,
            graph_index,
            i
          );
          *p = ty;
        }
        (_, false) => {
          for x in expected {
            ensure_covariant(p, x)?;
          }
        }
      }

      // Step 2: Special case for the schema type
      match p {
        VmType::Schema => {
          *p = VmType::from(vm.schema);
        }
        _ => {}
      }
    }

    let mut types: Vec<Option<VmType<&'a str>>> = Vec::with_capacity(g.nodes.len());
    for (i, (node, in_edges)) in g.nodes.iter().enumerate() {
      // Check in_edges invariant
      for j in in_edges {
        let j = *j as usize;
        if j >= i {
          return Err(TypeckError::InvalidInEdge.into());
        }
      }

      let ty: Option<VmType<&'a str>> = match node {
        TwGraphNode::BuildSet => {
          let [list_ty] = validate_in_edges::<1>(node, in_edges, &types)?;
          let element_ty = extract_list_element_type(list_ty)?;
          Some(VmType::Set(VmSetType {
            ty: Box::new(element_ty.clone()),
          }))
        }
        TwGraphNode::BuildTable(table_ty) => {
          let [map_ty] = validate_in_edges::<1>(node, in_edges, &types)?;
          let table_ty = vm
            .script
            .idents
            .get(*table_ty as usize)
            .ok_or_else(|| TypeckError::IdentIndexOob)?;
          let table_ty = vm
            .schema
            .types
            .get(table_ty.as_str())
            .ok_or_else(|| TypeckError::TableTypeNotFound(table_ty.clone()))?;
          match map_ty {
            VmType::Map(x) => {
              // Bi-directional field existence & type check
              for (name, actual_ty) in x {
                if let Some((field_ty, _)) = table_ty.fields.get(*name) {
                  let field_ty = VmType::from(field_ty);
                  ensure_covariant(&field_ty, actual_ty)?;
                } else {
                  return Err(
                    TypeckError::MapFieldNotPresentInTable(name.to_string(), table_ty.name.clone())
                      .into(),
                  );
                }
              }
              for (name, (field_ty, _)) in &table_ty.fields {
                if !x.contains_key(&**name) {
                  if let FieldType::Optional(_) = field_ty {
                  } else {
                    return Err(
                      TypeckError::TableFieldNotPresentInMap(name.clone(), format!("{:?}", map_ty))
                        .into(),
                    );
                  }
                }
              }
            }
            _ => return Err(TypeckError::NotMap(format!("{:?}", map_ty)).into()),
          }

          Some(VmType::Table(VmTableType {
            name: &*table_ty.name,
          }))
        }
        TwGraphNode::CreateMap => Some(VmType::Map(RedBlackTreeMapSync::new_sync())),
        TwGraphNode::DeleteFromSet => {
          let [primary_key_value_ty, set_ty] = validate_in_edges::<2>(node, in_edges, &types)?;
          let set_member_ty = extract_set_element_type(set_ty)?;
          let (key, _) = set_ty.primary_key(vm.schema).unwrap();
          match set_member_ty {
            VmType::Table(x) => {
              let table_ty = vm
                .schema
                .types
                .get(x.name)
                .ok_or_else(|| TypeckError::TableTypeNotFound(x.name.to_string()))?;
              let (field_ty, _) = table_ty.fields.get(key).ok_or_else(|| {
                TypeckError::FieldNotPresentInTable(key.to_string(), table_ty.name.clone())
              })?;
              let field_ty = VmType::from(field_ty);
              ensure_covariant(&field_ty, primary_key_value_ty)?;
              None
            }
            _ => return Err(TypeckError::NotTable(format!("{:?}", set_member_ty)).into()),
          }
        }
        TwGraphNode::DeleteFromMap(key_index) => {
          let [map_ty] = validate_in_edges::<1>(node, in_edges, &types)?;
          let key = vm
            .script
            .idents
            .get(*key_index as usize)
            .ok_or_else(|| TypeckError::IdentIndexOob)?;
          match map_ty {
            VmType::Map(x) => {
              let mut x = x.clone();
              x.remove_mut(key.as_str());
              Some(VmType::Map(x))
            }
            _ => return Err(TypeckError::NotMap(format!("{:?}", map_ty)).into()),
          }
        }
        TwGraphNode::DeleteFromTable(key_index) => {
          let [table_ty] = validate_in_edges::<1>(node, in_edges, &types)?;
          let key = vm
            .script
            .idents
            .get(*key_index as usize)
            .ok_or_else(|| TypeckError::IdentIndexOob)?;
          match table_ty {
            VmType::Table(x) => {
              let table_ty = vm
                .schema
                .types
                .get(x.name)
                .ok_or_else(|| TypeckError::TableTypeNotFound(x.name.to_string()))?;
              let (field, _) = table_ty.fields.get(key.as_str()).ok_or_else(|| {
                TypeckError::FieldNotPresentInTable(key.clone(), table_ty.name.clone())
              })?;
              if !field.is_optional() {
                return Err(
                  TypeckError::DeletingNonOptionalTableField(key.clone(), table_ty.name.clone())
                    .into(),
                );
              }
              None
            }
            _ => return Err(TypeckError::NotTable(format!("{:?}", table_ty)).into()),
          }
        }
        TwGraphNode::GetField(key_index) => {
          let [map_or_table_ty] = validate_in_edges::<1>(node, in_edges, &types)?;
          let key = vm
            .script
            .idents
            .get(*key_index as usize)
            .ok_or_else(|| TypeckError::IdentIndexOob)?;
          match map_or_table_ty {
            VmType::Map(x) => Some(x.get(key.as_str()).cloned().unwrap_or_else(|| VmType::Null)),
            VmType::Table(x) => {
              let table_ty = vm
                .schema
                .types
                .get(x.name)
                .ok_or_else(|| TypeckError::TableTypeNotFound(x.name.to_string()))?;
              Some(
                table_ty
                  .fields
                  .get(key.as_str())
                  .map(|x| VmType::from(&x.0))
                  .ok_or_else(|| {
                    TypeckError::FieldNotPresentInTable(key.clone(), table_ty.name.clone())
                  })?,
              )
            }
            _ => return Err(TypeckError::NotMapOrTable(format!("{:?}", map_or_table_ty)).into()),
          }
        }
        TwGraphNode::GetSetElement => {
          let [primary_key_value_ty, set_ty] = validate_in_edges::<2>(node, in_edges, &types)?;
          let set_member_ty = extract_set_element_type(set_ty)?;
          let (key, _) = set_ty.primary_key(vm.schema).unwrap();
          match set_member_ty {
            VmType::Table(x) => {
              let table_ty = vm
                .schema
                .types
                .get(x.name)
                .ok_or_else(|| TypeckError::TableTypeNotFound(x.name.to_string()))?;
              let (field_ty, _) = table_ty.fields.get(key).ok_or_else(|| {
                TypeckError::FieldNotPresentInTable(key.to_string(), table_ty.name.clone())
              })?;
              let field_ty = VmType::from(field_ty);
              ensure_covariant(&field_ty, primary_key_value_ty)?;
              Some(set_member_ty.clone())
            }
            _ => return Err(TypeckError::NotTable(format!("{:?}", set_member_ty)).into()),
          }
        }
        TwGraphNode::FilterSet(subgraph_index) => {
          let [subgraph_param, set_ty] = validate_in_edges::<2>(node, in_edges, &types)?;
          let set_member_ty = extract_set_element_type(set_ty)?;
          let subgraph = self.validate_subgraph_call(
            "FilterSet",
            *subgraph_index,
            subgraph_expected_param_types_sink,
            vec![set_member_ty.clone(), subgraph_param.clone()],
          )?;
          let output = subgraph
            .output_type
            .and_then(|x| vm.script.types.get(x as usize).map(VmType::<&'a str>::from));
          if let Some(VmType::Bool) = output {
            Some(VmType::OneOf(vec![set_member_ty.clone(), VmType::Null]))
          } else {
            return Err(
              TypeckError::ExpectingBoolOutputForFilterSubgraphs(format!("{:?}", output)).into(),
            );
          }
        }
        TwGraphNode::InsertIntoMap(key_index) => {
          let [value_ty, map_ty] = validate_in_edges::<2>(node, in_edges, &types)?;
          let key = vm
            .script
            .idents
            .get(*key_index as usize)
            .ok_or_else(|| TypeckError::IdentIndexOob)?;
          match map_ty {
            VmType::Map(x) => {
              let mut x = x.clone();
              x.insert_mut(key.as_str(), value_ty.clone());
              Some(VmType::Map(x))
            }
            _ => return Err(TypeckError::NotMap(format!("{:?}", map_ty)).into()),
          }
        }
        TwGraphNode::InsertIntoSet => {
          let [value_ty, set_ty] = validate_in_edges::<2>(node, in_edges, &types)?;
          match set_ty {
            VmType::Set(x) => {
              ensure_covariant(&x.ty, value_ty)?;
              None
            }
            _ => return Err(TypeckError::NotSet(format!("{:?}", set_ty)).into()),
          }
        }
        TwGraphNode::InsertIntoTable(key_index) => {
          let [value_ty, table_ty] = validate_in_edges::<2>(node, in_edges, &types)?;
          let key = vm
            .script
            .idents
            .get(*key_index as usize)
            .ok_or_else(|| TypeckError::IdentIndexOob)?;
          match table_ty {
            VmType::Table(x) => {
              let table_ty = vm
                .schema
                .types
                .get(x.name)
                .ok_or_else(|| TypeckError::TableTypeNotFound(x.name.to_string()))?;
              let field_ty = table_ty
                .fields
                .get(key.as_str())
                .map(|x| VmType::from(&x.0))
                .ok_or_else(|| {
                  TypeckError::FieldNotPresentInTable(key.clone(), table_ty.name.clone())
                })?;
              ensure_covariant(&field_ty, value_ty)?;
              None
            }
            _ => return Err(TypeckError::NotTable(format!("{:?}", table_ty)).into()),
          }
        }
        TwGraphNode::LoadConst(const_index) => {
          validate_in_edges::<0>(node, in_edges, &types)?;
          let const_value = vm
            .consts
            .get(*const_index as usize)
            .ok_or_else(|| TypeckError::ConstIndexOob)?;
          Some(VmType::from(&**const_value))
        }
        TwGraphNode::LoadParam(param_index) => {
          if *param_index as usize >= params.len() {
            return Err(TypeckError::ParamIndexOob.into());
          }
          Some(params[*param_index as usize].clone())
        }
        TwGraphNode::Eq => {
          let [left, right] = validate_in_edges::<2>(node, in_edges, &types)?;
          ensure_covariant(left, right)?;
          Some(VmType::Bool)
        }
        TwGraphNode::UnwrapOptional => {
          let [input] = validate_in_edges::<1>(node, in_edges, &types)?;
          Some(unwrap_optional(input.clone())?)
        }
      };
      types.push(ty);
    }

    let actual_output_ty = g
      .output
      .map(|x| {
        types
          .get(x as usize)
          .ok_or_else(|| TypeckError::OutputNodeIndexOob)
          .and_then(|x| ensure_type(x.as_ref()))
      })
      .transpose()?;
    match (output_type, actual_output_ty) {
      (Some(a), Some(b)) => ensure_covariant(a, b)?,
      (None, None) => {}
      _ => {
        return Err(
          TypeckError::OutputTypeMismatch(
            format!("{:?}", output_type),
            format!("{:?}", actual_output_ty),
          )
          .into(),
        )
      }
    }

    Ok(types)
  }

  fn validate_subgraph_call(
    &self,
    opname: &'static str,
    subgraph_index: u32,
    sink: &mut HashMap<u32, Vec<HashSet<VmType<&'a str>>>>,
    param_types: Vec<VmType<&'a str>>,
  ) -> Result<&'a TwGraph> {
    let subgraph = self
      .vm
      .script
      .graphs
      .get(subgraph_index as usize)
      .ok_or_else(|| TypeckError::SubgraphIndexOob)?;
    if subgraph.param_types.len() != param_types.len() {
      return Err(
        TypeckError::ParamCountMismatch(
          opname,
          param_types.len() as u32,
          subgraph.param_types.len() as u32,
        )
        .into(),
      );
    }
    let v = sink
      .entry(subgraph_index)
      .or_insert((0..param_types.len()).map(|_| HashSet::new()).collect());
    assert_eq!(v.len(), param_types.len());

    for (x, y) in param_types.into_iter().zip(v.iter_mut()) {
      y.insert(x);
    }
    Ok(subgraph)
  }
}

fn validate_in_edges<'a, 'b, const N: usize>(
  node: &TwGraphNode,
  in_edges: &[u32],
  types: &'b [Option<VmType<&'a str>>],
) -> Result<[&'b VmType<&'a str>; N]> {
  if N != in_edges.len() {
    Err(TypeckError::InEdgeCountMismatch(N, format!("{:?}", node), in_edges.len()).into())
  } else {
    let mut output: [Option<&'b VmType<&'a str>>; N] = [None; N];
    for i in 0..N {
      output[i] = Some(ensure_type(types[in_edges[i] as usize].as_ref())?);
    }
    Ok(unsafe {
      std::mem::transmute_copy::<[Option<&'b VmType<&'a str>>; N], [&'b VmType<&'a str>; N]>(
        &output,
      )
    })
  }
}

fn ensure_type<'a, 'b>(x: Option<&'b VmType<&'a str>>) -> Result<&'b VmType<&'a str>, TypeckError> {
  match x {
    Some(x) => Ok(x),
    None => Err(TypeckError::ExpectingTypedNode.into()),
  }
}

fn ensure_covariant<'a>(dst: &VmType<&'a str>, src: &VmType<&'a str>) -> Result<()> {
  if dst.is_covariant_from(src) {
    Ok(())
  } else {
    Err(TypeckError::NonCovariantTypes(format!("{:?}", dst), format!("{:?}", src)).into())
  }
}

fn extract_list_element_type<'a, 'b>(x: &'b VmType<&'a str>) -> Result<&'b VmType<&'a str>> {
  match x {
    VmType::List(x) => Ok(&**x),
    _ => Err(TypeckError::ExpectingList(format!("{:?}", x)).into()),
  }
}

fn extract_set_element_type<'a, 'b>(x: &'b VmType<&'a str>) -> Result<&'b VmType<&'a str>> {
  match x {
    VmType::Set(x) => Ok(&*x.ty),
    _ => Err(TypeckError::ExpectingSet(format!("{:?}", x)).into()),
  }
}

fn unwrap_optional<'a, 'b>(x: VmType<&'a str>) -> Result<VmType<&'a str>, TypeckError> {
  match x {
    VmType::OneOf(x) if x.iter().find(|x| x.is_null()).is_some() => Ok(flatten_oneof(
      VmType::OneOf(x.into_iter().filter(|x| !x.is_null()).collect()),
    )),
    _ => return Err(TypeckError::CannotUnwrapNonOptional(format!("{:?}", x))),
  }
}

fn flatten_oneof<'a>(x: VmType<&'a str>) -> VmType<&'a str> {
  match x {
    VmType::OneOf(x) if x.len() == 1 => x.into_iter().next().unwrap(),
    _ => x,
  }
}
