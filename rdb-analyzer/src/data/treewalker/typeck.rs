use std::sync::Arc;

use anyhow::Result;
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
  #[error("expecting bool output for filter subgraphs, got `{0}`")]
  ExpectingBoolOutputForFilterSubgraphs(String),
  #[error("field `{0}` is not present in table `{1}`")]
  FieldNotPresentInTable(String, Arc<str>),
}

pub fn typeck_graph<'a>(
  vm: &TwVm<'a>,
  g: &TwGraph,
  params: &[&VmType<'a>],
) -> Result<Vec<Option<VmType<'a>>>> {
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

  let mut types: Vec<Option<VmType<'a>>> = Vec::with_capacity(g.nodes.len());
  for (i, (node, in_edges)) in g.nodes.iter().enumerate() {
    // Check in_edges invariant
    for j in in_edges {
      let j = *j as usize;
      if j >= i {
        return Err(TypeckError::InvalidInEdge.into());
      }
    }

    let ty: Option<VmType<'a>> = match node {
      TwGraphNode::BuildSet => {
        let [list_ty] = validate_in_edge_count::<1>(node, in_edges, &types)?;
        let element_ty = extract_list_element_type(list_ty)?;
        Some(VmType::Set(VmSetType {
          ty: Box::new(element_ty.clone()),
        }))
      }
      TwGraphNode::BuildTable(table_ty) => {
        let [map_ty] = validate_in_edge_count::<1>(node, in_edges, &types)?;
        let map_ty = ensure_type(map_ty)?;
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
      TwGraphNode::DeleteFromMap(key_index) => {
        let [map_ty] = validate_in_edge_count::<1>(node, in_edges, &types)?;
        let map_ty = ensure_type(map_ty)?;
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
        let [table_ty] = validate_in_edge_count::<1>(node, in_edges, &types)?;
        let table_ty = ensure_type(table_ty)?;
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
            table_ty.fields.get(key.as_str()).ok_or_else(|| {
              TypeckError::FieldNotPresentInTable(key.clone(), table_ty.name.clone())
            })?;
            None
          }
          _ => return Err(TypeckError::NotTable(format!("{:?}", table_ty)).into()),
        }
      }
      TwGraphNode::GetMapField(key_index) => {
        let [map_ty] = validate_in_edge_count::<1>(node, in_edges, &types)?;
        let map_ty = ensure_type(map_ty)?;
        let key = vm
          .script
          .idents
          .get(*key_index as usize)
          .ok_or_else(|| TypeckError::IdentIndexOob)?;
        match map_ty {
          VmType::Map(x) => Some(x.get(key.as_str()).cloned().unwrap_or_else(|| VmType::Null)),
          _ => return Err(TypeckError::NotMap(format!("{:?}", map_ty)).into()),
        }
      }
      TwGraphNode::GetSetElement(subgraph_index) => {
        let [subgraph_param, set] = validate_in_edge_count::<2>(node, in_edges, &types)?;
        let set_member_ty = extract_set_element_type(set)?;
        let subgraph_params: Vec<&VmType<'a>> = match subgraph_param {
          Some(x) => vec![set_member_ty, x],
          None => vec![set_member_ty],
        };
        let subgraph = vm
          .script
          .graphs
          .get(*subgraph_index as usize)
          .ok_or_else(|| TypeckError::SubgraphIndexOob)?;
        let subgraph_types = typeck_graph(vm, subgraph, &subgraph_params)?;
        let output = subgraph
          .output
          .and_then(|x| subgraph_types[x as usize].clone());
        if let Some(VmType::Bool) = output {
          Some(VmType::OneOf(vec![set_member_ty.clone(), VmType::Null]))
        } else {
          return Err(
            TypeckError::ExpectingBoolOutputForFilterSubgraphs(format!("{:?}", output)).into(),
          );
        }
      }
      TwGraphNode::GetTableField(key_index) => {
        let [table_ty] = validate_in_edge_count::<1>(node, in_edges, &types)?;
        let table_ty = ensure_type(table_ty)?;
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
          _ => return Err(TypeckError::NotTable(format!("{:?}", table_ty)).into()),
        }
      }
      TwGraphNode::InsertIntoMap(key_index) => {
        let [value_ty, map_ty] = validate_in_edge_count::<2>(node, in_edges, &types)?;
        let value_ty = ensure_type(value_ty)?;
        let map_ty = ensure_type(map_ty)?;
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
        let [value_ty, set_ty] = validate_in_edge_count::<2>(node, in_edges, &types)?;
        let value_ty = ensure_type(value_ty)?;
        let set_ty = ensure_type(set_ty)?;
        match set_ty {
          VmType::Set(x) => {
            ensure_covariant(&x.ty, value_ty)?;
            None
          }
          _ => return Err(TypeckError::NotSet(format!("{:?}", set_ty)).into()),
        }
      }
      TwGraphNode::InsertIntoTable(key_index) => {
        let [value_ty, table_ty] = validate_in_edge_count::<2>(node, in_edges, &types)?;
        let value_ty = ensure_type(value_ty)?;
        let table_ty = ensure_type(table_ty)?;
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
        validate_in_edge_count::<0>(node, in_edges, &types)?;
        let const_value = vm
          .consts
          .get(*const_index as usize)
          .ok_or_else(|| TypeckError::ConstIndexOob)?;
        Some(VmType::from(const_value))
      }
      TwGraphNode::LoadParam(param_index) => {
        if *param_index as usize >= params.len() {
          return Err(TypeckError::ParamIndexOob.into());
        }
        Some(params[*param_index as usize].clone())
      }
    };
    types.push(ty);
  }
  Ok(types)
}

fn validate_in_edge_count<'a, 'b, const N: usize>(
  node: &TwGraphNode,
  in_edges: &[u32],
  types: &'b [Option<VmType<'a>>],
) -> Result<[&'b Option<VmType<'a>>; N]> {
  if N != in_edges.len() {
    Err(TypeckError::InEdgeCountMismatch(N, format!("{:?}", node), in_edges.len()).into())
  } else {
    let mut output: [Option<&'b Option<VmType<'a>>>; N] = [None; N];
    for i in 0..N {
      output[i] = Some(&types[in_edges[i] as usize]);
    }

    // SAFETY: This is safe because we have initialized each element of `output` to `Some`.
    let output = unsafe {
      std::mem::transmute_copy::<[Option<&'b Option<VmType<'a>>>; N], [&'b Option<VmType<'a>>; N]>(
        &output,
      )
    };
    Ok(output)
  }
}

fn ensure_type<'a, 'b>(x: &'b Option<VmType<'a>>) -> Result<&'b VmType<'a>> {
  match x {
    Some(x) => Ok(x),
    None => Err(TypeckError::ExpectingTypedNode.into()),
  }
}

fn ensure_covariant<'a>(dst: &VmType<'a>, src: &VmType<'a>) -> Result<()> {
  if dst.is_covariant_from(src) {
    Ok(())
  } else {
    Err(TypeckError::NonCovariantTypes(format!("{:?}", dst), format!("{:?}", src)).into())
  }
}

fn extract_list_element_type<'a, 'b>(x: &'b Option<VmType<'a>>) -> Result<&'b VmType<'a>> {
  match x {
    Some(VmType::List(x)) => Ok(&**x),
    _ => Err(TypeckError::ExpectingList(format!("{:?}", x)).into()),
  }
}

fn extract_set_element_type<'a, 'b>(x: &'b Option<VmType<'a>>) -> Result<&'b VmType<'a>> {
  match x {
    Some(VmType::Set(x)) => Ok(&*x.ty),
    _ => Err(TypeckError::ExpectingSet(format!("{:?}", x)).into()),
  }
}
