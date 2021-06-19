use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;

use crate::{data::treewalker::bytecode::TwGraphNode, schema::compile::FieldType};

use super::{bytecode::TwGraph, vm::TwVm, vm_value::VmType};

#[derive(Error, Debug)]
pub enum TypeckError {
  #[error("invalid in edge")]
  InvalidInEdge,
  #[error("const index out of bounds")]
  ConstIndexOob,
  #[error("ident index out of bounds")]
  IdentIndexOob,
  #[error("expecting {0} in edges on node `{1}`, got {2}")]
  InEdgeCountMismatch(usize, String, usize),
  #[error("expecting a typed node")]
  ExpectingTypedNode,
  #[error("expecting list, got `{0}`")]
  ExpectingList(String),
  #[error("type `{0}` is not covariant from `{1}`")]
  NonCovariantTypes(String, String),
  #[error("type `{0}` is not a map")]
  NotMap(String),
  #[error("table type `{0}` not found")]
  TableTypeNotFound(String),
  #[error("map field `{0}` is not present in table `{1}`")]
  MapFieldNotPresentInTable(String, Arc<str>),
  #[error("non-optional table field `{0}` is not present in map `{1}`")]
  TableFieldNotPresentInMap(Arc<str>, String),
}

pub fn typeck_graph<'a>(vm: &TwVm<'a>, g: &TwGraph) -> Result<()> {
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
      TwGraphNode::LoadConst(const_index) => {
        validate_in_edge_count::<0>(node, in_edges, &types)?;
        let const_value = vm
          .consts
          .get(*const_index as usize)
          .ok_or_else(|| TypeckError::ConstIndexOob)?;
        Some(VmType::from(const_value))
      }
      TwGraphNode::AppendList => {
        let [actual_element_ty, list_ty] = validate_in_edge_count::<2>(node, in_edges, &types)?;
        let element_ty = extract_list_element_type(list_ty)?;
        ensure_covariant(ensure_type(actual_element_ty)?, element_ty)?;
        Some(VmType::List(Box::new(element_ty.clone())))
      }
      TwGraphNode::BuildSet => {
        let [list_ty] = validate_in_edge_count::<1>(node, in_edges, &types)?;
        let element_ty = extract_list_element_type(list_ty)?;
        Some(VmType::Set(Box::new(element_ty.clone())))
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

        None
      }
      _ => todo!(),
    };
    types.push(ty);
  }
  Ok(())
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
