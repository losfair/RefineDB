use std::{
  collections::{BTreeMap, HashMap, HashSet},
  sync::Arc,
};

use anyhow::Result;
use rand::RngCore;

use crate::schema::compile::{CompiledSchema, FieldType};

use super::{StorageKey, StorageNode, StorageNodeKey, StoragePlan};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PlannerError {
  #[error("missing type: {0}")]
  MissingType(Arc<str>),
}

struct PlanState {
  subspaces_assigned: HashMap<usize, StorageKey>,
}

struct SubspaceState {
  fields_in_stack: HashSet<usize>,
}

pub fn generate_plan_for_schema(schema: &CompiledSchema) -> Result<StoragePlan> {
  let mut plan_st = PlanState {
    subspaces_assigned: HashMap::new(),
  };
  let mut plan = StoragePlan {
    nodes: BTreeMap::new(),
  };

  for (export_name, export_field) in &schema.exports {
    let node = generate_subspace(&mut plan_st, schema, export_field)?;
    plan.nodes.insert(export_name.clone(), node);
  }
  Ok(plan)
}

fn generate_subspace(
  plan_st: &mut PlanState,
  schema: &CompiledSchema,
  field: &FieldType,
) -> Result<StorageNode> {
  let key = field_type_key(field);

  // If this subspace is already generated, return a `subspace_reference` leaf node...
  if let Some(storage_key) = plan_st.subspaces_assigned.get(&key) {
    return Ok(StorageNode {
      ty: field.clone(),
      key: Some(StorageNodeKey::Const(*storage_key)),
      subspace_reference: true,
      children: BTreeMap::new(),
    });
  }

  // Otherwise, generate the subspace.
  let storage_key = rand_storage_key();
  plan_st.subspaces_assigned.insert(key, storage_key);

  let mut subspace_st = SubspaceState {
    fields_in_stack: HashSet::new(),
  };
  let res = generate_field(plan_st, &mut subspace_st, schema, field);
  plan_st.subspaces_assigned.remove(&key);
  res
}

fn generate_field(
  plan_st: &mut PlanState,
  subspace_st: &mut SubspaceState,
  schema: &CompiledSchema,
  field: &FieldType,
) -> Result<StorageNode> {
  match field {
    FieldType::Optional(x) => {
      // Push down optional
      generate_field(plan_st, subspace_st, schema, x)
    }
    FieldType::Named(x) => {
      // This type has children. Push down.
      let ty = schema
        .types
        .get(x)
        .ok_or_else(|| PlannerError::MissingType(x.clone()))?;

      // Push the current state.
      let key = field_type_key(field);
      subspace_st.fields_in_stack.insert(key);

      let mut children: BTreeMap<Arc<str>, StorageNode> = BTreeMap::new();

      // Iterate over the fields & recursively generate storage nodes.
      for subfield in &ty.fields {
        // If this is recursive, we need a new subspace.
        let res = if subspace_st.fields_in_stack.contains(&field_type_key(field)) {
          generate_subspace(plan_st, schema, field)
        } else {
          generate_field(plan_st, subspace_st, schema, &subfield.1 .0)
        };

        match res {
          Ok(x) => {
            children.insert(subfield.0.clone(), x);
          }
          Err(e) => {
            subspace_st.fields_in_stack.remove(&key);
            return Err(e);
          }
        }
      }
      subspace_st.fields_in_stack.remove(&key);

      Ok(StorageNode {
        ty: field.clone(),
        key: None,
        subspace_reference: false,
        children,
      })
    }
    FieldType::Primitive(_) => {
      // This is a primitive type (leaf node).
      Ok(StorageNode {
        ty: field.clone(),
        key: Some(StorageNodeKey::Const(rand_storage_key())),
        subspace_reference: false,
        children: BTreeMap::new(),
      })
    }
    FieldType::Set(x) => {
      // This is a set with dynamic node key.
      let inner = generate_field(plan_st, subspace_st, schema, x)?;
      Ok(StorageNode {
        ty: field.clone(),
        key: Some(StorageNodeKey::Set(Box::new(inner))),
        subspace_reference: false,
        children: BTreeMap::new(),
      })
    }
  }
}

fn field_type_key(x: &FieldType) -> usize {
  x as *const _ as usize
}

fn rand_storage_key() -> StorageKey {
  let mut ret = [0u8; 16];
  rand::thread_rng().fill_bytes(&mut ret);
  ret
}
