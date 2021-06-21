use std::{
  collections::{BTreeMap, HashMap, HashSet},
  sync::Arc,
  time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use byteorder::{BigEndian, ByteOrder};
use rand::RngCore;

use crate::schema::compile::{CompiledSchema, FieldAnnotation, FieldAnnotationList, FieldType};

use super::{StorageKey, StorageNode, StoragePlan};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PlannerError {
  #[error("missing type: {0}")]
  MissingType(Arc<str>),

  #[error("set member type `{0}` has no primary key")]
  SetMemberTypeWithoutPrimaryKey(Arc<str>),
}

struct PlanState<'a> {
  old_schema: &'a CompiledSchema,
  used_storage_keys: HashSet<StorageKey>,
  recursive_types: HashSet<Arc<str>>,
  set_member_types: HashSet<Arc<str>>,
  fields_in_stack: HashMap<Arc<str>, StorageKey>,
}

/// A point on the old tree.
#[derive(Copy, Clone)]
struct OldTreePoint<'a> {
  name: &'a str,
  ty: &'a FieldType,
  annotations: &'a [FieldAnnotation],
  node: &'a StorageNode,
}

impl<'a> OldTreePoint<'a> {
  fn reduce_optional(mut self) -> Self {
    if let FieldType::Optional(x) = self.ty {
      log::trace!(
        "optional field `{}` of type `{}` reduced to `{}`.",
        self.name,
        self.ty,
        x
      );
      self.ty = &**x;
    } else {
      log::info!("field `{}` was mandatory but now optional", self.name);
    }

    self
  }

  fn reduce_set(mut self) -> Option<Self> {
    if let FieldType::Set(x) = self.ty {
      log::trace!(
        "set `{}` of type `{}` reduced to `{}`.",
        self.name,
        self.ty,
        x
      );
      self.ty = &**x;
      match &self.node.set {
        Some(x) => {
          self.node = &**x;
          Some(self)
        }
        None => {
          log::error!("inconsistency detected: a storage node for the `set` type does not have an element node. dropping field. node: {:?}", self.node);
          None
        }
      }
    } else {
      log::warn!(
        "field `{}` becomes a set - previous value will not be preserved",
        self.name
      );
      None
    }
  }

  fn validate_type(
    self,
    expected_ty: &FieldType,
    expected_annotations: &[FieldAnnotation],
  ) -> Option<Self> {
    if self.ty != expected_ty {
      let mut mandatory_to_optional = false;
      if let FieldType::Optional(x) = expected_ty {
        if &**x == self.ty {
          mandatory_to_optional = true;
        }
      }
      if !mandatory_to_optional {
        log::warn!(
          "field `{}` had type `{}` but the new type is `{}` - previous value will not be preserved",
          self.name,
          self.ty,
          expected_ty,
        );
      }
      return None;
    }

    if self.annotations.iter().find(|x| x.is_packed()).is_some()
      && !expected_annotations
        .iter()
        .find(|x| x.is_packed())
        .is_some()
    {
      log::warn!(
        "field `{}` was not packed but is packed now - previous value will not be preserved",
        self.name
      );
      return None;
    }

    if !self.annotations.iter().find(|x| x.is_packed()).is_some()
      && expected_annotations
        .iter()
        .find(|x| x.is_packed())
        .is_some()
    {
      log::warn!(
        "field `{}` was packed but is not packed now - previous value will not be preserved",
        self.name
      );
      return None;
    }
    Some(self)
  }

  fn resolve_subfield(&self, plan_st: &PlanState<'a>, altnames: &[&str]) -> Option<Self> {
    let (name, child_node) = match altnames
      .iter()
      .find_map(|x| self.node.children.get(*x).map(|y| (*x, y)))
    {
      Some(x) => x,
      None => {
        log::info!(
          "none of the subfields `{:?}` exist in the old version of the type `{}` - creating.",
          altnames,
          self.ty,
        );
        return None;
      }
    };
    log::trace!(
      "subfield `{}` of type `{}` resolved to `{:?}`.",
      name,
      self.ty,
      child_node
    );
    let ty = match self.ty {
      FieldType::Table(type_name) => match plan_st.old_schema.types.get(type_name) {
        Some(x) => x,
        None => {
          log::warn!(
            "subfield `{}`'s type, `{}`, does not exist in the old schema",
            name,
            self.ty
          );
          return None;
        }
      },
      _ => {
        log::warn!(
          "cannot get subfield `{}` on a non-table type `{}`",
          name,
          self.ty
        );
        return None;
      }
    };
    let (child_name, child_ty) = match ty.fields.get_key_value(name) {
      Some(x) => x,
      None => {
        log::warn!(
          "subfield `{}` exists in the old plan but not in the old schema",
          name
        );
        return None;
      }
    };
    Some(Self {
      name: &**child_name,
      ty: &child_ty.0,
      annotations: child_ty.1.as_slice(),
      node: child_node,
    })
  }
}

pub fn generate_plan_for_schema(
  old_plan: &StoragePlan,
  old_schema: &CompiledSchema,
  schema: &CompiledSchema,
) -> Result<StoragePlan> {
  // Collect recursive types
  let mut recursive_types: HashSet<Arc<str>> = HashSet::new();
  let mut set_member_types: HashSet<Arc<str>> = HashSet::new();
  for (_, export_field) in &schema.exports {
    collect_special_types(
      export_field,
      schema,
      &mut HashSet::new(),
      &mut recursive_types,
      &mut set_member_types,
    )?;
  }
  log::debug!(
    "collected {} recursive types reachable from exports",
    recursive_types.len()
  );
  log::debug!(
    "collected {} set member types reachable from exports",
    set_member_types.len()
  );

  let mut plan_st = PlanState {
    old_schema,
    used_storage_keys: HashSet::new(),
    recursive_types,
    fields_in_stack: HashMap::new(),
    set_member_types,
  };

  // Deduplicate also against storage keys used in the previous plan.
  //
  // This is not strictly effective because we may have more than one historic schema
  // versions, but in that case the storage key generation mechanism should be enough
  // to prevent duplicates. (unless we generate a lot of schemas within a single
  // millisecond?)
  for (_, node) in &old_plan.nodes {
    collect_storage_keys(node, &mut plan_st.used_storage_keys);
  }
  log::debug!(
    "collected {} storage keys from old plan",
    plan_st.used_storage_keys.len()
  );
  let mut plan = StoragePlan {
    nodes: BTreeMap::new(),
  };

  for (export_name, export_field) in &schema.exports {
    // Retrieve the point in the old tree where the export possibly exists.
    let old_point = old_schema
      .exports
      .get(&**export_name)
      .and_then(|ty| old_plan.nodes.get(&**export_name).map(|x| (ty, x)))
      .map(|(ty, node)| OldTreePoint {
        name: &**export_name,
        ty,
        annotations: &[],
        node,
      })
      .and_then(|x| x.validate_type(export_field, &[]));

    let node = generate_field(&mut plan_st, schema, export_field, &[], old_point)?;
    plan.nodes.insert(export_name.clone(), node);
  }
  Ok(plan)
}

/// The `old_point` parameter must be validated to match `field` before being passed to this function.
fn generate_field(
  plan_st: &mut PlanState,
  schema: &CompiledSchema,
  field: &FieldType,
  annotations: &[FieldAnnotation],
  old_point: Option<OldTreePoint>,
) -> Result<StorageNode> {
  match field {
    FieldType::Optional(x) => {
      // Push down optional
      generate_field(
        plan_st,
        schema,
        x,
        annotations,
        old_point.map(|x| x.reduce_optional()),
      )
    }
    FieldType::Table(table_name) => {
      // This type has children. Push down.

      // For packed types, don't go down further...
      if annotations.iter().find(|x| x.is_packed()).is_some() {
        return Ok(StorageNode {
          key: old_point
            .map(|x| x.node.key)
            .unwrap_or_else(|| rand_storage_key(plan_st)),
          flattened: false,
          subspace_reference: None,
          packed: true,
          set: None,
          children: BTreeMap::new(),
        });
      }

      // First, check whether we are resolving something recursively...
      if let Some(&key) = plan_st.fields_in_stack.get(table_name) {
        return Ok(StorageNode {
          key: old_point
            .map(|x| x.node.key)
            .unwrap_or_else(|| rand_storage_key(plan_st)),
          flattened: false,
          subspace_reference: Some(key),
          packed: false,
          set: None,
          children: BTreeMap::new(),
        });
      }

      let ty = schema
        .types
        .get(table_name)
        .ok_or_else(|| PlannerError::MissingType(table_name.clone()))?;

      // Push the current state.
      let is_recursive_type;
      let storage_key = old_point
        .map(|x| x.node.key)
        .unwrap_or_else(|| rand_storage_key(plan_st));

      if plan_st.recursive_types.contains(table_name) {
        is_recursive_type = true;
        plan_st
          .fields_in_stack
          .insert(table_name.clone(), storage_key);
      } else {
        is_recursive_type = false;
      }

      let mut children: BTreeMap<Arc<str>, StorageNode> = BTreeMap::new();
      let mut has_primary_key = false;

      // Iterate over the fields & recursively generate storage nodes.
      for subfield in &ty.fields {
        let (_, annotations) = subfield.1;
        let mut altnames = vec![&**subfield.0];
        for ann in annotations {
          match ann {
            FieldAnnotation::RenameFrom(x) => {
              altnames.push(x.as_str());
            }
            _ => {}
          }
        }

        let subfield_old_point = old_point
          .and_then(|x| x.resolve_subfield(plan_st, &altnames))
          .and_then(|x| x.validate_type(&subfield.1 .0, &subfield.1 .1));
        match generate_field(
          plan_st,
          schema,
          &subfield.1 .0,
          &subfield.1 .1,
          subfield_old_point,
        ) {
          Ok(x) => {
            children.insert(subfield.0.clone(), x);
          }
          Err(e) => {
            return Err(e);
          }
        }
        has_primary_key |= annotations.as_slice().is_primary();
      }

      if is_recursive_type {
        plan_st.fields_in_stack.remove(table_name);
      }

      if plan_st.set_member_types.contains(table_name) && !has_primary_key {
        return Err(PlannerError::SetMemberTypeWithoutPrimaryKey(ty.name.clone()).into());
      }

      Ok(StorageNode {
        key: storage_key,
        flattened: true,
        subspace_reference: None,
        packed: false,
        set: None,
        children,
      })
    }
    FieldType::Primitive(_) => {
      // This is a primitive type (leaf node).
      Ok(StorageNode {
        key: old_point
          .map(|x| x.node.key)
          .unwrap_or_else(|| rand_storage_key(plan_st)),
        flattened: false,
        subspace_reference: None,
        packed: false,
        set: None,
        children: BTreeMap::new(),
      })
    }
    FieldType::Set(x) => {
      // This is a set with dynamic node key.
      let inner = generate_field(
        plan_st,
        schema,
        x,
        &[],
        old_point
          .and_then(|x| x.reduce_set())
          .and_then(|y| y.validate_type(x, annotations)),
      )?;
      Ok(StorageNode {
        key: old_point
          .map(|x| x.node.key)
          .unwrap_or_else(|| rand_storage_key(plan_st)),
        flattened: false,
        subspace_reference: None,
        packed: false,
        set: Some(Box::new(inner)),
        children: BTreeMap::new(),
      })
    }
  }
}

fn rand_storage_key(st: &mut PlanState) -> StorageKey {
  loop {
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64;
    let mut timebuf = [0u8; 8];
    BigEndian::write_u64(&mut timebuf, now);

    assert_eq!(timebuf[0], 0);
    assert_eq!(timebuf[1], 0);

    let mut ret = [0u8; 12];
    ret[..6].copy_from_slice(&timebuf[2..]);
    rand::thread_rng().fill_bytes(&mut ret[6..]);

    if st.used_storage_keys.insert(ret) {
      break ret;
    }
  }
}

fn collect_storage_keys(node: &StorageNode, sink: &mut HashSet<StorageKey>) {
  sink.insert(node.key);
  if let Some(x) = &node.set {
    collect_storage_keys(x, sink);
  }
  for (_, child) in &node.children {
    collect_storage_keys(child, sink);
  }
}

fn collect_special_types(
  ty: &FieldType,
  schema: &CompiledSchema,
  state: &mut HashSet<Arc<str>>,
  recursive_types_sink: &mut HashSet<Arc<str>>,
  set_member_types_sink: &mut HashSet<Arc<str>>,
) -> Result<()> {
  match ty {
    FieldType::Optional(x) => collect_special_types(
      x,
      schema,
      state,
      recursive_types_sink,
      set_member_types_sink,
    ),
    FieldType::Set(x) => {
      if let FieldType::Table(x) = &**x {
        set_member_types_sink.insert(x.clone());
      }
      collect_special_types(
        x,
        schema,
        state,
        recursive_types_sink,
        set_member_types_sink,
      )
    }
    FieldType::Primitive(_) => Ok(()),
    FieldType::Table(table_name) => {
      // if a cycle is detected...
      if state.insert(table_name.clone()) == false {
        recursive_types_sink.insert(table_name.clone());
        return Ok(());
      }

      let specialized_ty = schema
        .types
        .get(table_name)
        .ok_or_else(|| PlannerError::MissingType(table_name.clone()))?;

      for (_, (field, annotations)) in &specialized_ty.fields {
        // Skip packed fields
        if annotations.as_slice().is_packed() {
          continue;
        }

        collect_special_types(
          field,
          schema,
          state,
          recursive_types_sink,
          set_member_types_sink,
        )?;
      }

      state.remove(table_name);
      Ok(())
    }
  }
}
