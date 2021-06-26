//! KV database fixup after schema migration

use std::sync::Arc;

use anyhow::Result;

use crate::{
  schema::compile::{CompiledSchema, FieldAnnotation, FieldType},
  storage_plan::StoragePlan,
};

use super::{
  kv::{KeyValueStore, KvTransaction},
  pathwalker::PathWalker,
  value::PrimitiveValue,
};
use async_recursion::async_recursion;

/// Deprecated: DO NOT USE. Let's just leave the added fields empty.
///
/// It's unlikely that for large datasets we can complete the migration transaction in a
/// reasonable time.
pub async fn migrate_schema(
  schema: &CompiledSchema,
  plan: &StoragePlan,
  kv: &dyn KeyValueStore,
) -> Result<()> {
  let txn = kv.begin_transaction().await?;
  let mut futures = vec![];
  for (export_name, field_ty) in &schema.exports {
    let walker = PathWalker::from_export(plan, export_name)?;
    futures.push(walk_and_migrate(&*txn, schema, walker, field_ty, &[]));
  }
  futures::future::try_join_all(futures).await?;
  txn.commit().await?;
  Ok(())
}

#[async_recursion]
async fn walk_and_migrate<'a>(
  txn: &dyn KvTransaction,
  schema: &'a CompiledSchema,
  walker: Arc<PathWalker<'a>>,
  field_ty: &'a FieldType,
  annotations: &[FieldAnnotation],
) -> Result<()> {
  // First, ensure that this field is present...
  let key = walker.generate_key();
  let was_present = txn.get(&key).await?.is_some();
  if !was_present {
    if field_ty.is_optional() {
      // Don't go down further if this is an optional field that does not exist
      return Ok(());
    } else {
      // Otherwise, this is a new non-optional field and let's use the default value
      let default_value = match field_ty {
        FieldType::Primitive(x) => {
          if let Some(x) = annotations.iter().find_map(|x| match x {
            FieldAnnotation::Default(x) => Some(x),
            _ => None,
          }) {
            rmp_serde::to_vec(x)?
          } else {
            rmp_serde::to_vec(&PrimitiveValue::default_value_for_type(*x))?
          }
        }
        _ => vec![],
      };
      txn.put(&key, &default_value).await?;
    }
  }

  // Now we can unwrap the optional type, knowing that a value already exists.
  match field_ty.optional_unwrapped() {
    FieldType::Table(x) => {
      let ty = schema.types.get(&**x).unwrap();
      for (field_name, (field_ty, field_annotations)) in &ty.fields {
        let walker = walker.enter_field(&**field_name)?;
        walk_and_migrate(txn, schema, walker, field_ty, field_annotations.as_slice()).await?;
      }
    }
    FieldType::Set(member_ty) => {
      let range_start = walker.set_fast_scan_prefix().unwrap();
      let mut range_end = range_start.clone();
      *range_end.last_mut().unwrap() += 1;
      let range_end = range_end;

      let mut it = txn.scan_keys(&range_start, &range_end).await?;
      while let Some(k) = it.next().await? {
        let k = k.strip_prefix(range_start.as_slice()).unwrap();
        let walker = walker.enter_set_raw(k).unwrap();
        walk_and_migrate(txn, schema, walker, &**member_ty, &[]).await?;
      }
    }
    _ => {}
  }
  Ok(())
}
