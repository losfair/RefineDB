//! KV database fixup after schema migration

use anyhow::Result;

use crate::{schema::compile::CompiledSchema, storage_plan::StoragePlan};

use super::kv::KeyValueStore;

pub async fn migrate_schema(
  schema: &CompiledSchema,
  plan: &StoragePlan,
  kv: &dyn KeyValueStore,
) -> Result<()> {
  let txn = kv.begin_transaction().await?;
  Ok(())
}
