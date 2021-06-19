use anyhow::Result;

use crate::{schema::compile::CompiledSchema, storage_plan::StoragePlan};

use super::{bytecode::TwScript, vm_value::VmValue};

pub struct TwVm<'a> {
  pub schema: &'a CompiledSchema,
  pub storage_plan: &'a StoragePlan,
  pub script: &'a TwScript,
  pub consts: Vec<VmValue<'a>>,
}

impl<'a> TwVm<'a> {
  pub fn new(
    schema: &'a CompiledSchema,
    storage_plan: &'a StoragePlan,
    script: &'a TwScript,
  ) -> Result<Self> {
    let consts = script
      .consts
      .iter()
      .map(|x| VmValue::from_const(schema, x))
      .collect::<Result<Vec<_>>>()?;

    Ok(Self {
      schema,
      storage_plan,
      script,
      consts,
    })
  }
}
