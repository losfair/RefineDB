use std::{mem::ManuallyDrop, sync::Arc};

use anyhow::Result;
use rdb_analyzer::{
  data::treewalker::{
    asm::codegen::compile_twscript,
    bytecode::TwScript,
    exec::generate_root_map,
    typeck::{GlobalTyckContext, GlobalTypeInfo},
    vm::TwVm,
    vm_value::VmValue,
  },
  schema::compile::CompiledSchema,
  storage_plan::StoragePlan,
};

pub struct SchemaContext {
  pub schema: CompiledSchema,
  pub plan: StoragePlan,
}

pub struct ExecContext {
  _schema_ctx: Arc<SchemaContext>,
  _script: Box<TwScript>,
  dangerous: ManuallyDrop<DangerousExecContext<'static>>,
}

struct DangerousExecContext<'a> {
  vm: TwVm<'a>,
  type_info: GlobalTypeInfo<'a>,
  root_map: Arc<VmValue<'a>>,
}

impl ExecContext {
  pub fn load(schema_ctx: Arc<SchemaContext>, script: &str) -> Result<Self> {
    let script = Box::new(compile_twscript(script)?);
    let vm = TwVm::new(&schema_ctx.schema, &schema_ctx.plan, &*script)?;
    let type_info = GlobalTyckContext::new(&vm)?.typeck()?;
    let root_map = Arc::new(generate_root_map(&schema_ctx.schema, &schema_ctx.plan)?);
    let dangerous_ctx = DangerousExecContext {
      vm,
      type_info,
      root_map,
    };
    let dangerous_ctx = ManuallyDrop::new(unsafe {
      std::mem::transmute::<DangerousExecContext<'_>, DangerousExecContext<'static>>(dangerous_ctx)
    });
    Ok(Self {
      _schema_ctx: schema_ctx,
      _script: script,
      dangerous: dangerous_ctx,
    })
  }

  pub fn vm<'a>(&'a self) -> &'a TwVm<'a> {
    &self.dangerous.vm
  }

  pub fn type_info<'a>(&'a self) -> &'a GlobalTypeInfo<'a> {
    &self.dangerous.type_info
  }

  pub fn root_map<'a>(&'a self) -> &Arc<VmValue<'a>> {
    &self.dangerous.root_map
  }
}

impl Drop for ExecContext {
  fn drop(&mut self) {
    // Ensure that `dangerous` is dropped before other fields
    unsafe {
      ManuallyDrop::drop(&mut self.dangerous);
    }
  }
}
