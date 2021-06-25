mod dfvis;
mod memkv;
mod query;

use std::{ffi::CStr, os::raw::c_char, panic::AssertUnwindSafe, ptr::NonNull, sync::Arc};

use anyhow::Result;
use bumpalo::Bump;
use dfvis::visualize_df;
use memkv::MemKv;
use query::{get_vm_graphs, run_vm_query, VmGraphQuery};
use rdb_analyzer::{
  data::treewalker::{
    asm::codegen::compile_twscript,
    bytecode::TwScript,
    typeck::{GlobalTyckContext, GlobalTypeInfo},
    vm::TwVm,
  },
  schema::{
    compile::{compile, CompiledSchema},
    grammar::parse,
  },
  storage_plan::{planner::generate_plan_for_schema, StoragePlan},
};

fn main() {}

#[no_mangle]
pub extern "C" fn rdb_pgsvc_init() {
  wrap("rdb_pgsvc_init", || {
    std::env::set_var("RUST_LOG", "debug");
    pretty_env_logger::init_timed();
    log::info!("rdb_pgsvc initialized");
    Ok(())
  });
}

#[no_mangle]
pub extern "C" fn rdb_drop_schema(_: Option<Box<CompiledSchema>>) {}

#[no_mangle]
pub extern "C" fn rdb_drop_plan(_: Option<Box<StoragePlan>>) {}

#[no_mangle]
pub extern "C" fn rdb_drop_twscript<'a>(_: Option<Box<TwScript>>) {}

#[no_mangle]
pub extern "C" fn rdb_drop_vm<'a>(_: Option<Box<TwVm<'a>>>) {}

#[no_mangle]
pub extern "C" fn rdb_drop_global_type_info<'a>(_: Option<Box<GlobalTypeInfo<'a>>>) {}

#[no_mangle]
pub extern "C" fn rdb_acquire_memkv(x: Option<&Arc<MemKv>>) -> Option<Box<Arc<MemKv>>> {
  x.map(|x| Box::new(x.clone()))
}

#[no_mangle]
pub extern "C" fn rdb_release_memkv(_: Option<Box<Arc<MemKv>>>) {}

#[no_mangle]
pub unsafe extern "C" fn rdb_compile_schema(schema: *const c_char) -> Option<Box<CompiledSchema>> {
  wrap("rdb_compile_schema", || {
    let schema = CStr::from_ptr(schema);
    let schema = schema.to_str()?;
    let schema = compile(&parse(&Bump::new(), schema)?)?;
    Ok(Box::new(schema))
  })
}

#[no_mangle]
pub unsafe extern "C" fn rdb_dfasm(source: *const c_char) -> Option<Box<TwScript>> {
  wrap("rdb_dfasm", || {
    let source = CStr::from_ptr(source);
    let source = source.to_str()?;
    let twscript = compile_twscript(source)?;
    Ok(Box::new(twscript))
  })
}

#[no_mangle]
pub extern "C" fn rdb_vm_create<'a>(
  schema: &'a CompiledSchema,
  plan: &'a StoragePlan,
  script: &'a TwScript,
) -> Option<Box<TwVm<'a>>> {
  wrap("rdb_vm_create", || {
    Ok(Box::new(TwVm::new(schema, plan, script)?))
  })
}

#[no_mangle]
pub extern "C" fn rdb_vm_tyck<'a>(vm: &TwVm<'a>) -> Option<Box<GlobalTypeInfo<'a>>> {
  wrap("rdb_vm_tyck", || {
    Ok(Box::new(GlobalTyckContext::new(vm)?.typeck()?))
  })
}

#[no_mangle]
pub extern "C" fn rdb_vm_visualize_df<'a>(vm: &TwVm<'a>) -> Option<NonNull<c_char>> {
  wrap("rdb_vm_visualize_df", || Ok(mkcstr(&visualize_df(vm)?)))
}

#[no_mangle]
pub extern "C" fn rdb_vm_get_graphs<'a>(vm: &TwVm<'a>) -> Option<NonNull<c_char>> {
  wrap("rdb_vm_get_graphs", || {
    Ok(mkcstr(&serde_json::to_string(&get_vm_graphs(vm))?))
  })
}

#[no_mangle]
pub extern "C" fn rdb_vm_run_query<'a>(
  vm: &TwVm<'a>,
  kv: &Arc<MemKv>,
  type_info: &GlobalTypeInfo<'a>,
  query: *const c_char,
) -> Option<NonNull<c_char>> {
  wrap("rdb_vm_run_query", || {
    let query = unsafe { CStr::from_ptr(query) };
    let query: VmGraphQuery = serde_json::from_str(query.to_str()?)?;
    Ok(mkcstr(&serde_json::to_string(&run_vm_query(
      vm, &**kv, type_info, &query,
    )?)?))
  })
}

#[no_mangle]
pub extern "C" fn rdb_memkv_create() -> Option<Box<Arc<MemKv>>> {
  wrap("rdb_memkv_create", || Ok(Box::new(Arc::new(MemKv::new()))))
}

#[no_mangle]
pub extern "C" fn rdb_generate_storage_plan(
  schema: &CompiledSchema,
  old_schema: Option<&CompiledSchema>,
  old_plan: Option<&StoragePlan>,
) -> Option<Box<StoragePlan>> {
  wrap("rdb_generate_storage_plan", || {
    let mut reference_schema = &CompiledSchema::default();
    let mut reference_plan = &StoragePlan::default();

    if let Some(old_schema) = old_schema {
      let old_plan = old_plan.unwrap();
      reference_schema = old_schema;
      reference_plan = old_plan;
    }

    let new_plan = generate_plan_for_schema(reference_plan, reference_schema, schema)?;
    Ok(Box::new(new_plan))
  })
}

#[no_mangle]
pub extern "C" fn rdb_pretty_print_storage_plan(plan: &StoragePlan) -> Option<NonNull<c_char>> {
  wrap("rdb_pretty_print_storage_plan", || {
    let s = format!(
      "{}",
      serde_yaml::to_string(&StoragePlan::<String>::from(plan)).unwrap()
    );
    Ok(mkcstr(&s))
  })
}

fn wrap<T>(name: &str, x: impl FnOnce() -> Result<T>) -> Option<T> {
  match std::panic::catch_unwind(AssertUnwindSafe(x)) {
    Ok(Ok(x)) => Some(x),
    Ok(Err(e)) => {
      log::error!("{}: error: {:?}", name, e);
      None
    }
    Err(_) => {
      log::error!("{}: panic", name);
      None
    }
  }
}

fn mkcstr(s: &str) -> NonNull<c_char> {
  let s = s.as_bytes();
  unsafe {
    let p = libc::malloc(s.len() + 1);
    if p.is_null() {
      panic!("mkcstr: malloc failed");
    }
    {
      let slice = std::slice::from_raw_parts_mut(p as *mut u8, s.len() + 1);
      slice[..s.len()].copy_from_slice(s);
      slice[s.len()] = 0;
    }
    NonNull::new_unchecked(p as *mut c_char)
  }
}
