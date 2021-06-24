use std::convert::TryFrom;

use bumpalo::Bump;
use rdb_analyzer::{
  data::treewalker::{bytecode::TwScript, typeck::GlobalTyckContext, vm::TwVm},
  schema::{
    compile::{compile, CompiledSchema},
    grammar::parse,
  },
  storage_plan::planner::generate_plan_for_schema,
};

use crate::{
  sysops::{SysopCollection, SYSOPS},
  system::SCHEMA,
};

fn tyck_sysop(schema: &CompiledSchema, script: &TwScript) {
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  GlobalTyckContext::new(&vm).unwrap().typeck().unwrap();
}

fn get_schema() -> CompiledSchema {
  let alloc = Bump::new();
  let ast = parse(&alloc, SCHEMA).unwrap();
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  schema
}

#[test]
fn check_sysops() {
  let _ = pretty_env_logger::try_init();
  let schema = get_schema();
  let sysops = SysopCollection::<TwScript>::try_from(&SYSOPS).unwrap();
  tyck_sysop(&schema, &sysops.add_namespace);
  tyck_sysop(&schema, &sysops.delete_namespace);
}
