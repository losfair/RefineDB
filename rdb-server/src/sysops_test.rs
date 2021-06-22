use bumpalo::Bump;
use rdb_analyzer::{
  data::treewalker::{bytecode::TwScript, typeck::typeck_graph, vm::TwVm},
  schema::{
    compile::{compile, CompiledSchema},
    grammar::parse,
  },
  storage_plan::planner::generate_plan_for_schema,
};

use crate::system::SCHEMA;

fn tyck_sysop(schema: &CompiledSchema, script: &TwScript) {
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
  let vm = TwVm::new(&schema, &plan, script).unwrap();
  for g in &script.graphs {
    typeck_graph(&vm, g).unwrap();
  }
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
fn check_sysop_add_namespace() {
  use crate::sysops::sysop_add_namespace;

  let _ = pretty_env_logger::try_init();
  let schema = get_schema();

  tyck_sysop(&schema, &sysop_add_namespace(&schema));
}

#[test]
fn check_sysop_delete_namespace() {
  use crate::sysops::sysop_delete_namespace;

  let _ = pretty_env_logger::try_init();
  let schema = get_schema();

  tyck_sysop(&schema, &sysop_delete_namespace(&schema));
}
