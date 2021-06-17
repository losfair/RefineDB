use bumpalo::Bump;

use crate::{
  schema::{compile::compile, grammar::parse},
  storage_plan::{planner::generate_plan_for_schema, StoragePlan},
};

use super::planner::{QueryPlan, QueryPlanner, QueryStep};

fn check_stack(plan: &QueryPlan, start: usize) {
  let mut stack_depth = start;
  for step in &plan.steps {
    let (pop, push): (usize, usize) = match step {
      QueryStep::Const(_) => (0, 1),
      QueryStep::CurrentPoint => (0, 1),
      QueryStep::ExtendPoint(_) => (0, 1),
      QueryStep::LensGet { .. } => (1, 1),
      QueryStep::LensPut { .. } => (2, 0),
      QueryStep::PeekAndFulfullResult(_) => (1, 1),
      QueryStep::PointGet { .. } => (1, 1),
      QueryStep::PointPut { .. } => (2, 0),
      QueryStep::Pop => (1, 0),
      QueryStep::RangeScan { subplan } => {
        check_stack(subplan, 1);
        (2, 0)
      }
      QueryStep::Swap2 => (2, 2),
    };
    assert!(stack_depth >= pop);
    stack_depth -= pop;
    stack_depth += push;
  }
  assert_eq!(stack_depth, start);
}

#[test]
fn simple_point_get() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
  type Item {
    a: int64,
    b: string,
  }
  export Item item;
  "#,
  )
  .unwrap();
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let storage_plan =
    generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
  let mut planner = QueryPlanner::new(&schema, &storage_plan);
  planner.add_query(".item.a").unwrap();
  planner.add_query(".item.b").unwrap();
  let plan = planner.plan().unwrap();
  check_stack(&plan, 0);
  println!("{}", serde_yaml::to_string(&plan).unwrap());
}

#[test]
fn simple_set_scan_with_index() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
  type Item {
    @unique
    a: int64,
    b: string,
  }
  export set<Item> items;
  "#,
  )
  .unwrap();
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let storage_plan =
    generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
  let mut planner = QueryPlanner::new(&schema, &storage_plan);
  planner.add_query(".items[a = 42].a").unwrap();
  planner.add_query(".items[a = 42].b").unwrap();
  let plan = planner.plan().unwrap();
  check_stack(&plan, 0);
  println!("{}", serde_yaml::to_string(&plan).unwrap());
}

#[test]
fn recursive_set_scan_with_index() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
  type Item {
    @unique
    a: int64,
    b: set<Item>,
  }
  export set<Item> items;
  "#,
  )
  .unwrap();
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let storage_plan =
    generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
  println!(
    "{}",
    serde_yaml::to_string(&StoragePlan::<String>::from(&storage_plan)).unwrap()
  );
  let mut planner = QueryPlanner::new(&schema, &storage_plan);
  planner
    .add_query(".items[a = 42].b[a = 21].b[a = 10].b[a = 9].b[a = 8].b[a = 7].a")
    .unwrap();
  let plan = planner.plan().unwrap();
  check_stack(&plan, 0);
  println!("{}", serde_yaml::to_string(&plan).unwrap());
}
