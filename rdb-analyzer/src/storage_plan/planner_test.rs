use bumpalo::Bump;

use crate::schema::{compile::compile, grammar::parse};

use super::planner::generate_plan_for_schema;

const SIMPLE_SCHEMA: &str = r#"
type Item<T> {
  @packed inner: T,
  inner2: T,
  something_else: string,
}
type Duration<T> {
  start: T,
  end: T,
}
type Recursive<T> {
  inner: Recursive<T>?,
}
type BinaryTree<T> {
  left: BinaryTree<T>?,
  right: BinaryTree<T>?,
  value: T?,
}
type InternalSet {
  s: set<int64>,
}
export set<Item<Duration<int64>>> items;
export Recursive<int64> item;
export BinaryTree<int64> a_binary_tree;
export set<BinaryTree<int64>> many_binary_trees;
export InternalSet an_internal_set;
export set<InternalSet> nested_internal_sets;
"#;

#[test]
fn test_planner_simple() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(&alloc, SIMPLE_SCHEMA).unwrap();
  let output = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &output).unwrap();
  println!("{}", plan);
}

#[test]
fn test_many_binary_trees() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
    type BinaryTree<T> {
      left: BinaryTree<T>?,
      right: BinaryTree<T>?,
      value: T?,
    }
    type Tuple<A, B> {
      first: A,
      second: B,
    }
    export BinaryTree<int64> binary_tree;
    export set<BinaryTree<int64>> set_of_binary_trees;
    export BinaryTree<set<int64>> binary_tree_of_sets;
    export BinaryTree<BinaryTree<int64>> binary_tree_of_binary_trees;
    export BinaryTree<Tuple<BinaryTree<int64>, BinaryTree<string>>> complex_structure;
  "#,
  )
  .unwrap();
  let output = compile(&ast).unwrap();
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &output).unwrap();
  println!(
    "test_many_binary_trees: serialized size of plan: {}",
    plan.serialize_compressed().unwrap().len()
  );
}

#[test]
fn test_planner_migration_identity() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(&alloc, SIMPLE_SCHEMA).unwrap();
  let schema1 = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan1 = generate_plan_for_schema(&Default::default(), &Default::default(), &schema1).unwrap();

  let alloc = Bump::new();
  let ast = parse(&alloc, SIMPLE_SCHEMA).unwrap();
  let schema2 = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);

  let plan2 = generate_plan_for_schema(&plan1, &schema1, &schema2).unwrap();
  assert_eq!(
    plan1.serialize_compressed().unwrap(),
    plan2.serialize_compressed().unwrap(),
  )
}
