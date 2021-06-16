use bumpalo::Bump;

use crate::schema::{compile::compile, grammar::parse};

use super::planner::generate_plan_for_schema;

#[test]
fn test_planner_simple() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
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
  "#,
  )
  .unwrap();
  let output = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &output).unwrap();
  println!("{}", plan);
}

#[test]
fn test_planner_migration() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
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
  "#,
  )
  .unwrap();
  let schema1 = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan1 = generate_plan_for_schema(&Default::default(), &Default::default(), &schema1).unwrap();

  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
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
  "#,
  )
  .unwrap();
  let schema2 = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);

  let plan2 = generate_plan_for_schema(&plan1, &schema1, &schema2).unwrap();
}
