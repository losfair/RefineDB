use bumpalo::Bump;

use crate::schema::{compile::compile, grammar::parse};

use super::planner::generate_plan_for_schema;

#[test]
fn test_planner_simple() {
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
  let plan = generate_plan_for_schema(&output).unwrap();
  println!("{}", plan);
}
