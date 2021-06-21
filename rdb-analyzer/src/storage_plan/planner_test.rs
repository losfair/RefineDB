use std::convert::TryFrom;

use bumpalo::Bump;
use console::Style;
use similar::{ChangeTag, TextDiff};

use crate::{
  schema::{compile::compile, grammar::parse},
  storage_plan::StoragePlan,
};

use super::planner::generate_plan_for_schema;

const SIMPLE_SCHEMA: &str = r#"
type Item<T> {
  @packed inner: T,
  inner2: T,
  @primary
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

type TrinaryTree<T> {
  left: TrinaryTree<T>?,
  middle: TrinaryTree<T>?,
  right: TrinaryTree<T>?,
  value: T?,
}

type InternalSet {
  @primary
  key: bytes,
  s: set<Wrapper<int64>>,
}

type Wrapper<T> {
  @primary
  value: T,
}

export set<Item<Duration<int64>>> items;
export Recursive<int64> item;
export BinaryTree<int64> a_binary_tree;
export InternalSet an_internal_set;
export set<InternalSet> nested_internal_sets;
export TrinaryTree<int64> a_trinary_tree;
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
  println!(
    "{}",
    serde_yaml::to_string(&StoragePlan::<String>::from(&plan)).unwrap()
  );
}

#[test]
fn planner_example_1() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
  type Item<T> {
    @primary
    id: string,
    value: T,
  }
  type RecursiveItem<T> {
    @primary
    id: string,
    value: T?,
    recursive: RecursiveItem<T>?,
  }
  type Duration<T> {
    start: T,
    end: T,
  }
  export set<Item<Duration<int64>>> items;
  export set<RecursiveItem<Duration<int64>>> recursive_items;
  "#,
  )
  .unwrap();
  let output = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &output).unwrap();
  println!(
    "{}",
    serde_yaml::to_string(&StoragePlan::<String>::from(&plan)).unwrap()
  );
}

#[test]
fn recursion_cycles() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
  type A<T> {
    @primary
    id: string,
    value: B<T>,
  }
  type B<T> {
    value1: A<T>?,
    value2: C<T>?,
  }
  type C<T> {
    value: T,
    that1: A<T>,
    that2: B<T>,
  }
  export set<A<int64>> items;
  "#,
  )
  .unwrap();
  let output = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &output).unwrap();
  println!(
    "{}",
    serde_yaml::to_string(&StoragePlan::<String>::from(&plan)).unwrap()
  );
}

#[test]
fn test_yaml_serialization() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(&alloc, SIMPLE_SCHEMA).unwrap();
  let output = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &output).unwrap();
  let plan2 = serde_yaml::to_string(&StoragePlan::<String>::from(&plan)).unwrap();
  let plan2: StoragePlan<String> = serde_yaml::from_str(&plan2).unwrap();
  let plan2 = StoragePlan::try_from(&plan2).unwrap();
  assert_eq!(
    plan.serialize_compressed().unwrap(),
    plan2.serialize_compressed().unwrap()
  );
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
      @primary
      first: A,
      second: B,
    }
    export BinaryTree<int64> binary_tree;
    export set<Tuple<bytes, BinaryTree<int64>>> set_of_binary_trees;
    export BinaryTree<set<Tuple<int64, int64>>> binary_tree_of_sets;
    export BinaryTree<BinaryTree<int64>> binary_tree_of_binary_trees;
    export BinaryTree<Tuple<string, BinaryTree<string>>> complex_structure;
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
fn test_tuple_set() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
    type Tuple<A, B> {
      first: A,
      second: B,
    }
    type SetBox<T> {
      inner: set<Box<T>>,
    }
    type Box<T> {
      @primary
      inner: T,
    }
    export Tuple<SetBox<string>, set<Box<bytes>>> something;
  "#,
  )
  .unwrap();
  let output = compile(&ast).unwrap();
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &output).unwrap();
  println!("{}", plan);
}

#[test]
fn test_set_member_with_primary_key() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
    type A {
      @primary
      a: int64,
    }
    export set<A> some_set;
  "#,
  )
  .unwrap();
  let output = compile(&ast).unwrap();
  generate_plan_for_schema(&Default::default(), &Default::default(), &output).unwrap();
}

#[test]
fn test_set_member_without_primary_key() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
    type A {
      a: int64,
    }
    export set<A> some_set;
  "#,
  )
  .unwrap();
  let output = compile(&ast).unwrap();
  match generate_plan_for_schema(&Default::default(), &Default::default(), &output) {
    Ok(_) => panic!("test_set_member_without_primary_key: did not get expected error"),
    Err(e) => assert!(e.to_string().contains("has no primary key")),
  }
}

fn run_planner_migration_stats(old: &str, new: &str) -> (usize, usize) {
  struct Line(Option<usize>);

  impl std::fmt::Display for Line {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
      match self.0 {
        None => write!(f, "    "),
        Some(idx) => write!(f, "{:<4}", idx + 1),
      }
    }
  }

  let alloc = Bump::new();
  let ast = parse(&alloc, old).unwrap();
  let schema1 = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan1 = generate_plan_for_schema(&Default::default(), &Default::default(), &schema1).unwrap();

  let alloc = Bump::new();
  let ast = parse(&alloc, new).unwrap();
  let schema2 = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);

  let plan2 = generate_plan_for_schema(&plan1, &schema1, &schema2).unwrap();

  let plan1 = serde_yaml::to_string(&StoragePlan::<String>::from(&plan1)).unwrap();
  let plan2 = serde_yaml::to_string(&StoragePlan::<String>::from(&plan2)).unwrap();
  let diff = TextDiff::from_lines(&plan1, &plan2);
  let mut insert_count = 0usize;
  let mut delete_count = 0usize;
  for (idx, group) in diff.grouped_ops(3).iter().enumerate() {
    if idx > 0 {
      println!("{:-^1$}", "-", 80);
    }
    for op in group {
      for change in diff.iter_inline_changes(op) {
        let (sign, s) = match change.tag() {
          ChangeTag::Delete => {
            delete_count += 1;
            ("-", Style::new().red())
          }
          ChangeTag::Insert => {
            insert_count += 1;
            ("+", Style::new().green())
          }
          ChangeTag::Equal => (" ", Style::new().dim()),
        };
        print!(
          "{}{} |{}",
          console::style(Line(change.old_index())).dim(),
          console::style(Line(change.new_index())).dim(),
          s.apply_to(sign).bold(),
        );
        for (emphasized, value) in change.iter_strings_lossy() {
          if emphasized {
            print!("{}", s.apply_to(value).underlined().on_black());
          } else {
            print!("{}", s.apply_to(value));
          }
        }
        if change.missing_newline() {
          println!();
        }
      }
    }
  }
  (insert_count, delete_count)
}

#[test]
fn test_planner_migration_identity() {
  let _ = pretty_env_logger::try_init();
  let (insert_count, delete_count) = run_planner_migration_stats(SIMPLE_SCHEMA, SIMPLE_SCHEMA);
  assert!(insert_count == 0);
  assert!(delete_count == 0);
}

#[test]
fn test_planner_migration_add_and_remove_field_simple() {
  let _ = pretty_env_logger::try_init();
  let old = r#"
  type Item {
    a: int64,
    b: string,
    c: bytes,
  }
  export Item data;
  "#;
  let new = r#"
  type Item {
    a: int64,
    b: string,
    c: bytes,
    d: string,
  }
  export Item data;
  "#;
  let (insert_count_1, delete_count_1) = run_planner_migration_stats(old, new);
  assert!(insert_count_1 > 0);
  assert!(delete_count_1 == 0);
  println!(
    "test_planner_migration_add_and_remove_field_simple: insert {}, delete {}",
    insert_count_1, delete_count_1
  );
  let (insert_count_2, delete_count_2) = run_planner_migration_stats(new, old);
  assert!(insert_count_2 == 0);
  assert!(delete_count_2 > 0);
  assert_eq!(insert_count_2, delete_count_1);
  assert_eq!(delete_count_2, insert_count_1);
}

#[test]
fn test_planner_migration_mandatory_to_optional() {
  let _ = pretty_env_logger::try_init();
  let old = r#"
  type Item {
    a: int64,
  }
  export Item data;
  "#;
  let new = r#"
  type Item {
    a: int64?,
  }
  export Item data;
  "#;
  let (insert_count_1, delete_count_1) = run_planner_migration_stats(old, new);
  assert!(insert_count_1 == delete_count_1);
  println!(
    "test_planner_migration_mandatory_to_optional: insert {}, delete {}",
    insert_count_1, delete_count_1
  );
}

#[test]
fn test_planner_migration_add_and_remove_field_complex() {
  let _ = pretty_env_logger::try_init();
  let old = r#"
  type BinaryTree<T> {
    left: BinaryTree<T>?,
    right: BinaryTree<T>?,
    value: T?,
  }
  export BinaryTree<int64> data;
  "#;
  let new = r#"
  type BinaryTree<T> {
    left: BinaryTree<T>?,
    right: BinaryTree<T>?,
    value: T?,
    value2: T?,
  }
  export BinaryTree<int64> data;
  "#;
  let (insert_count_1, delete_count_1) = run_planner_migration_stats(old, new);
  assert!(insert_count_1 > 0);
  assert!(delete_count_1 == 0);
  println!(
    "test_planner_migration_add_and_remove_field_complex: insert {}, delete {}",
    insert_count_1, delete_count_1
  );
  let (insert_count_2, delete_count_2) = run_planner_migration_stats(new, old);
  assert!(insert_count_2 == 0);
  assert!(delete_count_2 > 0);
  assert_eq!(insert_count_2, delete_count_1);
  assert_eq!(delete_count_2, insert_count_1);
}

#[test]
fn test_planner_migration_field_rename() {
  let _ = pretty_env_logger::try_init();
  let old = r#"
  type Item {
    a: int64,
    c: int64,
  }
  export Item data;
  "#;
  let new = r#"
  type Item {
    @rename_from("a")
    b: int64,
    c: int64
  }
  export Item data;
  "#;
  let (insert_count_1, delete_count_1) = run_planner_migration_stats(old, new);
  assert_eq!(insert_count_1, 1);
  assert_eq!(delete_count_1, 1);
  println!(
    "test_planner_migration_field_rename: insert {}, delete {}",
    insert_count_1, delete_count_1
  );
}

#[test]
fn primitive_exports() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
    export int64 a;
    export string b;
  "#,
  )
  .unwrap();
  let output = compile(&ast).unwrap();
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &output).unwrap();
  println!("{}", plan);
}
