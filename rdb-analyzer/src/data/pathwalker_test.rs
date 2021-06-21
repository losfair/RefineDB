use bumpalo::Bump;

use crate::{
  data::value::PrimitiveValue,
  schema::{compile::compile, grammar::parse},
  storage_plan::{planner::generate_plan_for_schema, StoragePlan},
};

use super::pathwalker::PathWalker;

#[test]
fn basic() {
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
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
  println!(
    "{}",
    serde_yaml::to_string(&StoragePlan::<String>::from(&plan)).unwrap()
  );
  assert_eq!(
    PathWalker::from_export(&plan, "items")
      .unwrap()
      .enter_set(&PrimitiveValue::String("test".into()))
      .unwrap()
      .generate_key()
      .len(),
    30
  );
  assert_eq!(
    PathWalker::from_export(&plan, "items")
      .unwrap()
      .enter_set(&PrimitiveValue::String("test".into()))
      .unwrap()
      .enter_field("id")
      .unwrap()
      .generate_key()
      .len(),
    30
  );
  assert_eq!(
    PathWalker::from_export(&plan, "recursive_items")
      .unwrap()
      .enter_set(&PrimitiveValue::String("test".into()))
      .unwrap()
      .enter_field("value")
      .unwrap()
      .generate_key()
      .len(),
    30
  );
  assert_eq!(
    PathWalker::from_export(&plan, "recursive_items")
      .unwrap()
      .enter_set(&PrimitiveValue::String("test".into()))
      .unwrap()
      .enter_field("recursive")
      .unwrap()
      .generate_key()
      .len(),
    30
  );
  assert_eq!(
    PathWalker::from_export(&plan, "recursive_items")
      .unwrap()
      .enter_set(&PrimitiveValue::String("test".into()))
      .unwrap()
      .enter_field("recursive")
      .unwrap()
      .enter_field("value")
      .unwrap()
      .generate_key()
      .len(),
    42
  );
  assert_eq!(
    PathWalker::from_export(&plan, "recursive_items")
      .unwrap()
      .enter_set(&PrimitiveValue::String("test".into()))
      .unwrap()
      .enter_field("recursive")
      .unwrap()
      .enter_field("value")
      .unwrap()
      .enter_field("start")
      .unwrap()
      .generate_key()
      .len(),
    42
  );
  assert_eq!(
    PathWalker::from_export(&plan, "recursive_items")
      .unwrap()
      .enter_set(&PrimitiveValue::String("test".into()))
      .unwrap()
      .enter_field("recursive")
      .unwrap()
      .enter_field("recursive")
      .unwrap()
      .enter_field("value")
      .unwrap()
      .enter_field("start")
      .unwrap()
      .generate_key()
      .len(),
    54
  );
}
