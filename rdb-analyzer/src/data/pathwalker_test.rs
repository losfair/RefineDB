use std::{collections::HashSet, sync::Arc};

use bumpalo::Bump;

use crate::{
  data::value::PrimitiveValue,
  schema::{
    compile::{compile, CompiledSchema, FieldAnnotationList, FieldType},
    grammar::parse,
  },
  storage_plan::{planner::generate_plan_for_schema, StorageNode, StoragePlan},
};

use super::pathwalker::PathWalker;

fn print_path_examples(
  schema: &CompiledSchema,
  field: &FieldType,
  node: &StorageNode,
  walker: Arc<PathWalker>,
  path: &String,
  recursion_set: &mut HashSet<usize>,
) {
  println!("{} -> {}", path, walker.generate_key_pretty());
  match field {
    FieldType::Table(x) => {
      let specialized_ty = schema.types.get(x).unwrap();
      for (name, (field, _)) in &specialized_ty.fields {
        if recursion_set.contains(&(field as *const _ as usize)) {
          continue;
        }
        recursion_set.insert(field as *const _ as usize);
        let path = format!("{}.{}", path, name);
        let walker = walker.enter_field(&**name).unwrap();
        let node = walker.node();
        print_path_examples(schema, field, node, walker, &path, recursion_set);
        recursion_set.remove(&(field as *const _ as usize));
      }
    }
    FieldType::Primitive(_) => {}
    FieldType::Set(ty) => {
      let specialized_ty = match &**ty {
        FieldType::Table(x) => schema.types.get(x).unwrap(),
        _ => unreachable!(),
      };
      let (primary_key_name, (primary_key_ty, _)) = specialized_ty
        .fields
        .iter()
        .find(|(_, (_, ann))| ann.as_slice().is_primary())
        .unwrap();
      let primary_key_example = match primary_key_ty {
        FieldType::Primitive(x) => PrimitiveValue::example_value_for_type(*x),
        _ => unreachable!(),
      };
      let walker = walker.enter_set(&[], &primary_key_example).unwrap();
      let path = format!(
        "{}[{} == {:?}]",
        path, primary_key_name, primary_key_example
      );
      let node = node.set.as_ref().unwrap();
      print_path_examples(schema, &**ty, &**node, walker, &path, recursion_set);
    }
    FieldType::Optional(x) => {
      let path = format!("{}!", path);
      print_path_examples(schema, &**x, node, walker, &path, recursion_set);
    }
  }
}

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
      .enter_set(&[], &PrimitiveValue::String("test".into()))
      .unwrap()
      .generate_key()
      .len(),
    31
  );
  assert_eq!(
    PathWalker::from_export(&plan, "items")
      .unwrap()
      .enter_set(&[], &PrimitiveValue::String("test".into()))
      .unwrap()
      .enter_field("id")
      .unwrap()
      .generate_key()
      .len(),
    31
  );
  assert_eq!(
    PathWalker::from_export(&plan, "recursive_items")
      .unwrap()
      .enter_set(&[], &PrimitiveValue::String("test".into()))
      .unwrap()
      .enter_field("value")
      .unwrap()
      .generate_key()
      .len(),
    31
  );
  assert_eq!(
    PathWalker::from_export(&plan, "recursive_items")
      .unwrap()
      .enter_set(&[], &PrimitiveValue::String("test".into()))
      .unwrap()
      .enter_field("recursive")
      .unwrap()
      .generate_key()
      .len(),
    31
  );
  assert_eq!(
    PathWalker::from_export(&plan, "recursive_items")
      .unwrap()
      .enter_set(&[], &PrimitiveValue::String("test".into()))
      .unwrap()
      .enter_field("recursive")
      .unwrap()
      .enter_field("value")
      .unwrap()
      .generate_key()
      .len(),
    43
  );
  assert_eq!(
    PathWalker::from_export(&plan, "recursive_items")
      .unwrap()
      .enter_set(&[], &PrimitiveValue::String("test".into()))
      .unwrap()
      .enter_field("recursive")
      .unwrap()
      .enter_field("value")
      .unwrap()
      .enter_field("start")
      .unwrap()
      .generate_key()
      .len(),
    43
  );
  assert_eq!(
    PathWalker::from_export(&plan, "recursive_items")
      .unwrap()
      .enter_set(&[], &PrimitiveValue::String("test".into()))
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
    55
  );
  for (export_name, export_ty) in &schema.exports {
    print_path_examples(
      &schema,
      export_ty,
      plan.nodes.get(&**export_name).unwrap(),
      PathWalker::from_export(&plan, &**export_name).unwrap(),
      &format!("{}", export_name),
      &mut HashSet::new(),
    );
  }
}
