use std::sync::Arc;

use bumpalo::Bump;
use rpds::RedBlackTreeMapSync;

use crate::{
  data::{
    fixup::migrate_schema,
    mock_kv::MockKv,
    pathwalker::PathWalker,
    treewalker::{
      bytecode::{TwGraph, TwGraphNode, TwScript},
      exec::Executor,
      typeck::GlobalTyckContext,
      vm::TwVm,
      vm_value::{VmConst, VmType},
    },
    value::PrimitiveValue,
  },
  schema::{
    compile::{compile, CompiledSchema, FieldType, PrimitiveType},
    grammar::parse,
  },
  storage_plan::{planner::generate_plan_for_schema, StoragePlan},
};

use super::vm_value::{
  VmMapValue, VmSetValue, VmSetValueKind, VmTableValue, VmTableValueKind, VmValue,
};

fn root_map<'a>(schema: &'a CompiledSchema, plan: &'a StoragePlan) -> VmValue<'a> {
  let mut m = RedBlackTreeMapSync::new_sync();
  for (field_name, field_ty) in &schema.exports {
    match field_ty {
      FieldType::Table(x) => {
        m.insert_mut(
          &**field_name,
          Arc::new(VmValue::Table(VmTableValue {
            ty: &**x,
            kind: VmTableValueKind::Resident(PathWalker::from_export(plan, &**field_name).unwrap()),
          })),
        );
      }
      FieldType::Set(x) => {
        m.insert_mut(
          &**field_name,
          Arc::new(VmValue::Set(VmSetValue {
            member_ty: VmType::from(&**x),
            kind: VmSetValueKind::Resident(PathWalker::from_export(plan, &**field_name).unwrap()),
          })),
        );
      }
      _ => unimplemented!(),
    }
  }
  VmValue::Map(VmMapValue { elements: m })
}

#[tokio::test]
async fn basic_exec() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
  type Item {
    id: string,
    name: string,
    duration: Duration<int64>,
  }
  type Duration<T> {
    start: T,
    end: T,
  }
  export Item some_item;
  "#,
  )
  .unwrap();
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
  let script = TwScript {
    graphs: vec![TwGraph {
      nodes: vec![
        (TwGraphNode::LoadParam(0), vec![], None),           // 0
        (TwGraphNode::GetField(0), vec![0], None),           // 1
        (TwGraphNode::GetField(1), vec![1], None),           // 2
        (TwGraphNode::GetField(2), vec![1], None),           // 3
        (TwGraphNode::GetField(3), vec![3], None),           // 4
        (TwGraphNode::CreateMap, vec![], None),              // 5
        (TwGraphNode::InsertIntoMap(4), vec![2, 5], None),   // 6
        (TwGraphNode::InsertIntoMap(5), vec![4, 6], None),   // 7
        (TwGraphNode::LoadConst(0), vec![], None),           // 8
        (TwGraphNode::InsertIntoTable(1), vec![8, 1], None), // 0
      ],
      output: Some(7),
      effects: vec![],
      output_type: Some(1),
      param_types: vec![0],
    }],
    entry: 0,
    consts: vec![VmConst::Primitive(PrimitiveValue::String(
      "test_name".into(),
    ))],
    idents: vec![
      "some_item".into(),
      "name".into(),
      "duration".into(),
      "start".into(),
      "field_1".into(),
      "field_2".into(),
    ],
    types: vec![
      VmType::Schema,
      VmType::Map(
        vec![
          (
            "field_1".to_string(),
            VmType::Primitive(PrimitiveType::String),
          ),
          (
            "field_2".to_string(),
            VmType::Primitive(PrimitiveType::Int64),
          ),
        ]
        .into_iter()
        .collect(),
      ),
      VmType::Primitive(PrimitiveType::Int64),
    ],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  GlobalTyckContext::new(&vm).unwrap().typeck().unwrap();

  let kv = MockKv::new();
  migrate_schema(&schema, &plan, &kv).await.unwrap();
  let executor = Executor::new_assume_typechecked(&vm, &kv);
  let output = executor
    .run_graph(0, &[Arc::new(root_map(&schema, &plan))])
    .await
    .unwrap();
  println!("{:?}", output);
  let output = output.unwrap();
  match &*output {
    VmValue::Map(x) => {
      match &**x.elements.get("field_1").unwrap() {
        VmValue::Primitive(PrimitiveValue::String(x)) if x == "" => {}
        _ => unreachable!(),
      }
      match &**x.elements.get("field_2").unwrap() {
        VmValue::Primitive(PrimitiveValue::Int64(x)) if *x == 0 => {}
        _ => unreachable!(),
      }
    }
    _ => unreachable!(),
  }

  let script = TwScript {
    graphs: vec![TwGraph {
      nodes: vec![
        (TwGraphNode::LoadParam(0), vec![], None), // 0
        (TwGraphNode::GetField(0), vec![0], None), // 1
        (TwGraphNode::GetField(1), vec![1], None), // 2
      ],
      output: Some(2),
      effects: vec![],
      output_type: Some(1),
      param_types: vec![0],
    }],
    entry: 0,
    consts: vec![],
    idents: vec!["some_item".into(), "name".into()],
    types: vec![VmType::Schema, VmType::Primitive(PrimitiveType::String)],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  GlobalTyckContext::new(&vm).unwrap().typeck().unwrap();
  let executor = Executor::new_assume_typechecked(&vm, &kv);
  let output = executor
    .run_graph(0, &[Arc::new(root_map(&schema, &plan))])
    .await
    .unwrap();
  println!("{:?}", output);
  match &*output.unwrap() {
    VmValue::Primitive(PrimitiveValue::String(x)) if x == "test_name" => {}
    _ => unreachable!(),
  };
}

#[tokio::test]
async fn set_queries() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
  type Item {
    @primary
    id: string,
    name: string,
  }
  export set<Item> some_item;
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
  let script = TwScript {
    graphs: vec![TwGraph {
      nodes: vec![
        (TwGraphNode::LoadParam(0), vec![], None),         // 0
        (TwGraphNode::LoadConst(0), vec![], None),         // 1
        (TwGraphNode::LoadConst(1), vec![], None),         // 2
        (TwGraphNode::CreateMap, vec![], None),            // 3
        (TwGraphNode::InsertIntoMap(1), vec![1, 3], None), // 4
        (TwGraphNode::InsertIntoMap(2), vec![2, 4], None), // 5
        (TwGraphNode::BuildTable(3), vec![5], None),       // 6
        (TwGraphNode::GetField(0), vec![0], None),         // 7
        (TwGraphNode::InsertIntoSet, vec![6, 7], None),    // 8
      ],
      output: None,
      effects: vec![],
      output_type: None,
      param_types: vec![0],
    }],
    entry: 0,
    consts: vec![
      VmConst::Primitive(PrimitiveValue::String("test_id".into())),
      VmConst::Primitive(PrimitiveValue::String("test_name".into())),
    ],
    idents: vec![
      "some_item".into(),
      "id".into(),
      "name".into(),
      "Item<>".into(),
    ],
    types: vec![VmType::Schema],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  GlobalTyckContext::new(&vm).unwrap().typeck().unwrap();

  let kv = MockKv::new();
  migrate_schema(&schema, &plan, &kv).await.unwrap();
  let executor = Executor::new_assume_typechecked(&vm, &kv);
  executor
    .run_graph(0, &[Arc::new(root_map(&schema, &plan))])
    .await
    .unwrap();

  let script = TwScript {
    graphs: vec![TwGraph {
      nodes: vec![
        (TwGraphNode::LoadParam(0), vec![], None),      // 0
        (TwGraphNode::LoadConst(0), vec![], None),      // 1
        (TwGraphNode::GetField(0), vec![0], None),      // 2
        (TwGraphNode::GetSetElement, vec![1, 2], None), // 3
        (TwGraphNode::GetField(1), vec![3], None),      // 4
      ],
      output: Some(4),
      effects: vec![],
      output_type: Some(1),
      param_types: vec![0],
    }],
    entry: 0,
    consts: vec![VmConst::Primitive(PrimitiveValue::String("test_id".into()))],
    idents: vec!["some_item".into(), "name".into()],
    types: vec![VmType::Schema, VmType::Primitive(PrimitiveType::String)],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  GlobalTyckContext::new(&vm).unwrap().typeck().unwrap();
  let executor = Executor::new_assume_typechecked(&vm, &kv);
  let output = executor
    .run_graph(0, &[Arc::new(root_map(&schema, &plan))])
    .await
    .unwrap();
  println!("{:?}", output);
  match &*output.unwrap() {
    VmValue::Primitive(PrimitiveValue::String(x)) if x == "test_name" => {}
    _ => unreachable!(),
  };

  let script = TwScript {
    graphs: vec![TwGraph {
      nodes: vec![
        (TwGraphNode::LoadParam(0), vec![], None),      // 0
        (TwGraphNode::LoadConst(0), vec![], None),      // 1
        (TwGraphNode::GetField(0), vec![0], None),      // 2
        (TwGraphNode::DeleteFromSet, vec![1, 2], None), // 3
      ],
      output: None,
      effects: vec![],
      output_type: None,
      param_types: vec![0],
    }],
    entry: 0,
    consts: vec![VmConst::Primitive(PrimitiveValue::String("test_id".into()))],
    idents: vec!["some_item".into(), "name".into()],
    types: vec![
      VmType::<String>::from(&schema),
      VmType::Primitive(PrimitiveType::String),
    ],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  GlobalTyckContext::new(&vm).unwrap().typeck().unwrap();
  let executor = Executor::new_assume_typechecked(&vm, &kv);
  executor
    .run_graph(0, &[Arc::new(root_map(&schema, &plan))])
    .await
    .unwrap();

  let script = TwScript {
    graphs: vec![TwGraph {
      nodes: vec![
        (TwGraphNode::LoadParam(0), vec![], None),      // 0
        (TwGraphNode::LoadConst(0), vec![], None),      // 1
        (TwGraphNode::GetField(0), vec![0], None),      // 2
        (TwGraphNode::GetSetElement, vec![1, 2], None), // 3
        (TwGraphNode::GetField(1), vec![3], None),      // 4
      ],
      output: Some(4),
      effects: vec![],
      output_type: Some(1),
      param_types: vec![0],
    }],
    entry: 0,
    consts: vec![VmConst::Primitive(PrimitiveValue::String("test_id".into()))],
    idents: vec!["some_item".into(), "name".into()],
    types: vec![VmType::Schema, VmType::Primitive(PrimitiveType::String)],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  GlobalTyckContext::new(&vm).unwrap().typeck().unwrap();
  let executor = Executor::new_assume_typechecked(&vm, &kv);
  let output = executor
    .run_graph(0, &[Arc::new(root_map(&schema, &plan))])
    .await
    .unwrap();
  println!("{:?}", output);
  match &*output.unwrap() {
    VmValue::Null => {}
    _ => unreachable!(),
  };
}
