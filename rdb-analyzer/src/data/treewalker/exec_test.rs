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
      typeck::typeck_graph,
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
        (TwGraphNode::LoadParam(0), vec![]),           // 0
        (TwGraphNode::GetField(0), vec![0]),           // 1
        (TwGraphNode::GetField(1), vec![1]),           // 2
        (TwGraphNode::GetField(2), vec![1]),           // 3
        (TwGraphNode::GetField(3), vec![3]),           // 4
        (TwGraphNode::CreateMap, vec![]),              // 5
        (TwGraphNode::InsertIntoMap(4), vec![2, 5]),   // 6
        (TwGraphNode::InsertIntoMap(5), vec![4, 6]),   // 7
        (TwGraphNode::LoadConst(0), vec![]),           // 8
        (TwGraphNode::InsertIntoTable(1), vec![8, 1]), // 0
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
      VmType::<String>::from(&schema),
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
  typeck_graph(&vm, &script.graphs[0]).unwrap();

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
        (TwGraphNode::LoadParam(0), vec![]), // 0
        (TwGraphNode::GetField(0), vec![0]), // 1
        (TwGraphNode::GetField(1), vec![1]), // 2
      ],
      output: Some(2),
      effects: vec![],
      output_type: Some(1),
      param_types: vec![0],
    }],
    entry: 0,
    consts: vec![],
    idents: vec!["some_item".into(), "name".into()],
    types: vec![
      VmType::<String>::from(&schema),
      VmType::Primitive(PrimitiveType::String),
    ],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  typeck_graph(&vm, &script.graphs[0]).unwrap();
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
