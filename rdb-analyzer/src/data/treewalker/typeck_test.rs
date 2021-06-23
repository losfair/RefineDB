use bumpalo::Bump;
use rpds::RedBlackTreeMapSync;

use crate::{
  data::{
    treewalker::{
      bytecode::{TwGraph, TwGraphNode},
      typeck::GlobalTyckContext,
      vm::TwVm,
      vm_value::{VmConst, VmType},
    },
    value::PrimitiveValue,
  },
  schema::{
    compile::{compile, PrimitiveType},
    grammar::parse,
  },
  storage_plan::planner::generate_plan_for_schema,
};

use super::{bytecode::TwScript, vm_value::VmTableType};

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

/*
fn root_map<'a>(schema: &'a CompiledSchema, plan: &'a StoragePlan) -> VmValue<'a> {
  let mut m = RedBlackTreeMapSync::new_sync();
  m.insert_mut(
    "a_trinary_tree",
    Arc::new(VmValue::Table(VmTableValue {
      ty: "TrinaryTree<int64>",
      kind: VmTableValueKind::Resident(Arc::new(VmResidentPath {
        storage_key: VmResidentStorageKey::Fixed(
          &plan.nodes.get("TrinaryTree<int64>").unwrap().key,
        ),
        prev: None,
      })),
    })),
  );
  VmValue::Map(VmMapValue { elements: m })
}
*/

#[test]
fn basic_typeck() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(&alloc, SIMPLE_SCHEMA).unwrap();
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
  let script = TwScript {
    graphs: vec![TwGraph {
      nodes: vec![
        (TwGraphNode::LoadParam(0), vec![], None),    // 0
        (TwGraphNode::GetField(0), vec![0], None),    // 1
        (TwGraphNode::GetField(1), vec![1], None),    // 2
        (TwGraphNode::UnwrapOptional, vec![2], None), // 3
        (TwGraphNode::GetField(2), vec![3], None),    // 4
        (TwGraphNode::UnwrapOptional, vec![4], None), // 5
        (TwGraphNode::GetField(3), vec![5], None),    // 6
        (TwGraphNode::UnwrapOptional, vec![6], None), // 7
      ],
      output: Some(7),
      effects: vec![],
      output_type: Some(1),
      param_types: vec![0],
    }],
    entry: 0,
    consts: vec![],
    idents: vec![
      "a_trinary_tree".into(),
      "middle".into(),
      "left".into(),
      "value".into(),
    ],
    types: vec![VmType::Schema, VmType::Primitive(PrimitiveType::Int64)],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  GlobalTyckContext::new(&vm).unwrap().typeck().unwrap();
}

#[test]
fn filter_set() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(&alloc, SIMPLE_SCHEMA).unwrap();
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
  let script = TwScript {
    graphs: vec![
      TwGraph {
        nodes: vec![
          (TwGraphNode::LoadParam(0), vec![], None),     // 0
          (TwGraphNode::GetField(0), vec![0], None),     // 1
          (TwGraphNode::LoadConst(1), vec![], None),     // 2
          (TwGraphNode::FilterSet(1), vec![2, 1], None), // 3
        ],
        output: Some(3),
        effects: vec![],
        output_type: Some(1),
        param_types: vec![0],
      },
      TwGraph {
        nodes: vec![
          (TwGraphNode::LoadConst(0), vec![], None), // 0
        ],
        output: Some(0),
        effects: vec![],
        output_type: Some(2),
        param_types: vec![3, 3],
      },
    ],
    entry: 0,
    consts: vec![VmConst::Bool(true), VmConst::Null],
    idents: vec![
      "items".into(),
      "middle".into(),
      "left".into(),
      "value".into(),
    ],
    types: vec![
      VmType::Schema,
      VmType::OneOf(vec![
        VmType::Null,
        VmType::Table(VmTableType {
          name: "Item<Duration<int64>>".into(),
        }),
      ]),
      VmType::Bool,
      VmType::Unknown,
    ],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  GlobalTyckContext::new(&vm).unwrap().typeck().unwrap();
}

#[test]
fn basic_typeck_fail_unknown_name() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(&alloc, SIMPLE_SCHEMA).unwrap();
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
  let script = TwScript {
    graphs: vec![TwGraph {
      nodes: vec![
        (TwGraphNode::LoadParam(0), vec![], None),    // 0
        (TwGraphNode::GetField(0), vec![0], None),    // 1
        (TwGraphNode::GetField(1), vec![1], None),    // 2
        (TwGraphNode::UnwrapOptional, vec![2], None), // 3
        (TwGraphNode::GetField(2), vec![3], None),    // 4
        (TwGraphNode::UnwrapOptional, vec![4], None), // 5
        (TwGraphNode::GetField(3), vec![5], None),    // 6
        (TwGraphNode::UnwrapOptional, vec![6], None), // 7
      ],
      output: Some(7),
      effects: vec![],
      output_type: Some(1),
      param_types: vec![0],
    }],
    entry: 0,
    consts: vec![],
    idents: vec![
      "a_trinary_tree".into(),
      "middle".into(),
      "left_".into(),
      "value".into(),
    ],
    types: vec![VmType::Schema, VmType::Primitive(PrimitiveType::Int64)],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  assert!(
    GlobalTyckContext::new(&vm)
      .unwrap()
      .typeck()
      .unwrap_err()
      .to_string()
      == "field `left_` is not present in table `TrinaryTree<int64>`"
  );
}

#[test]
fn basic_typeck_fail_missing_unwrap() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(&alloc, SIMPLE_SCHEMA).unwrap();
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
  let script = TwScript {
    graphs: vec![TwGraph {
      nodes: vec![
        (TwGraphNode::LoadParam(0), vec![], None),    // 0
        (TwGraphNode::GetField(0), vec![0], None),    // 1
        (TwGraphNode::GetField(1), vec![1], None),    // 2
        (TwGraphNode::UnwrapOptional, vec![2], None), // 3
        (TwGraphNode::GetField(2), vec![3], None),    // 4
        (TwGraphNode::UnwrapOptional, vec![4], None), // 5
        (TwGraphNode::GetField(3), vec![5], None),    // 6
      ],
      output: Some(6),
      effects: vec![],
      output_type: Some(1),
      param_types: vec![0],
    }],
    entry: 0,
    consts: vec![],
    idents: vec![
      "a_trinary_tree".into(),
      "middle".into(),
      "left".into(),
      "value".into(),
    ],
    types: vec![VmType::Schema, VmType::Primitive(PrimitiveType::Int64)],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  assert!(GlobalTyckContext::new(&vm)
    .unwrap()
    .typeck()
    .unwrap_err()
    .to_string()
    .contains("type `Primitive(Int64)` is not covariant from"));
}

#[test]
fn basic_typeck_output_type_mismatch() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(&alloc, SIMPLE_SCHEMA).unwrap();
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
  let script = TwScript {
    graphs: vec![TwGraph {
      nodes: vec![
        (TwGraphNode::LoadParam(0), vec![], None),    // 0
        (TwGraphNode::GetField(0), vec![0], None),    // 1
        (TwGraphNode::GetField(1), vec![1], None),    // 2
        (TwGraphNode::UnwrapOptional, vec![2], None), // 3
        (TwGraphNode::GetField(2), vec![3], None),    // 4
        (TwGraphNode::UnwrapOptional, vec![4], None), // 5
        (TwGraphNode::GetField(3), vec![5], None),    // 6
        (TwGraphNode::UnwrapOptional, vec![6], None), // 7
      ],
      output: Some(7),
      effects: vec![],
      output_type: Some(1),
      param_types: vec![0],
    }],
    entry: 0,
    consts: vec![],
    idents: vec![
      "a_trinary_tree".into(),
      "middle".into(),
      "left".into(),
      "value".into(),
    ],
    types: vec![VmType::Schema, VmType::Primitive(PrimitiveType::String)],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  assert!(
    GlobalTyckContext::new(&vm)
      .unwrap()
      .typeck()
      .unwrap_err()
      .to_string()
      == "type `Primitive(String)` is not covariant from `Primitive(Int64)`"
  );
}

#[test]
fn typeck_set_point_get() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(&alloc, SIMPLE_SCHEMA).unwrap();
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
  let mut expected_result_type = RedBlackTreeMapSync::new_sync();
  expected_result_type.insert_mut(
    "start".to_string(),
    VmType::<String>::Primitive(PrimitiveType::Int64),
  );
  expected_result_type.insert_mut(
    "the_item".to_string(),
    VmType::<String>::Table(VmTableType {
      name: "Item<Duration<int64>>".into(),
    }),
  );
  let script = TwScript {
    graphs: vec![TwGraph {
      nodes: vec![
        (TwGraphNode::LoadParam(0), vec![], None),         // 0
        (TwGraphNode::LoadConst(0), vec![], None),         // 1
        (TwGraphNode::GetField(0), vec![0], None),         // 2
        (TwGraphNode::GetSetElement, vec![1, 2], None),    // 3
        (TwGraphNode::GetField(2), vec![3], None),         // 4
        (TwGraphNode::GetField(3), vec![4], None),         // 5
        (TwGraphNode::CreateMap, vec![], None),            // 6
        (TwGraphNode::InsertIntoMap(4), vec![3, 6], None), // 7
        (TwGraphNode::InsertIntoMap(3), vec![5, 7], None), // 8
      ],
      output: Some(8),
      effects: vec![],
      output_type: Some(1),
      param_types: vec![0],
    }],
    entry: 0,
    consts: vec![VmConst::Primitive(PrimitiveValue::String("test".into()))],
    idents: vec![
      "items".into(),
      "something_else".into(),
      "inner2".into(),
      "start".into(),
      "the_item".into(),
    ],
    types: vec![VmType::Schema, VmType::Map(expected_result_type)],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  GlobalTyckContext::new(&vm).unwrap().typeck().unwrap();
}
