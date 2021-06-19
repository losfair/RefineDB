use bumpalo::Bump;
use rpds::RedBlackTreeMapSync;

use crate::{
  data::treewalker::{
    bytecode::{TwGraph, TwGraphNode},
    typeck::typeck_graph,
    vm::TwVm,
    vm_value::VmType,
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

fn root_type<'a>() -> VmType<String> {
  let mut m = RedBlackTreeMapSync::new_sync();
  m.insert_mut(
    "a_trinary_tree".to_string(),
    VmType::Table(VmTableType {
      name: "TrinaryTree<int64>".to_string(),
    }),
  );
  VmType::Map(m)
}

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
        (TwGraphNode::LoadParam(0), vec![]),      // 0
        (TwGraphNode::GetMapField(0), vec![0]),   // 1
        (TwGraphNode::GetTableField(1), vec![1]), // 2
        (TwGraphNode::UnwrapOptional, vec![2]),   // 3
        (TwGraphNode::GetTableField(2), vec![3]), // 4
        (TwGraphNode::UnwrapOptional, vec![4]),   // 5
        (TwGraphNode::GetTableField(3), vec![5]), // 6
        (TwGraphNode::UnwrapOptional, vec![6]),   // 7
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
    types: vec![root_type(), VmType::Primitive(PrimitiveType::Int64)],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  typeck_graph(&vm, &script.graphs[0]).unwrap();
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
        (TwGraphNode::LoadParam(0), vec![]),      // 0
        (TwGraphNode::GetMapField(0), vec![0]),   // 1
        (TwGraphNode::GetTableField(1), vec![1]), // 2
        (TwGraphNode::UnwrapOptional, vec![2]),   // 3
        (TwGraphNode::GetTableField(2), vec![3]), // 4
        (TwGraphNode::UnwrapOptional, vec![4]),   // 5
        (TwGraphNode::GetTableField(3), vec![5]), // 6
        (TwGraphNode::UnwrapOptional, vec![6]),   // 7
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
    types: vec![root_type(), VmType::Primitive(PrimitiveType::Int64)],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  assert!(
    typeck_graph(&vm, &script.graphs[0])
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
        (TwGraphNode::LoadParam(0), vec![]),      // 0
        (TwGraphNode::GetMapField(0), vec![0]),   // 1
        (TwGraphNode::GetTableField(1), vec![1]), // 2
        (TwGraphNode::UnwrapOptional, vec![2]),   // 3
        (TwGraphNode::GetTableField(2), vec![3]), // 4
        (TwGraphNode::UnwrapOptional, vec![4]),   // 5
        (TwGraphNode::GetTableField(3), vec![5]), // 6
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
    types: vec![root_type(), VmType::Primitive(PrimitiveType::Int64)],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  assert!(typeck_graph(&vm, &script.graphs[0])
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
        (TwGraphNode::LoadParam(0), vec![]),      // 0
        (TwGraphNode::GetMapField(0), vec![0]),   // 1
        (TwGraphNode::GetTableField(1), vec![1]), // 2
        (TwGraphNode::UnwrapOptional, vec![2]),   // 3
        (TwGraphNode::GetTableField(2), vec![3]), // 4
        (TwGraphNode::UnwrapOptional, vec![4]),   // 5
        (TwGraphNode::GetTableField(3), vec![5]), // 6
        (TwGraphNode::UnwrapOptional, vec![6]),   // 7
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
    types: vec![root_type(), VmType::Primitive(PrimitiveType::String)],
  };
  let vm = TwVm::new(&schema, &plan, &script).unwrap();
  assert!(
    typeck_graph(&vm, &script.graphs[0])
      .unwrap_err()
      .to_string()
      == "type `Primitive(String)` is not covariant from `Primitive(Int64)`"
  );
}
