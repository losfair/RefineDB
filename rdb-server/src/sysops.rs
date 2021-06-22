use rdb_analyzer::{
  data::treewalker::{
    bytecode::{TwGraph, TwGraphNode, TwScript},
    vm_value::{VmConst, VmConstSetValue, VmType},
  },
  schema::compile::{CompiledSchema, PrimitiveType},
};

pub fn sysop_add_namespace(schema: &CompiledSchema) -> TwScript {
  TwScript {
    graphs: vec![TwGraph {
      nodes: vec![
        (TwGraphNode::LoadParam(0), vec![]),         // 0
        (TwGraphNode::LoadParam(1), vec![]),         // 1 namespace_id
        (TwGraphNode::GetField(0), vec![0]),         // 2 <root>.system
        (TwGraphNode::GetField(1), vec![2]),         // 3 <root>.system.namespaces
        (TwGraphNode::CreateMap, vec![]),            // 4
        (TwGraphNode::InsertIntoMap(2), vec![1, 4]), // 5
        (TwGraphNode::LoadConst(0), vec![]),         // 6
        (TwGraphNode::InsertIntoMap(3), vec![6, 5]), // 7
        (TwGraphNode::BuildTable(4), vec![7]),       // 8
        (TwGraphNode::InsertIntoSet, vec![8, 3]),    // 9
      ],
      output: None,
      effects: vec![],
      output_type: None,
      param_types: vec![0, 1],
    }],
    entry: 0,
    consts: vec![VmConst::Set(VmConstSetValue {
      member_ty: "Deployment<>".into(),
      members: vec![],
    })],
    idents: vec![
      "system".into(),
      "namespaces".into(),
      "id".into(),
      "all_deployments".into(),
      "Namespace<>".into(),
    ],
    types: vec![
      VmType::<String>::from(schema),
      VmType::Primitive(PrimitiveType::String),
    ],
  }
}
