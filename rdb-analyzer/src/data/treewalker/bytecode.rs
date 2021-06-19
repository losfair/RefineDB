use serde::{Deserialize, Serialize};

use super::vm_value::VmConst;

#[derive(Serialize, Deserialize, Debug)]
pub struct TwScript {
  pub graphs: Vec<TwGraph>,
  pub consts: Vec<VmConst>,
  pub idents: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TwGraph {
  /// Topologically sorted nodes.
  ///
  /// (node, in_edges)
  pub nodes: Vec<(TwGraphNode, Vec<u32>)>,

  /// The output value of this graph.
  pub output: Option<u32>,

  /// The effects of this graph.
  pub effects: Vec<u32>,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum TwGraphNode {
  /// T
  ///
  /// Const param: param_index
  LoadParam(u32),

  /// T
  ///
  /// Const param: const_index
  LoadConst(u32),

  /// Map -> Table<T>
  ///
  /// Const param: ident (table_type)
  BuildTable(u32),

  /// Table<T> -> T
  ///
  /// Const param: ident
  GetTableField(u32),

  /// List<T> -> Set<T>
  BuildSet,

  /// Selector -> Set<T> -> T
  ///
  /// Filter the set with the given subgraph.
  ///
  /// Const param: subgraph_index
  GetSetElement(u32),

  /// T -> Set<T> -> ()
  ///
  /// This is an effect node.
  ///
  /// Const param: ident
  InsertIntoSet(u32),

  /// List<T>
  ///
  /// Const param: type
  CreateList(u32),

  /// T -> List<T> -> List<T>
  AppendList,

  /// Map
  CreateMap,

  /// Map -> T
  ///
  /// Const param: ident
  GetMapField(u32),

  /// T -> Map -> Map
  ///
  /// Const param: ident
  InsertIntoMap(u32),

  /// Map -> Map
  ///
  /// Const param: ident
  DeleteFromMap(u32),
}
