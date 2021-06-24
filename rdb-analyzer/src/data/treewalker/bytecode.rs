use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};

use super::vm_value::{VmConst, VmType};

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct TwScript {
  pub graphs: Vec<TwGraph>,
  pub entry: u32,
  pub consts: Vec<VmConst>,
  pub idents: Vec<String>,
  pub types: Vec<VmType<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TwGraph {
  /// Name.
  pub name: String,

  /// Topologically sorted nodes.
  ///
  /// (node, in_edges, precondition)
  pub nodes: Vec<(TwGraphNode, Vec<u32>, Option<u32>)>,

  /// The output value of this graph.
  pub output: Option<u32>,

  /// Param types.
  pub param_types: Vec<u32>,

  /// Output type.
  pub output_type: Option<u32>,
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

  /// List<T> -> Set<T>
  BuildSet,

  /// Map
  CreateMap,

  /// (Map | Table<T>) -> T
  ///
  /// Const param: ident
  GetField(u32),

  /// T::PrimaryKeyValue -> Set<T> -> T
  ///
  /// Point-get on a set.
  GetSetElement,

  /// U (subgraph parameter) -> Set<T> -> T
  ///
  /// Filter the set with the given subgraph.
  ///
  /// Const param: subgraph_index
  FilterSet(u32),

  /// T -> Map -> Map
  ///
  /// Const param: ident
  InsertIntoMap(u32),

  /// T -> Table<T> -> ()
  ///
  /// This is an effect node.
  ///
  /// Const param: ident
  InsertIntoTable(u32),

  /// T -> Set<T> -> ()
  ///
  /// This is an effect node.
  InsertIntoSet,

  /// T::PrimaryKeyValue -> Set<T> -> ()
  ///
  /// Point-delete on a set.
  /// This is an effect node.
  ///
  /// Const param: ident
  DeleteFromSet,

  /// Map -> Map
  ///
  /// Const param: ident
  DeleteFromMap(u32),

  /// Table<T> -> ()
  ///
  /// This is an effect node.
  ///
  /// Const param: ident
  DeleteFromTable(u32),

  /// T -> T -> Bool
  Eq,

  /// T -> T -> Bool
  Ne,

  /// Bool -> Bool -> Bool
  And,

  /// Bool -> Bool -> Bool
  Or,

  /// Bool -> Bool
  Not,

  /// Optional<T> -> T
  UnwrapOptional,

  /// Fire if either of its parameters are satisfied.
  ///
  /// T -> T -> T
  Select,

  /// True if this table or set is actually present.
  ///
  /// Always true for fresh values, and true for resident values if its storage key exists.
  ///
  /// T -> Bool
  IsPresent,
}

impl TwGraphNode {
  pub fn is_select(&self) -> bool {
    match self {
      Self::Select => true,
      _ => false,
    }
  }
  pub fn subgraph_references(&self) -> SmallVec<[u32; 1]> {
    match self {
      Self::FilterSet(x) => smallvec![*x],
      _ => smallvec![],
    }
  }
}
