use std::collections::{HashMap, HashSet};

use super::language::RootParser;
use super::{ast, state::State};
use crate::data::treewalker::asm::TwAsmError;
use crate::data::treewalker::bytecode::{TwGraph, TwGraphNode, TwScript};
use crate::data::treewalker::vm_value::{VmConst, VmConstSetValue, VmSetType, VmTableType, VmType};
use crate::data::value::PrimitiveValue;
use crate::schema::compile::PrimitiveType;
use crate::util::first_duplicate;
use anyhow::Result;
use bumpalo::boxed::Box as BumpBox;
use bumpalo::Bump;

pub fn compile_twscript(input: &str) -> Result<TwScript> {
  let bump = Bump::new();
  let root = parse(&bump, input)?;

  let mut builder = Builder {
    bump: &bump,
    script: TwScript::default(),
    ident_pool: HashMap::new(),
    vmtype_pool: HashMap::new(),
    const_pool: HashMap::new(),
    type_aliases: HashMap::new(),
  };
  if let Some(x) = first_duplicate(root.graphs.iter().map(|x| x.name)) {
    return Err(TwAsmError::DuplicateGraph(x.into()).into());
  }

  // Collect type aliases
  if let Some(x) = first_duplicate(root.type_aliases.iter().map(|x| x.name)) {
    return Err(TwAsmError::DuplicateTypeAlias(x.into()).into());
  }
  // XXX: Here we don't allow recursive type aliases - should this be changed?
  for alias in &root.type_aliases {
    let vmtype = builder.generate_vmtype(&alias.ty)?;
    builder.type_aliases.insert(alias.name, vmtype);
  }

  for g in &root.graphs {
    if let Some(x) = first_duplicate(g.params.iter().map(|x| x.0)) {
      return Err(TwAsmError::DuplicateParam(x.into()).into());
    }
    let target = TwGraph {
      name: g.name.to_string(),
      nodes: vec![],
      output: None,
      param_types: g
        .params
        .iter()
        .map(|(_, ty)| {
          ty.as_ref()
            .map(|x| builder.generate_vmtype(x))
            .unwrap_or_else(|| Ok(VmType::Unknown))
            .map(|x| builder.alloc_vmtype(x))
        })
        .collect::<Result<_>>()?,
      output_type: g
        .return_type
        .as_ref()
        .map(|x| builder.generate_vmtype(x))
        .transpose()?
        .map(|x| builder.alloc_vmtype(x)),
    };
    let output;
    {
      let mut ctx = GraphContext {
        names: HashMap::new(),
        builder: &mut builder,
        target,
        condition_stack: vec![],
      };
      for (i, (p, _)) in g.params.iter().enumerate() {
        ctx.push_node((TwGraphNode::LoadParam(i as u32), vec![], None), Some(*p))?;
      }
      for stmt in &g.stmts {
        ctx.generate_stmt(g, stmt)?;
      }
      output = ctx.target;
    }
    builder.script.graphs.push(output);
  }
  builder.emit_pools();
  Ok(builder.script)
}

struct Builder<'a> {
  bump: &'a Bump,
  script: TwScript,
  ident_pool: HashMap<&'a str, u32>,
  vmtype_pool: HashMap<BumpBox<'a, VmType<String>>, u32>,
  const_pool: HashMap<VmConst, u32>,
  type_aliases: HashMap<&'a str, VmType<String>>,
}

struct GraphContext<'a, 'b> {
  names: HashMap<&'a str, u32>,
  builder: &'b mut Builder<'a>,
  target: TwGraph,
  condition_stack: Vec<u32>,
}

impl<'a, 'b> GraphContext<'a, 'b> {
  fn generate_stmt(&mut self, g: &ast::Graph<'a>, stmt: &ast::Stmt<'a>) -> Result<()> {
    match &stmt.kind {
      ast::StmtKind::Return { value } => {
        let node = self.generate_expr(g, None, value)?;
        if self.target.output.is_some() {
          return Err(TwAsmError::DuplicateReturn.into());
        }
        self.target.output = Some(node);
      }
      ast::StmtKind::If {
        precondition,
        if_body,
        else_body,
      } => {
        let precondition = self.generate_expr(g, None, precondition)?;
        let condition_true = self.generate_condition(precondition)?;
        self.condition_stack.push(condition_true);
        for stmt in if_body {
          self.generate_stmt(g, stmt)?;
        }
        self.condition_stack.pop().unwrap();

        if let Some(else_body) = else_body {
          let precondition = self.push_node((TwGraphNode::Not, vec![precondition], None), None)?;
          let condition_false = self.generate_condition(precondition)?;
          self.condition_stack.push(condition_false);
          for stmt in else_body {
            self.generate_stmt(g, stmt)?;
          }
          self.condition_stack.pop().unwrap();
        }
      }
      ast::StmtKind::Node { name, value } => {
        self.generate_expr(g, *name, value)?;
      }
    }
    Ok(())
  }

  fn generate_condition(&mut self, condition: u32) -> Result<u32> {
    if let Some(last) = self.condition_stack.last() {
      let last = *last;
      self.push_node((TwGraphNode::And, vec![condition, last], None), None)
    } else {
      Ok(condition)
    }
  }

  fn generate_expr(
    &mut self,
    g: &ast::Graph<'a>,
    name: Option<&'a str>,
    expr: &ast::Expr<'a>,
  ) -> Result<u32> {
    use ast::ExprKind as K;
    let precondition = self.condition_stack.last().copied();
    let ret = match &expr.kind {
      K::Node(x) => self.lookup_node(*x)?,
      K::And(l, r) => {
        let l = self.generate_expr(g, None, l)?;
        let r = self.generate_expr(g, None, r)?;
        self.push_node((TwGraphNode::And, vec![l, r], precondition), name)?
      }
      K::BuildTable(ty, map) => {
        let ty = self
          .builder
          .alloc_ident_external(&format_type_for_table(ty)?);
        let map = self.generate_expr(g, None, *map)?;
        self.push_node((TwGraphNode::BuildTable(ty), vec![map], precondition), name)?
      }
      K::CreateMap => self.push_node((TwGraphNode::CreateMap, vec![], precondition), name)?,
      K::DeleteFromMap(field, map) => {
        let field = self.builder.alloc_ident(*field);
        let map = self.generate_expr(g, None, *map)?;
        self.push_node(
          (TwGraphNode::DeleteFromMap(field), vec![map], precondition),
          name,
        )?
      }
      K::DeleteFromSet(set, selector) => {
        let set = self.generate_expr(g, None, *set)?;
        let selector = self.generate_expr(g, None, *selector)?;
        self.push_node(
          (
            TwGraphNode::DeleteFromSet,
            vec![selector, set],
            precondition,
          ),
          name,
        )?
      }
      K::DeleteFromTable(field, table) => {
        let field = self.builder.alloc_ident(*field);
        let table = self.generate_expr(g, None, *table)?;
        self.push_node(
          (
            TwGraphNode::DeleteFromTable(field),
            vec![table],
            precondition,
          ),
          name,
        )?
      }
      K::Eq(l, r) => {
        let l = self.generate_expr(g, None, *l)?;
        let r = self.generate_expr(g, None, *r)?;
        self.push_node((TwGraphNode::Eq, vec![l, r], precondition), name)?
      }
      K::GetField(field, table_or_set) => {
        let field = self.builder.alloc_ident(*field);
        let table_or_set = self.generate_expr(g, None, *table_or_set)?;
        self.push_node(
          (
            TwGraphNode::GetField(field),
            vec![table_or_set],
            precondition,
          ),
          name,
        )?
      }
      K::GetSetElement(set, selector) => {
        let set = self.generate_expr(g, None, *set)?;
        let selector = self.generate_expr(g, None, *selector)?;
        self.push_node(
          (
            TwGraphNode::GetSetElement,
            vec![selector, set],
            precondition,
          ),
          name,
        )?
      }
      K::InsertIntoMap(field, v, map) => {
        let field = self.builder.alloc_ident(*field);
        let v = self.generate_expr(g, None, *v)?;
        let map = self.generate_expr(g, None, *map)?;
        self.push_node(
          (
            TwGraphNode::InsertIntoMap(field),
            vec![v, map],
            precondition,
          ),
          name,
        )?
      }
      K::InsertIntoSet(set, v) => {
        let set = self.generate_expr(g, None, *set)?;
        let v = self.generate_expr(g, None, *v)?;
        self.push_node(
          (TwGraphNode::InsertIntoSet, vec![v, set], precondition),
          name,
        )?
      }
      K::InsertIntoTable(field, table, v) => {
        let field = self.builder.alloc_ident(*field);
        let table = self.generate_expr(g, None, *table)?;
        let v = self.generate_expr(g, None, *v)?;
        self.push_node(
          (
            TwGraphNode::InsertIntoTable(field),
            vec![v, table],
            precondition,
          ),
          name,
        )?
      }
      K::LoadConst(x) => {
        let vmconst = self.builder.literal_to_vmconst(x)?;
        let x = self.builder.alloc_const(vmconst);
        self.push_node((TwGraphNode::LoadConst(x), vec![], precondition), name)?
      }
      K::Select(l, r) => {
        let l = self.generate_expr(g, None, *l)?;
        let r = self.generate_expr(g, None, *r)?;
        self.push_node((TwGraphNode::Select, vec![l, r], precondition), name)?
      }
      K::Ne(l, r) => {
        let l = self.generate_expr(g, None, *l)?;
        let r = self.generate_expr(g, None, *r)?;
        self.push_node((TwGraphNode::Ne, vec![l, r], precondition), name)?
      }
      K::Or(l, r) => {
        let l = self.generate_expr(g, None, *l)?;
        let r = self.generate_expr(g, None, *r)?;
        self.push_node((TwGraphNode::Or, vec![l, r], precondition), name)?
      }
      K::Not(x) => {
        let x = self.generate_expr(g, None, *x)?;
        self.push_node((TwGraphNode::Not, vec![x], precondition), name)?
      }
      K::IsPresent(x) => {
        let x = self.generate_expr(g, None, *x)?;
        self.push_node((TwGraphNode::IsPresent, vec![x], precondition), name)?
      }
      K::IsNull(x) => {
        let x = self.generate_expr(g, None, *x)?;
        self.push_node((TwGraphNode::IsNull, vec![x], precondition), name)?
      }
      K::OrElse(l, r) => {
        let l = self.generate_expr(g, None, *l)?;
        let comparator = self.push_node((TwGraphNode::IsNull, vec![l], None), None)?;
        let not_comparator = self.push_node((TwGraphNode::Not, vec![comparator], None), None)?;

        // If `l` is null...
        let condition = self.generate_condition(comparator)?;
        self.condition_stack.push(condition);
        let on_null = self.generate_expr(g, None, *r)?;
        self.condition_stack.pop().unwrap();

        // Otherwise, use `l`...
        let condition = self.generate_condition(not_comparator)?;
        let on_notnull = self.push_node((TwGraphNode::Nop, vec![l], Some(condition)), None)?;

        self.push_node(
          (TwGraphNode::Select, vec![on_null, on_notnull], precondition),
          name,
        )?
      }
    };
    Ok(ret)
  }

  fn push_node(
    &mut self,
    node: (TwGraphNode, Vec<u32>, Option<u32>),
    name: Option<&'a str>,
  ) -> Result<u32> {
    let index = self.target.nodes.len() as u32;
    self.target.nodes.push(node);
    if let Some(name) = name {
      if self.names.contains_key(name) {
        return Err(TwAsmError::DuplicateNodeName(name.into()).into());
      }
      self.names.insert(name, index);
    }
    Ok(index)
  }

  fn lookup_node(&self, name: &str) -> Result<u32> {
    match self.names.get(name) {
      Some(x) => Ok(*x),
      None => Err(TwAsmError::NodeNotFound(name.to_string()).into()),
    }
  }
}

impl<'a> Builder<'a> {
  fn alloc_vmtype(&mut self, ty: VmType<String>) -> u32 {
    if let Some(x) = self.vmtype_pool.get(&ty) {
      *x
    } else {
      let id = self.vmtype_pool.len() as u32;
      self.vmtype_pool.insert(BumpBox::new_in(ty, self.bump), id);
      id
    }
  }

  fn alloc_ident(&mut self, id: &'a str) -> u32 {
    if let Some(x) = self.ident_pool.get(&id) {
      *x
    } else {
      let index = self.ident_pool.len() as u32;
      self.ident_pool.insert(id, index);
      index
    }
  }

  fn alloc_ident_external(&mut self, id: &str) -> u32 {
    if let Some(x) = self.ident_pool.get(id) {
      *x
    } else {
      let index = self.ident_pool.len() as u32;
      self.ident_pool.insert(self.bump.alloc_str(id), index);
      index
    }
  }

  fn alloc_const(&mut self, x: VmConst) -> u32 {
    if let Some(x) = self.const_pool.get(&x) {
      *x
    } else {
      let index = self.const_pool.len() as u32;
      self.const_pool.insert(x, index);
      index
    }
  }

  fn emit_pools(&mut self) {
    let mut const_pool = std::mem::replace(&mut self.const_pool, HashMap::new())
      .into_iter()
      .collect::<Vec<_>>();
    const_pool.sort_by(|a, b| a.1.cmp(&b.1));

    let mut ident_pool = std::mem::replace(&mut self.ident_pool, HashMap::new())
      .into_iter()
      .collect::<Vec<_>>();
    ident_pool.sort_by(|a, b| a.1.cmp(&b.1));

    let mut vmtype_pool = std::mem::replace(&mut self.vmtype_pool, HashMap::new())
      .into_iter()
      .collect::<Vec<_>>();
    vmtype_pool.sort_by(|a, b| a.1.cmp(&b.1));

    self.script.consts = const_pool.into_iter().map(|x| x.0).collect();
    self.script.idents = ident_pool.into_iter().map(|x| x.0.to_string()).collect();
    self.script.types = vmtype_pool.into_iter().map(|x| x.0.clone()).collect();
  }

  fn generate_vmtype(&self, ty: &ast::Type) -> Result<VmType<String>> {
    Ok(match ty {
      ast::Type::Primitive(x) => VmType::Primitive(*x),
      ast::Type::Table { name, .. } => {
        if let Some(x) = self.type_aliases.get(name) {
          x.clone()
        } else {
          VmType::Table(VmTableType {
            name: format_type_for_table(ty)?,
          })
        }
      }
      ast::Type::Set(x) => VmType::Set(VmSetType {
        ty: Box::new(self.generate_vmtype(*x)?),
      }),
      ast::Type::Map(x) => VmType::Map(
        x.iter()
          .map(|(k, v)| self.generate_vmtype(v).map(|x| (k.to_string(), x)))
          .collect::<Result<_>>()?,
      ),
      ast::Type::Bool => VmType::Bool,
      ast::Type::Schema => VmType::Schema,
    })
  }

  fn literal_to_vmconst(&self, x: &ast::Literal) -> Result<VmConst> {
    Ok(match x {
      ast::Literal::Null(ty) => VmConst::Null(self.generate_vmtype(ty)?),
      ast::Literal::Bool(x) => VmConst::Bool(*x),
      ast::Literal::Integer(x) => VmConst::Primitive(PrimitiveValue::Int64(*x)),
      ast::Literal::HexBytes(x) => VmConst::Primitive(PrimitiveValue::Bytes(x.to_vec())),
      ast::Literal::String(x) => VmConst::Primitive(PrimitiveValue::String(x.to_string())),
      ast::Literal::EmptySet(member_ty) => VmConst::Set(VmConstSetValue {
        member_ty: format_type_for_table(member_ty)?,
        members: vec![],
      }),
    })
  }
}

fn parse<'a, 'b: 'a>(alloc: &'a Bump, input: &'b str) -> Result<ast::Root<'a>> {
  // Clone this to satisfy lifetimes
  let mut st: State<'a> = State {
    alloc,
    string_table: HashSet::new(),
  };
  let parser = RootParser::new();
  let root = parser
    .parse(&mut st, input)
    .map_err(|x| x.map_token(|x| x.to_string()))?;
  Ok(root)
}

fn format_type_for_table(ty: &ast::Type) -> Result<String> {
  Ok(match ty {
    ast::Type::Primitive(x) => match x {
      PrimitiveType::String => "string".into(),
      PrimitiveType::Bytes => "bytes".into(),
      PrimitiveType::Int64 => "int64".into(),
      PrimitiveType::Double => "double".into(),
    },
    ast::Type::Set(x) => format!("set<{}>", format_type_for_table(x)?),
    ast::Type::Table { name, params } => format!(
      "{}<{}>",
      name,
      params
        .iter()
        .map(|x| format_type_for_table(x))
        .collect::<Result<Vec<_>>>()?
        .join(", "),
    ),
    _ => return Err(TwAsmError::TypeUnsupportedInTable.into()),
  })
}
