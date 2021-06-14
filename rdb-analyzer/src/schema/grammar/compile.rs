use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use anyhow::Result;
use indexmap::IndexMap;
use thiserror::Error;

use crate::schema::grammar::ast::SchemaItem;

use super::ast::{self, TypeExpr};

#[derive(Error, Debug)]
pub enum SchemaCompileError {
  #[error("recursive types")]
  RecursiveTypes,

  #[error("missing type: {0}")]
  MissingType(String),

  #[error("expecting {expected_args} arguments on type {ty}, got {got_args}")]
  ArgCountMismatch {
    expected_args: usize,
    ty: String,
    got_args: usize,
  },

  #[error("cannot specialize a type parameter `{0}`.")]
  CannotSpecializeTypeParameter(String),
}

pub struct CompiledSchema {
  pub types: IndexMap<Arc<str>, SpecializedType>,
  pub exports: IndexMap<Arc<str>, Arc<str>>,
}

impl Display for CompiledSchema {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    for (_, ty) in &self.types {
      write!(f, "{}\n", ty)?;
    }
    for (k, v) in &self.exports {
      write!(f, "export {} {};\n", v, k)?;
    }
    Ok(())
  }
}

pub fn compile<'a>(input: &ast::Schema<'a>) -> Result<CompiledSchema> {
  log::debug!("resolving types");
  let mut resolution_ctx = TypeResolutionContext::new(input);
  let mut result = CompiledSchema {
    types: IndexMap::new(),
    exports: IndexMap::new(),
  };

  for item in &input.items {
    match item {
      SchemaItem::Export(x) => {
        let ty = resolution_ctx.resolve_type_expr(&HashMap::new(), &x.ty)?;
        result.exports.insert(Arc::from(x.table_name.0), ty);
      }
      _ => {}
    }
  }
  result.types = resolution_ctx.resolved.clone();
  Ok(result)
}

#[derive(Clone)]
pub struct SpecializedType {
  pub name: Arc<str>,
  pub fields: IndexMap<Arc<str>, Arc<str>>,
}

impl Display for SpecializedType {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "type {} {{\n", self.name)?;
    for (k, v) in &self.fields {
      write!(f, "  {}: {},\n", k, v)?;
    }
    write!(f, "}}\n")?;
    Ok(())
  }
}

struct TypeResolutionContext<'a> {
  unresolved: HashMap<&'a str, &'a ast::TypeItem<'a>>,
  resolved: IndexMap<Arc<str>, SpecializedType>,
}

impl<'a> TypeResolutionContext<'a> {
  fn new(schema: &ast::Schema<'a>) -> Self {
    let types: HashMap<&'a str, &'a ast::TypeItem<'a>> = schema
      .items
      .iter()
      .filter_map(|x| match x {
        ast::SchemaItem::Type(x) => Some((x.name.0, *x)),
        _ => None,
      })
      .collect();
    Self {
      unresolved: types,
      resolved: IndexMap::new(),
    }
  }

  fn resolve_type_expr(
    &mut self,
    local_context: &HashMap<&'a str, &Arc<str>>,
    e: &TypeExpr<'a>,
  ) -> Result<Arc<str>> {
    let (id, args) = match e {
      TypeExpr::Unit(x) => (x, &[] as _),
      TypeExpr::Specialize(x, args) => (x, args.as_slice()),
    };

    // If this type is in its local context (type parameters of the type), return it.
    if let Some(&x) = local_context.get(id.0) {
      if args.len() != 0 {
        return Err(SchemaCompileError::CannotSpecializeTypeParameter(id.0.to_string()).into());
      }
      return Ok(x.clone());
    }

    let ty = self
      .unresolved
      .get(id.0)
      .copied()
      .ok_or_else(|| SchemaCompileError::MissingType(id.0.to_string()))?;
    if ty.generics.len() != args.len() {
      return Err(
        SchemaCompileError::ArgCountMismatch {
          expected_args: ty.generics.len(),
          ty: id.0.to_string(),
          got_args: args.len(),
        }
        .into(),
      );
    }

    let args = args
      .iter()
      .map(|x| self.resolve_type_expr(local_context, x))
      .collect::<Result<Vec<_>>>()?;

    let repr = Arc::from(format!(
      "{}<{}>",
      id.0,
      args
        .iter()
        .map(|x| &**self.resolved.get_key_value(x).unwrap().0)
        .collect::<Vec<_>>()
        .join(", "),
    ));

    // Now we have the type itself, let's look at the fields.
    // If the type is already resolved, use it.
    if self.resolved.contains_key(&repr) {
      return Ok(repr);
    }

    // Construct a new local context: specialized types of the type parameters.
    let local_context: HashMap<&'a str, &Arc<str>> =
      ty.generics.iter().map(|x| x.0).zip(args.iter()).collect();

    // First insert with empty fields; fill the actual types in later.
    // This allows us to have recursive types.
    self.resolved.insert_full(
      repr.clone(),
      SpecializedType {
        name: repr.clone(),
        fields: IndexMap::new(),
      },
    );

    // Then, recursively resolve the types of fields.
    let fields = ty
      .fields
      .iter()
      .map(|x| {
        self
          .resolve_type_expr(&local_context, &x.value)
          .map(|y| (Arc::from(x.name.0), y))
      })
      .collect::<Result<IndexMap<_, _>>>()?;
    self.resolved.get_mut(&repr).unwrap().fields = fields;

    Ok(repr)
  }
}
