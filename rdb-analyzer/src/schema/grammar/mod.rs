pub mod ast;
pub mod error;

use std::collections::HashSet;

use anyhow::Result;
use bumpalo::Bump;
use lalrpop_util::lalrpop_mod;

lalrpop_mod!(pub schema, "/schema/grammar/parser.rs");

use schema::SchemaSourceParser;

pub struct State<'a> {
  alloc: &'a Bump,
  string_table: HashSet<&'a str>,
}

impl<'a> State<'a> {
  pub fn resolve_str(&mut self, s: &str) -> &'a str {
    match self.string_table.get(s) {
      Some(x) => x,
      None => {
        let s = self.alloc.alloc_str(s);
        self.string_table.insert(s);
        s
      }
    }
  }
}

pub fn parse<'a>(alloc: &'a Bump, input: &str) -> Result<ast::Schema<'a>> {
  // Clone this to satisfy lifetimes
  let input = alloc.alloc_str(input);
  let mut st: State<'a> = State {
    alloc,
    string_table: HashSet::new(),
  };
  let parser = SchemaSourceParser::new();
  let schema = parser
    .parse(&mut st, input)
    .map_err(|x| x.map_token(|x| x.to_string()))?;
  Ok(schema)
}
