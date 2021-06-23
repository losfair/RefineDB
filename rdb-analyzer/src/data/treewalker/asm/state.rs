use std::collections::HashSet;

use bumpalo::Bump;

pub struct State<'a> {
  pub alloc: &'a Bump,
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
