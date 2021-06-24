use std::convert::TryFrom;

use anyhow::Result;
use rdb_analyzer::data::treewalker::{asm::codegen::compile_twscript, bytecode::TwScript};

pub struct SysopCollection<T> {
  pub add_namespace: T,
  pub delete_namespace: T,
}

impl TryFrom<&SysopCollection<&str>> for SysopCollection<TwScript> {
  type Error = anyhow::Error;

  fn try_from(that: &SysopCollection<&str>) -> Result<Self> {
    Ok(Self {
      add_namespace: compile_twscript(that.add_namespace)?,
      delete_namespace: compile_twscript(that.delete_namespace)?,
    })
  }
}

#[allow(dead_code)]
pub static SYSOPS: SysopCollection<&'static str> = SysopCollection {
  add_namespace: r#"
  graph main(root: schema, namespace_id: string): bool {
    ns = root.system.namespaces;
    if is_present $ point_get ns namespace_id {
      r1 = false;
    } else {
      s_insert root.system.namespaces $
        build_table(Namespace) $
        m_insert(id) namespace_id $
        m_insert(all_deployments) empty_set<Deployment> $
        create_map;
      r2 = true;
    }
    return select r1 r2;
  }
  "#,
  delete_namespace: r#"
  graph main(root: schema, namespace_id: string): bool {
    ns = root.system.namespaces;
    if is_present $ point_get ns namespace_id {
      s_delete ns namespace_id;
      r1 = true;
    } else {
      r2 = false;
    }
    return select r1 r2;
  }
  "#,
};
