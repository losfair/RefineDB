use std::sync::Arc;

use bumpalo::Bump;
use console::Style;
use rdb_analyzer::{
  data::kv::KeyValueStore,
  schema::{compile::compile, grammar::parse},
  storage_plan::{planner::generate_plan_for_schema, StoragePlan},
};
use sha2::{Digest, Sha256};
use similar::{ChangeTag, TextDiff};

use crate::exec_core::{ExecContext, SchemaContext};

pub struct SystemSchema {
  pub exec_ctx: ExecContext,
}

pub const SCHEMA: &str = include_str!("./system_schema.rschema");
pub const SYS_RQL: &str = include_str!("./sys.rql");

impl SystemSchema {
  pub async fn new(migration_hash: Option<String>, meta_store: &dyn KeyValueStore) -> Self {
    let schema = compile(&parse(&Bump::new(), SCHEMA).unwrap()).unwrap();
    let txn = meta_store.begin_transaction().await.unwrap();
    let old_schema_text = txn
      .get(b"schema")
      .await
      .unwrap()
      .map(|x| String::from_utf8(x))
      .transpose()
      .unwrap();
    let old_plan = txn
      .get(b"plan")
      .await
      .unwrap()
      .map(|x| StoragePlan::deserialize_compressed(&x))
      .transpose()
      .unwrap();

    let plan = if let Some(old_schema_text) = old_schema_text {
      let old_schema = compile(&parse(&Bump::new(), &old_schema_text).unwrap()).unwrap();
      let old_plan = old_plan.expect("old plan not found");
      let new_plan = generate_plan_for_schema(&old_plan, &old_schema, &schema).unwrap();

      let old_plan_serialized = rmp_serde::to_vec_named(&old_plan).unwrap();
      let new_plan_serialized = rmp_serde::to_vec_named(&new_plan).unwrap();

      if old_schema_text.as_str() != SCHEMA || old_plan_serialized != new_plan_serialized {
        // Migration required
        let mut hasher = Sha256::new();

        // XXX: Plan may contain randomly generated data and we only know that the schema doesn't change across restarts
        hasher.update(SCHEMA.as_bytes());
        let hash = hex::encode(&hasher.finalize()[..]);
        if migration_hash != Some(hash.clone()) {
          print_diff(&old_plan, &new_plan);
          log::error!("Schema change detected. Please check the storage plan diff and rerun the server with `--migration-hash={}`.", hash);
          std::process::abort();
        }
        log::warn!("Applying schema migration.");
        txn.put(b"schema", SCHEMA.as_bytes()).await.unwrap();
        txn
          .put(b"plan", &new_plan.serialize_compressed().unwrap())
          .await
          .unwrap();
        txn.commit().await.unwrap();
      } else {
        log::info!("Schema unchanged.");
      }
      new_plan
    } else {
      let new_plan =
        generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();
      log::warn!("Creating system schema.");
      txn.put(b"schema", SCHEMA.as_bytes()).await.unwrap();
      txn
        .put(b"plan", &new_plan.serialize_compressed().unwrap())
        .await
        .unwrap();
      txn.commit().await.unwrap();
      new_plan
    };

    let exec_ctx = ExecContext::load(Arc::new(SchemaContext { schema, plan }), SYS_RQL).unwrap();

    Self { exec_ctx }
  }
}

fn print_diff(plan1: &StoragePlan, plan2: &StoragePlan) {
  struct Line(Option<usize>);

  impl std::fmt::Display for Line {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
      match self.0 {
        None => write!(f, "    "),
        Some(idx) => write!(f, "{:<4}", idx + 1),
      }
    }
  }

  let plan1 = serde_yaml::to_string(&StoragePlan::<String>::from(plan1)).unwrap();
  let plan2 = serde_yaml::to_string(&StoragePlan::<String>::from(plan2)).unwrap();
  let diff = TextDiff::from_lines(&plan1, &plan2);
  for (idx, group) in diff.grouped_ops(3).iter().enumerate() {
    if idx > 0 {
      println!("{:-^1$}", "-", 80);
    }
    for op in group {
      for change in diff.iter_inline_changes(op) {
        let (sign, s) = match change.tag() {
          ChangeTag::Delete => ("-", Style::new().red()),
          ChangeTag::Insert => ("+", Style::new().green()),
          ChangeTag::Equal => (" ", Style::new().dim()),
        };
        print!(
          "{}{} |{}",
          console::style(Line(change.old_index())).dim(),
          console::style(Line(change.new_index())).dim(),
          s.apply_to(sign).bold(),
        );
        for (emphasized, value) in change.iter_strings_lossy() {
          if emphasized {
            print!("{}", s.apply_to(value).underlined().on_black());
          } else {
            print!("{}", s.apply_to(value));
          }
        }
        if change.missing_newline() {
          println!();
        }
      }
    }
  }
}
