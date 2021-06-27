use console::Style;
use rdb_analyzer::storage_plan::StoragePlan;
use similar::{ChangeTag, TextDiff};

pub fn print_diff(plan1: &StoragePlan, plan2: &StoragePlan) -> (usize, usize) {
  struct Line(Option<usize>);

  impl std::fmt::Display for Line {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
      match self.0 {
        None => write!(f, "    "),
        Some(idx) => write!(f, "{:<4}", idx + 1),
      }
    }
  }

  let mut num_insert = 0usize;
  let mut num_delete = 0usize;

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
          ChangeTag::Delete => {
            num_delete += 1;
            ("-", Style::new().red())
          }
          ChangeTag::Insert => {
            num_insert += 1;
            ("+", Style::new().green())
          }
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

  (num_insert, num_delete)
}
