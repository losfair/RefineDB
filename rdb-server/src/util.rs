use std::time::{SystemTime, UNIX_EPOCH};

pub fn current_millis() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64
}
