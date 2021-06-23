use std::{collections::HashSet, hash::Hash};

pub fn first_duplicate<T>(iter: T) -> Option<T::Item>
where
  T: IntoIterator,
  T::Item: Eq + Hash,
{
  let mut uniq = HashSet::new();
  for x in iter {
    if uniq.contains(&x) {
      return Some(x);
    }
    uniq.insert(x);
  }
  None
}
