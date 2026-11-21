//! The key space.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;

/// What a key holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Str(Bytes),
    List(VecDeque<Bytes>),
}

/// The operation was against a key holding the wrong kind of value.
#[derive(Debug, PartialEq, Eq)]
pub struct WrongType;

/// Which end of a list to push onto.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum End {
    Left,
    Right,
}

/// The whole key space. Cheap to clone; clones share the same data.
///
/// *Why one big lock?* Every operation takes a single `std::sync::Mutex`, does
/// a small amount of work, and lets go, never holding it across an `.await`.
/// That makes every operation atomic, so the server is linearizable for
/// free, and the lock is held so briefly that contention stays low for our
/// purposes. (The tokio docs recommend exactly this.) If you want more
/// throughput, sharding the map by key hash is the usual next step, but then
/// operations over many keys need more care.
#[derive(Clone, Default)]
pub struct Db {
    data: Arc<Mutex<HashMap<Bytes, Value>>>,
}

impl Db {
    pub fn new() -> Db {
        Db::default()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<Bytes, Value>> {
        // A panic while holding the lock can't leave the map half-updated in
        // a way that matters here, so we keep going rather than poisoning
        // every later request.
        self.data.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>, WrongType> {
        match self.lock().get(key) {
            None => Ok(None),
            Some(Value::Str(s)) => Ok(Some(s.clone())),
            Some(Value::List(_)) => Err(WrongType),
        }
    }

    /// Set `key` to a string, replacing whatever it held before.
    pub fn set(&self, key: Bytes, value: Bytes) {
        self.lock().insert(key, Value::Str(value));
    }

    /// How many of `keys` exist. A key listed twice counts twice.
    pub fn exists(&self, keys: &[Bytes]) -> i64 {
        let data = self.lock();
        keys.iter().filter(|k| data.contains_key(&k[..])).count() as i64
    }

    /// Push `values` onto the list at `key`, one at a time, creating it if
    /// needed. Returns the list's new length.
    pub fn push(&self, key: Bytes, values: Vec<Bytes>, end: End) -> Result<i64, WrongType> {
        let mut data = self.lock();
        let list = match data.entry(key) {
            Entry::Vacant(e) => match e.insert(Value::List(VecDeque::new())) {
                Value::List(list) => list,
                Value::Str(_) => unreachable!(),
            },
            Entry::Occupied(e) => match e.into_mut() {
                Value::List(list) => list,
                Value::Str(_) => return Err(WrongType),
            },
        };
        for v in values {
            match end {
                End::Left => list.push_front(v),
                End::Right => list.push_back(v),
            }
        }
        Ok(list.len() as i64)
    }

    /// The elements of the list at `key` from `start` to `stop`, inclusive.
    /// Negative indices count from the end, as in `LRANGE`.
    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Bytes>, WrongType> {
        let data = self.lock();
        let list = match data.get(key) {
            None => return Ok(vec![]),
            Some(Value::List(list)) => list,
            Some(Value::Str(_)) => return Err(WrongType),
        };
        let len = list.len() as i64;
        let start = if start < 0 { (start + len).max(0) } else { start };
        let stop = if stop < 0 { stop + len } else { stop.min(len - 1) };
        if start > stop || start >= len {
            return Ok(vec![]);
        }
        Ok(list.range(start as usize..=stop as usize).cloned().collect())
    }

    pub fn llen(&self, key: &[u8]) -> Result<i64, WrongType> {
        match self.lock().get(key) {
            None => Ok(0),
            Some(Value::List(list)) => Ok(list.len() as i64),
            Some(Value::Str(_)) => Err(WrongType),
        }
    }

    /// Run `f` with exclusive access to the whole key space.
    ///
    /// This is the escape hatch for anything that has to happen atomically
    /// across many keys. Keep `f` quick: every other request waits for it.
    pub fn with_data<R>(&self, f: impl FnOnce(&mut HashMap<Bytes, Value>) -> R) -> R {
        f(&mut self.lock())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn list(db: &Db, key: &str, start: i64, stop: i64) -> Vec<String> {
        db.lrange(key.as_bytes(), start, stop)
            .unwrap()
            .into_iter()
            .map(|v| String::from_utf8(v.to_vec()).unwrap())
            .collect()
    }

    #[test]
    fn strings() {
        let db = Db::new();
        assert_eq!(db.get(b"k"), Ok(None));
        db.set(b("k"), b("v1"));
        db.set(b("k"), b("v2"));
        assert_eq!(db.get(b"k"), Ok(Some(b("v2"))));
        assert_eq!(db.exists(&[b("k"), b("k"), b("nope")]), 2);
    }

    #[test]
    fn push_order() {
        let db = Db::new();
        assert_eq!(db.push(b("l"), vec![b("a"), b("b")], End::Right), Ok(2));
        assert_eq!(db.push(b("l"), vec![b("x"), b("y")], End::Left), Ok(4));
        assert_eq!(list(&db, "l", 0, -1), ["y", "x", "a", "b"]);
    }

    #[test]
    fn lrange_indices() {
        let db = Db::new();
        let items: Vec<Bytes> = ["0", "1", "2", "3", "4"].map(b).into();
        db.push(b("l"), items, End::Right).unwrap();
        assert_eq!(list(&db, "l", 1, 3), ["1", "2", "3"]);
        assert_eq!(list(&db, "l", -2, -1), ["3", "4"]);
        assert_eq!(list(&db, "l", -100, 1), ["0", "1"]);
        assert_eq!(list(&db, "l", 3, 100), ["3", "4"]);
        assert!(list(&db, "l", 3, 1).is_empty());
        assert!(list(&db, "l", 5, 10).is_empty());
        assert!(list(&db, "l", 0, -6).is_empty());
        assert!(list(&db, "missing", 0, -1).is_empty());
    }

    #[test]
    fn wrong_type() {
        let db = Db::new();
        db.set(b("s"), b("v"));
        db.push(b("l"), vec![b("a")], End::Right).unwrap();
        assert_eq!(db.get(b"l"), Err(WrongType));
        assert_eq!(db.push(b("s"), vec![b("a")], End::Left), Err(WrongType));
        assert_eq!(db.lrange(b"s", 0, -1), Err(WrongType));
        assert_eq!(db.llen(b"s"), Err(WrongType));
        // SET overwrites any type.
        db.set(b("l"), b("now a string"));
        assert_eq!(db.get(b"l"), Ok(Some(b("now a string"))));
    }
}
