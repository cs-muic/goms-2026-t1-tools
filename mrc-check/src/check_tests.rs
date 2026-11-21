//! Checker tests on hand-written histories. Times are in microseconds.

use crate::check::{check, Kind};
use crate::history::{Cmd, History, Op, Outcome};

#[derive(Default)]
struct H {
    h: History,
}

impl H {
    fn op(mut self, key: &str, cmd: Cmd, value: Option<&str>, inv: u64, resp: u64, outcome: Outcome) -> Self {
        let id = self.h.ops.len() as u64 + 1;
        self.h.ops.push(Op {
            id,
            client: id as u32,
            router: "r1".into(),
            key: key.into(),
            cmd,
            value: value.map(String::from),
            inv,
            resp,
            outcome,
            final_read: false,
        });
        self
    }
    fn set(self, k: &str, v: &str, inv: u64, resp: u64) -> Self {
        self.op(k, Cmd::Set, Some(v), inv, resp, Outcome::Ok)
    }
    fn set_unknown(self, k: &str, v: &str, inv: u64, resp: u64) -> Self {
        self.op(k, Cmd::Set, Some(v), inv, resp, Outcome::Unknown { msg: "timeout".into() })
    }
    fn set_err(self, k: &str, v: &str, inv: u64, resp: u64) -> Self {
        self.op(k, Cmd::Set, Some(v), inv, resp, Outcome::Error { msg: "ERR moved".into() })
    }
    fn get(self, k: &str, v: Option<&str>, inv: u64, resp: u64) -> Self {
        self.op(k, Cmd::Get, None, inv, resp, Outcome::Value { value: v.map(String::from) })
    }
    fn rpush(self, k: &str, v: &str, len: i64, inv: u64, resp: u64) -> Self {
        self.op(k, Cmd::Rpush, Some(v), inv, resp, Outcome::Int { n: len })
    }
    fn rpush_unknown(self, k: &str, v: &str, inv: u64, resp: u64) -> Self {
        self.op(k, Cmd::Rpush, Some(v), inv, resp, Outcome::Unknown { msg: "timeout".into() })
    }
    fn lrange(self, k: &str, items: &[&str], inv: u64, resp: u64) -> Self {
        self.op(k, Cmd::Lrange, None, inv, resp, Outcome::List { items: items.iter().map(|s| s.to_string()).collect() })
    }
    fn llen(self, k: &str, n: i64, inv: u64, resp: u64) -> Self {
        self.op(k, Cmd::Llen, None, inv, resp, Outcome::Int { n })
    }
    /// Marks the most recent op as a final read.
    fn fin(mut self) -> Self {
        self.h.ops.last_mut().unwrap().final_read = true;
        self
    }
    fn kinds(&self) -> Vec<Kind> {
        let mut k: Vec<Kind> = check(&self.h).into_iter().map(|v| v.kind).collect();
        k.sort();
        k.dedup();
        k
    }
}

fn assert_ok(h: H) {
    let v = check(&h.h);
    assert!(v.is_empty(), "expected no violations, got {v:#?}");
}

fn assert_has(h: H, kind: Kind) {
    let k = h.kinds();
    assert!(k.contains(&kind), "expected {kind:?}, got {k:?}");
}

// ---------- strings: good histories ----------

#[test]
fn sequential_register_is_ok() {
    assert_ok(
        H::default()
            .get("k", None, 0, 5)
            .set("k", "a", 10, 20)
            .get("k", Some("a"), 30, 40)
            .set("k", "b", 50, 60)
            .get("k", Some("b"), 70, 80)
            .get("k", Some("b"), 100, 110)
            .fin(),
    );
}

#[test]
fn concurrent_writes_either_order_is_ok() {
    // b may linearize before a, a may still win at the end.
    assert_ok(
        H::default()
            .set("k", "a", 0, 100)
            .set("k", "b", 10, 110)
            .get("k", Some("b"), 20, 30)
            .get("k", Some("a"), 120, 130)
            .fin(),
    );
}

#[test]
fn read_concurrent_with_write_may_see_old_or_new() {
    assert_ok(
        H::default()
            .set("k", "a", 0, 10)
            .set("k", "b", 20, 60)
            .get("k", Some("a"), 30, 40)
            .get("k", Some("b"), 35, 45),
    );
    // ...but once a read has seen b, a later read may not go back to a.
    assert_has(
        H::default()
            .set("k", "a", 0, 10)
            .set("k", "b", 20, 60)
            .get("k", Some("b"), 35, 45)
            .get("k", Some("a"), 50, 55),
        Kind::StaleRead,
    );
}

#[test]
fn indeterminate_writes_may_land_late_or_never() {
    assert_ok(
        H::default()
            .set_unknown("k", "x", 0, 50)
            .set("k", "y", 60, 70)
            .get("k", Some("x"), 80, 90) // x landed after y
            .set_err("k", "z", 100, 110) // z never landed
            .get("k", Some("x"), 120, 130)
            .fin(),
    );
}

#[test]
fn nil_read_before_first_write_completes_is_ok() {
    assert_ok(H::default().set("k", "a", 10, 20).get("k", None, 0, 15));
}

// ---------- strings: bad histories ----------

#[test]
fn overwritten_value_read_later_is_stale() {
    assert_has(H::default().set("k", "a", 0, 10).set("k", "b", 20, 30).get("k", Some("a"), 40, 50), Kind::StaleRead);
}

#[test]
fn nil_after_acknowledged_write_is_stale() {
    assert_has(H::default().set("k", "a", 0, 10).get("k", None, 20, 30), Kind::StaleRead);
}

#[test]
fn final_read_missing_last_write_is_lost_write() {
    assert_has(
        H::default().set("k", "a", 0, 10).set("k", "b", 20, 30).get("k", Some("a"), 100, 110).fin(),
        Kind::LostWrite,
    );
    assert_has(H::default().set("k", "a", 0, 10).get("k", None, 100, 110).fin(), Kind::LostWrite);
}

#[test]
fn going_back_after_observing_indeterminate_write_is_stale() {
    // b never got a reply, but a reader saw it; later reading a is stale.
    assert_has(
        H::default()
            .set("k", "a", 0, 10)
            .set_unknown("k", "b", 20, 1000)
            .get("k", Some("b"), 30, 40)
            .get("k", Some("a"), 50, 60),
        Kind::StaleRead,
    );
}

#[test]
fn value_nobody_wrote_is_reported() {
    assert_has(H::default().set("k", "a", 0, 10).get("k", Some("zzz"), 20, 30), Kind::UnknownValue);
}

#[test]
fn value_read_before_it_was_written_is_reported() {
    assert_has(H::default().get("k", Some("a"), 0, 5).set("k", "a", 10, 20), Kind::FutureRead);
}

#[test]
fn wrong_reply_type_is_reported() {
    assert_has(H::default().op("k", Cmd::Get, None, 0, 1, Outcome::Protocol { msg: "GET returned :1".into() }), Kind::BadReply);
}

// ---------- lists: good histories ----------

#[test]
fn sequential_list_is_ok() {
    assert_ok(
        H::default()
            .lrange("l", &[], 0, 5)
            .rpush("l", "a", 1, 10, 20)
            .llen("l", 1, 25, 28)
            .rpush("l", "b", 2, 30, 40)
            .lrange("l", &["a", "b"], 50, 60)
            .lrange("l", &["a", "b"], 100, 110)
            .fin(),
    );
}

#[test]
fn concurrent_pushes_in_either_order_are_ok() {
    assert_ok(
        H::default()
            .rpush("l", "a", 2, 0, 100)
            .rpush("l", "b", 1, 0, 100)
            .lrange("l", &["b"], 10, 20)
            .llen("l", 1, 10, 20)
            .lrange("l", &["b", "a"], 110, 120)
            .fin(),
    );
}

#[test]
fn indeterminate_pushes_may_appear_or_not() {
    assert_ok(
        H::default()
            .rpush_unknown("l", "c", 0, 10)
            .rpush("l", "d", 1, 20, 30)
            .lrange("l", &["d"], 35, 40)
            .lrange("l", &["d", "c"], 50, 60) // c landed late
            .op("l", Cmd::Rpush, Some("e"), 70, 80, Outcome::Error { msg: "ERR".into() })
            .lrange("l", &["d", "c"], 100, 110)
            .fin(),
    );
}

#[test]
fn reads_overlapping_a_push_may_or_may_not_see_it() {
    assert_ok(
        H::default()
            .rpush("l", "a", 1, 0, 10)
            .rpush("l", "b", 2, 20, 60)
            .lrange("l", &["a", "b"], 25, 30)
            .llen("l", 2, 31, 32)
            .lrange("l", &["a"], 24, 40), // began before the other read finished
    );
}

// ---------- lists: bad histories ----------

#[test]
fn missing_acknowledged_push_is_stale() {
    assert_has(H::default().rpush("l", "a", 1, 0, 10).lrange("l", &[], 20, 30), Kind::StaleRead);
}

#[test]
fn final_list_missing_push_is_lost_write() {
    assert_has(
        H::default().rpush("l", "a", 1, 0, 10).rpush("l", "b", 2, 20, 30).lrange("l", &["a"], 100, 110).fin(),
        Kind::LostWrite,
    );
}

#[test]
fn lists_that_are_not_prefixes_diverge() {
    assert_has(
        H::default()
            .rpush_unknown("l", "a", 0, 10)
            .rpush_unknown("l", "b", 0, 10)
            .lrange("l", &["a", "b"], 20, 30)
            .lrange("l", &["b"], 20, 30),
        Kind::ListDivergence,
    );
}

#[test]
fn duplicate_item_is_reported() {
    assert_has(H::default().rpush("l", "a", 1, 0, 10).lrange("l", &["a", "a"], 20, 30), Kind::DuplicateInList);
}

#[test]
fn push_length_must_match_position() {
    assert_has(
        H::default()
            .rpush_unknown("l", "b", 0, 10)
            .rpush_unknown("l", "c", 0, 10)
            .rpush("l", "a", 2, 0, 10)
            .lrange("l", &["b", "c", "a"], 20, 30),
        Kind::MisplacedPush,
    );
}

#[test]
fn two_pushes_with_same_length_are_reported() {
    assert_has(H::default().rpush("l", "a", 1, 0, 10).rpush("l", "b", 1, 0, 10), Kind::DuplicatePushLength);
}

#[test]
fn push_order_must_respect_real_time() {
    assert_has(
        H::default().rpush("l", "a", 1, 0, 10).rpush_unknown("l", "b", 20, 30).lrange("l", &["b", "a"], 40, 50),
        Kind::PushOrder,
    );
}

#[test]
fn list_getting_shorter_is_reported() {
    assert_has(
        H::default()
            .rpush_unknown("l", "x", 0, 1)
            .rpush_unknown("l", "y", 0, 1)
            .rpush_unknown("l", "z", 0, 1)
            .llen("l", 3, 2, 10)
            .llen("l", 2, 20, 30),
        Kind::LengthRegression,
    );
    // A push that finishes after another must return a larger length.
    assert_has(H::default().rpush("l", "a", 2, 0, 10).rpush("l", "b", 2, 20, 30), Kind::LengthRegression);
}

#[test]
fn length_beyond_pushes_sent_is_reported() {
    assert_has(H::default().rpush("l", "a", 1, 0, 10).llen("l", 5, 20, 30), Kind::LengthTooLarge);
    assert_has(H::default().llen("l", 1, 0, 10).rpush("l", "a", 1, 20, 30), Kind::LengthTooLarge);
}

#[test]
fn checks_are_per_key() {
    // Same values on different keys don't interfere.
    assert_ok(
        H::default()
            .set("k1", "a", 0, 10)
            .set("k2", "a", 0, 10)
            .set("k1", "b", 20, 30)
            .get("k2", Some("a"), 40, 50)
            .get("k1", Some("b"), 40, 50),
    );
}

#[test]
fn render_mentions_ops() {
    let h = H::default().set("k", "a", 0, 10).set("k", "b", 20, 30).get("k", Some("a"), 40, 50);
    let v = check(&h.h);
    let s = crate::check::render(&v, &h.h, 5);
    assert!(s.contains("STALE READ"), "{s}");
    assert!(s.contains("#3"), "{s}");
}

// ---------- required (non-strict) checks ----------

fn required(h: &H) -> Vec<Kind> {
    let (fail, _) = crate::check::partition(check(&h.h), false);
    let mut k: Vec<Kind> = fail.into_iter().map(|v| v.kind).collect();
    k.sort();
    k.dedup();
    k
}

#[test]
fn stale_reads_during_the_run_are_not_required() {
    let h = H::default()
        .set("k", "a", 0, 10)
        .set("k", "b", 20, 30)
        .get("k", Some("a"), 40, 50)
        .get("k", Some("b"), 100, 110)
        .fin();
    assert_eq!(required(&h), vec![]);
    assert_eq!(h.kinds(), vec![Kind::StaleRead]);
}

#[test]
fn list_order_during_the_run_is_not_required() {
    // Mid-run reads disagree on order with the final list; nothing was lost.
    let h = H::default()
        .rpush("l", "a", 1, 0, 10)
        .rpush("l", "b", 1, 20, 30)
        .lrange("l", &["b"], 35, 40)
        .lrange("l", &["a", "b"], 100, 110)
        .fin();
    assert_eq!(required(&h), vec![]);
    assert!(!h.kinds().is_empty());
}

#[test]
fn push_seen_mid_run_but_missing_at_the_end_is_lost() {
    let h = H::default()
        .rpush_unknown("l", "a", 0, 10)
        .lrange("l", &["a"], 20, 30)
        .lrange("l", &[], 100, 110)
        .fin();
    assert_eq!(required(&h), vec![Kind::LostWrite]);
}

#[test]
fn final_list_shorter_than_observed_length_is_lost() {
    let h = H::default()
        .rpush_unknown("l", "a", 0, 10)
        .rpush_unknown("l", "b", 0, 10)
        .llen("l", 2, 20, 30)
        .lrange("l", &["a"], 100, 110)
        .fin();
    assert_eq!(required(&h), vec![Kind::LostWrite]);
}

#[test]
fn final_value_overwritten_by_later_write_is_lost_even_without_strict() {
    let h = H::default().set("k", "a", 0, 10).set("k", "b", 20, 30).get("k", Some("a"), 100, 110).fin();
    assert_eq!(required(&h), vec![Kind::LostWrite]);
}
