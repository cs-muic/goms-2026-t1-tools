//! Per-key safety checks over a recorded history.
//!
//! Every check here is *sound*: it only reports a violation when no
//! linearization of that key's operations could explain what was observed.
//! (It is not complete: some non-linearizable histories pass. A full
//! linearizability search is out of scope.)
//!
//! Conventions:
//! * Values written by SET/RPUSH are unique, so each value read identifies
//!   the write that produced it.
//! * A write "definitely took effect by time t" (its `eff` time) if it was
//!   acknowledged at t, or if some read that returned its value completed at t.
//!   Writes that got an error, a bad reply, or no reply at all are
//!   indeterminate: they may take effect at any point after their invocation,
//!   or never.
//! * Recorded intervals contain the real ones, so `a.resp < b.inv` in the
//!   history means a really finished before b began.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;

use crate::history::{secs, Cmd, History, Op, Outcome};

const INF: u64 = u64::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    LostWrite,
    StaleRead,
    UnknownValue,
    FutureRead,
    ListDivergence,
    DuplicateInList,
    MisplacedPush,
    DuplicatePushLength,
    PushOrder,
    LengthRegression,
    LengthTooLarge,
    BadReply,
    MixedKey,
    DuplicateWrite,
}

impl Kind {
    pub fn title(self) -> &'static str {
        match self {
            Kind::LostWrite => "lost write",
            Kind::StaleRead => "stale read",
            Kind::UnknownValue => "value never written",
            Kind::FutureRead => "read from the future",
            Kind::ListDivergence => "lists diverge",
            Kind::DuplicateInList => "duplicate list item",
            Kind::MisplacedPush => "RPUSH length disagrees with list",
            Kind::DuplicatePushLength => "two RPUSHes returned the same length",
            Kind::PushOrder => "list order contradicts real time",
            Kind::LengthRegression => "list got shorter",
            Kind::LengthTooLarge => "list longer than pushes so far",
            Kind::BadReply => "malformed or wrong-type reply",
            Kind::MixedKey => "key used as both string and list (checker bug)",
            Kind::DuplicateWrite => "value written twice (checker bug)",
        }
    }

    pub fn explain(self) -> &'static str {
        match self {
            Kind::LostWrite => "After the workload stopped, a final read did not see a write that had definitely taken effect.",
            Kind::StaleRead => "A read returned a value that had definitely been overwritten (or missed a push that had definitely happened) before the read began.",
            Kind::UnknownValue => "A read returned a value that no client ever wrote to that key.",
            Kind::FutureRead => "A read returned a value whose write had not even been sent when the read completed.",
            Kind::ListDivergence => "Two LRANGE results are not prefixes of one another. With only RPUSH, every read must see a prefix of the same list.",
            Kind::DuplicateInList => "An LRANGE result contains the same pushed value twice, but each value was pushed once.",
            Kind::MisplacedPush => "RPUSH returned length k, so its value must be item k of the list, but a read shows something else there.",
            Kind::DuplicatePushLength => "Two acknowledged RPUSHes to the same key returned the same new length.",
            Kind::PushOrder => "One RPUSH finished before another started, yet the list holds them in the opposite order.",
            Kind::LengthRegression => "An operation observed a shorter list than an operation that finished before it started.",
            Kind::LengthTooLarge => "A list length is larger than the number of RPUSHes sent to the key so far.",
            Kind::BadReply => "A reply had the wrong RESP type for the command (e.g. GET answered with an integer).",
            Kind::MixedKey => "The history mixes string and list commands on one key. mrc-check never does this.",
            Kind::DuplicateWrite => "The history writes the same value twice to one key. mrc-check never does this.",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Violation {
    pub kind: Kind,
    pub key: String,
    pub msg: String,
    pub ops: Vec<u64>,
}

/// Runs every check over the history.
pub fn check(h: &History) -> Vec<Violation> {
    let mut out = Vec::new();
    let mut by_key: BTreeMap<&str, Vec<&Op>> = BTreeMap::new();
    for op in &h.ops {
        if let Outcome::Protocol { msg } = &op.outcome {
            out.push(Violation {
                kind: Kind::BadReply,
                key: op.key.clone(),
                msg: msg.clone(),
                ops: vec![op.id],
            });
        }
        by_key.entry(&op.key).or_default().push(op);
    }
    for (key, ops) in by_key {
        let lists = ops.iter().filter(|o| o.cmd.is_list()).count();
        if lists == 0 {
            check_register(key, &ops, &mut out);
        } else if lists == ops.len() {
            check_list(key, &ops, &mut out);
        } else {
            out.push(Violation {
                kind: Kind::MixedKey,
                key: key.to_string(),
                msg: "key has both string and list operations".into(),
                ops: vec![],
            });
        }
    }
    out
}

fn v(kind: Kind, key: &str, msg: String, ops: Vec<u64>) -> Violation {
    Violation { kind, key: key.to_string(), msg, ops }
}

/// Indexes writes by value, reporting duplicates.
fn index_writes<'a>(key: &str, writes: &[&'a Op], out: &mut Vec<Violation>) -> HashMap<&'a str, usize> {
    let mut by_val = HashMap::new();
    for (i, w) in writes.iter().enumerate() {
        let val = w.value.as_deref().unwrap_or("");
        if let Some(&j) = by_val.get(val) {
            let first: &Op = writes[j];
            out.push(v(
                Kind::DuplicateWrite,
                key,
                format!("value {val:?} written by both #{} and #{}", first.id, w.id),
                vec![first.id, w.id],
            ));
        } else {
            by_val.insert(val, i);
        }
    }
    by_val
}

fn how(w: &Op, eff: u64) -> &'static str {
    if w.acked() && w.resp == eff {
        "was acknowledged"
    } else {
        "had been seen by another read"
    }
}

fn check_register(key: &str, ops: &[&Op], out: &mut Vec<Violation>) {
    let writes: Vec<&Op> = ops.iter().copied().filter(|o| o.cmd == Cmd::Set).collect();
    let by_val = index_writes(key, &writes, out);
    let reads: Vec<(&Op, Option<&str>)> = ops
        .iter()
        .filter_map(|o| match (&o.cmd, &o.outcome) {
            (Cmd::Get, Outcome::Value { value }) => Some((*o, value.as_deref())),
            _ => None,
        })
        .collect();

    let mut eff: Vec<u64> = writes.iter().map(|w| if w.acked() { w.resp } else { INF }).collect();
    // Source write of each read: None = skip, Some(None) = nil, Some(Some(i)) = writes[i].
    let mut src: Vec<Option<Option<usize>>> = Vec::with_capacity(reads.len());
    for &(r, val) in &reads {
        let s = match val {
            None => Some(None),
            Some(x) => match by_val.get(x) {
                None => {
                    out.push(v(
                        Kind::UnknownValue,
                        key,
                        format!("GET returned {x:?}, which no SET to this key ever wrote"),
                        vec![r.id],
                    ));
                    None
                }
                Some(&i) if writes[i].inv > r.resp => {
                    out.push(v(
                        Kind::FutureRead,
                        key,
                        format!(
                            "GET returned {x:?} at {}, but SET #{} of that value was only sent at {}",
                            secs(r.resp),
                            writes[i].id,
                            secs(writes[i].inv)
                        ),
                        vec![r.id, writes[i].id],
                    ));
                    None
                }
                Some(&i) => {
                    eff[i] = eff[i].min(r.resp);
                    Some(Some(i))
                }
            },
        };
        src.push(s);
    }

    // Writes sorted by invocation, with suffix minima of eff: for any time t,
    // the write that started after t and was earliest known to be in effect.
    let mut order: Vec<usize> = (0..writes.len()).collect();
    order.sort_by_key(|&i| writes[i].inv);
    let invs: Vec<u64> = order.iter().map(|&i| writes[i].inv).collect();
    let mut suf = vec![(INF, usize::MAX); order.len() + 1];
    for j in (0..order.len()).rev() {
        let i = order[j];
        suf[j] = suf[j + 1].min((eff[i], i));
    }

    for (&(r, val), s) in reads.iter().zip(src) {
        let Some(s) = s else { continue };
        let start = match s {
            None => 0,
            Some(i) if writes[i].acked() => {
                let t = writes[i].resp;
                invs.partition_point(|&x| x <= t)
            }
            // The source write is indeterminate; it could have landed arbitrarily late.
            Some(_) => continue,
        };
        let (e, wi) = suf[start];
        if e >= r.inv {
            continue;
        }
        let later = writes[wi];
        let kind = if r.final_read { Kind::LostWrite } else { Kind::StaleRead };
        let (what, mut ids) = match s {
            None => ("nil".to_string(), vec![r.id]),
            Some(i) => (
                format!("{:?} (from #{}, acknowledged at {})", val.unwrap_or(""), writes[i].id, secs(writes[i].resp)),
                vec![r.id, writes[i].id],
            ),
        };
        ids.push(later.id);
        let after = if s.is_some() { "started after that and " } else { "" };
        out.push(v(
            kind,
            key,
            format!(
                "{}GET starting at {} returned {what}, but SET #{} of {:?} {after}{} by {}{}",
                if r.final_read { "final " } else { "" },
                secs(r.inv),
                later.id,
                later.value.as_deref().unwrap_or(""),
                how(later, e),
                secs(e),
                if r.final_read { ": that write was lost" } else { "" }
            ),
            ids,
        ));
    }
}

fn check_list(key: &str, ops: &[&Op], out: &mut Vec<Violation>) {
    let pushes: Vec<&Op> = ops.iter().copied().filter(|o| o.cmd == Cmd::Rpush).collect();
    let by_val = index_writes(key, &pushes, out);

    // 1. Each LRANGE on its own: no duplicates, no unknown values, no future values.
    let mut lranges: Vec<(&Op, &[String])> = Vec::new();
    for o in ops {
        let (Cmd::Lrange, Outcome::List { items }) = (&o.cmd, &o.outcome) else { continue };
        let mut seen = HashSet::new();
        let mut ok = true;
        for (j, x) in items.iter().enumerate() {
            if !seen.insert(x.as_str()) {
                out.push(v(Kind::DuplicateInList, key, format!("LRANGE has {x:?} twice (again at index {j})"), vec![o.id]));
                ok = false;
                break;
            }
            match by_val.get(x.as_str()) {
                None => {
                    out.push(v(
                        Kind::UnknownValue,
                        key,
                        format!("LRANGE item {j} is {x:?}, which no RPUSH to this key ever sent"),
                        vec![o.id],
                    ));
                    ok = false;
                    break;
                }
                Some(&i) if pushes[i].inv > o.resp => {
                    out.push(v(
                        Kind::FutureRead,
                        key,
                        format!(
                            "LRANGE returned {x:?} at {}, but RPUSH #{} of it was only sent at {}",
                            secs(o.resp),
                            pushes[i].id,
                            secs(pushes[i].inv)
                        ),
                        vec![o.id, pushes[i].id],
                    ));
                    ok = false;
                    break;
                }
                Some(_) => {}
            }
        }
        if ok {
            lranges.push((o, items));
        }
    }

    // 2. All LRANGE results must be prefixes of the longest one, M.
    let (m_op, m): (Option<&Op>, &[String]) = lranges
        .iter()
        .max_by_key(|(o, items)| (items.len(), std::cmp::Reverse(o.id)))
        .map(|(o, items)| (Some(*o), *items))
        .unwrap_or((None, &[]));
    lranges.retain(|&(o, items)| {
        if m[..items.len()] == *items {
            return true;
        }
        let j = (0..items.len()).find(|&j| items[j] != m[j]).unwrap();
        let mo = m_op.unwrap();
        out.push(v(
            Kind::ListDivergence,
            key,
            format!(
                "LRANGE #{} has {:?} at index {j}, but the longer LRANGE #{} has {:?} there",
                o.id, items[j], mo.id, m[j]
            ),
            vec![o.id, mo.id],
        ));
        false
    });
    let pos: HashMap<&str, usize> = m.iter().enumerate().map(|(j, x)| (x.as_str(), j)).collect();
    let pos_of = |p: &Op| pos.get(p.value.as_deref().unwrap_or("")).copied();
    let where_is = |p: &Op| match pos_of(p) {
        Some(j) => format!("at index {j} in LRANGE #{}", m_op.unwrap().id),
        None => "in no LRANGE result".to_string(),
    };

    let mut eff: Vec<u64> = pushes.iter().map(|p| if p.acked() { p.resp } else { INF }).collect();
    for &(r, items) in &lranges {
        for x in items {
            let i = by_val[x.as_str()];
            eff[i] = eff[i].min(r.resp);
        }
    }

    // 3. RPUSH replies: length k means the value is item k (index k-1).
    let mut by_len: HashMap<i64, usize> = HashMap::new();
    for (i, p) in pushes.iter().enumerate() {
        let Outcome::Int { n: k } = p.outcome else { continue };
        let val = p.value.as_deref().unwrap_or("");
        if k < 1 {
            out.push(v(Kind::MisplacedPush, key, format!("RPUSH {val:?} returned length {k}"), vec![p.id]));
            continue;
        }
        if let Some(&j) = by_len.get(&k) {
            out.push(v(
                Kind::DuplicatePushLength,
                key,
                format!("RPUSH #{} and RPUSH #{} both returned length {k}", pushes[j].id, p.id),
                vec![pushes[j].id, p.id],
            ));
        } else {
            by_len.insert(k, i);
        }
        let idx = (k - 1) as usize;
        if idx < m.len() && m[idx] != val {
            out.push(v(
                Kind::MisplacedPush,
                key,
                format!(
                    "RPUSH {val:?} returned length {k}, so it should be at index {idx}, but LRANGE #{} has {:?} there and {val:?} {}",
                    m_op.unwrap().id,
                    m[idx],
                    match pos_of(p) {
                        Some(j) => format!("at index {j}"),
                        None => "missing".into(),
                    }
                ),
                vec![p.id, m_op.unwrap().id],
            ));
        }
    }

    let mut flagged: HashSet<u64> = HashSet::new();

    // 4. Stale LRANGE: every push known to be in effect before the read began
    //    must be inside the returned prefix.
    let mut by_eff: Vec<usize> = (0..pushes.len()).filter(|&i| eff[i] < INF).collect();
    by_eff.sort_by_key(|&i| eff[i]);
    let mut reads_by_inv: Vec<&(&Op, &[String])> = lranges.iter().collect();
    reads_by_inv.sort_by_key(|(o, _)| o.inv);
    let mut ptr = 0;
    let mut worst: Option<(usize, usize)> = None; // (pos or MAX, push index)
    for &&(r, items) in &reads_by_inv {
        while ptr < by_eff.len() && eff[by_eff[ptr]] < r.inv {
            let i = by_eff[ptr];
            let p = pos_of(pushes[i]).unwrap_or(usize::MAX);
            if worst.is_none_or(|(wp, _)| p > wp) {
                worst = Some((p, i));
            }
            ptr += 1;
        }
        let Some((wp, i)) = worst else { continue };
        if wp < items.len() {
            continue;
        }
        let p = pushes[i];
        flagged.insert(r.id);
        out.push(v(
            if r.final_read { Kind::LostWrite } else { Kind::StaleRead },
            key,
            format!(
                "{}LRANGE starting at {} returned {} items, but RPUSH #{} of {:?} {} by {} and is {}{}",
                if r.final_read { "final " } else { "" },
                secs(r.inv),
                items.len(),
                p.id,
                p.value.as_deref().unwrap_or(""),
                how(p, eff[i]),
                secs(eff[i]),
                where_is(p),
                if r.final_read && pos_of(p).is_none() { ": that push was lost" } else { "" }
            ),
            vec![r.id, p.id],
        ));
    }

    // 5. Real-time order of pushes that appear in M.
    let mut in_m_by_inv: Vec<usize> = (0..pushes.len()).filter(|&i| pos_of(pushes[i]).is_some()).collect();
    in_m_by_inv.sort_by_key(|&i| pushes[i].inv);
    let mut ptr = 0;
    let mut worst: Option<(usize, usize)> = None;
    for &i2 in &in_m_by_inv {
        let p2 = pushes[i2];
        while ptr < by_eff.len() && eff[by_eff[ptr]] < p2.inv {
            let i = by_eff[ptr];
            if let Some(p) = pos_of(pushes[i]) {
                if worst.is_none_or(|(wp, _)| p > wp) {
                    worst = Some((p, i));
                }
            }
            ptr += 1;
        }
        let Some((wp, i1)) = worst else { continue };
        let pos2 = pos_of(p2).unwrap();
        if wp > pos2 {
            let p1 = pushes[i1];
            out.push(v(
                Kind::PushOrder,
                key,
                format!(
                    "RPUSH #{} of {:?} {} by {}, before RPUSH #{} of {:?} started at {}, but the list has {:?} at index {pos2} and {:?} at index {wp}",
                    p1.id,
                    p1.value.as_deref().unwrap_or(""),
                    how(p1, eff[i1]),
                    secs(eff[i1]),
                    p2.id,
                    p2.value.as_deref().unwrap_or(""),
                    secs(p2.inv),
                    p2.value.as_deref().unwrap_or(""),
                    p1.value.as_deref().unwrap_or(""),
                ),
                vec![p1.id, p2.id],
            ));
        }
    }

    // 6. Lengths. Every RPUSH reply, LRANGE and LLEN observes a length; lists
    //    only grow, and each RPUSH grows them.
    struct Obs<'a> {
        op: &'a Op,
        len: i64,
        push: bool,
    }
    let mut obs: Vec<Obs> = Vec::new();
    for p in &pushes {
        if let Outcome::Int { n } = p.outcome {
            obs.push(Obs { op: p, len: n, push: true });
        }
    }
    for &(r, items) in &lranges {
        obs.push(Obs { op: r, len: items.len() as i64, push: false });
    }
    for o in ops {
        if let (Cmd::Llen, Outcome::Int { n }) = (&o.cmd, &o.outcome) {
            obs.push(Obs { op: o, len: *n, push: false });
        }
    }
    let mut by_resp: Vec<&Obs> = obs.iter().collect();
    by_resp.sort_by_key(|o| o.op.resp);
    let resps: Vec<u64> = by_resp.iter().map(|o| o.op.resp).collect();
    let mut pre: Vec<Option<&Obs>> = vec![None; by_resp.len() + 1];
    for j in 0..by_resp.len() {
        pre[j + 1] = match pre[j] {
            Some(best) if best.len >= by_resp[j].len => Some(best),
            _ => Some(by_resp[j]),
        };
    }
    let mut push_invs: Vec<u64> = pushes.iter().map(|p| p.inv).collect();
    push_invs.sort_unstable();
    for b in &obs {
        if let Some(a) = pre[resps.partition_point(|&t| t < b.op.inv)] {
            let shrank = a.len > b.len || (b.push && a.len >= b.len);
            if shrank && !flagged.contains(&b.op.id) {
                out.push(v(
                    if b.op.final_read { Kind::LostWrite } else { Kind::LengthRegression },
                    key,
                    format!(
                        "{} #{} saw length {} at {}, but {} #{} had already seen length {} by {}",
                        b.op.cmd.name(),
                        b.op.id,
                        b.len,
                        secs(b.op.inv),
                        a.op.cmd.name(),
                        a.op.id,
                        a.len,
                        secs(a.op.resp)
                    ),
                    vec![b.op.id, a.op.id],
                ));
            }
        }
        let sent = push_invs.partition_point(|&t| t <= b.op.resp) as i64;
        if b.len > sent {
            out.push(v(
                Kind::LengthTooLarge,
                key,
                format!(
                    "{} #{} saw length {}, but only {sent} RPUSHes to this key had been sent by {}",
                    b.op.cmd.name(),
                    b.op.id,
                    b.len,
                    secs(b.op.resp)
                ),
                vec![b.op.id],
            ));
        }
    }

}

/// Human-readable report: counts per kind, then up to `max_examples` of each.
pub fn render(violations: &[Violation], h: &History, max_examples: usize) -> String {
    let ops: HashMap<u64, &Op> = h.ops.iter().map(|o| (o.id, o)).collect();
    let mut by_kind: BTreeMap<Kind, Vec<&Violation>> = BTreeMap::new();
    for x in violations {
        by_kind.entry(x.kind).or_default().push(x);
    }
    let mut s = String::new();
    for (kind, list) in &by_kind {
        let keys: HashSet<&str> = list.iter().map(|x| x.key.as_str()).collect();
        let _ = writeln!(s, "\n{} x{} on {} key(s): {}", kind.title().to_uppercase(), list.len(), keys.len(), kind.explain());
        for x in list.iter().take(max_examples) {
            let _ = writeln!(s, "  key {}: {}", x.key, x.msg);
            for id in &x.ops {
                if let Some(o) = ops.get(id) {
                    let _ = writeln!(s, "      {}", o.describe());
                }
            }
        }
        if list.len() > max_examples {
            let _ = writeln!(s, "  ... and {} more", list.len() - max_examples);
        }
    }
    s
}
