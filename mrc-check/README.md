# mrc-check

A black-box correctness checker for Mini 3 (Micro-Redis Cluster). It runs many
concurrent RESP2 clients against one or more routers. While they run, it adds and
removes storage nodes and pauses routers. Then it checks the recorded history,
key by key, for lost writes and lists that don't add up. With `--strict`, it
also checks for stale reads and other linearizability violations.

Passing doesn't prove a cluster correct. Failing proves it isn't: every reported
violation is something no cluster meeting the requirements could have produced.

## Build and test

```bash
cargo build --release        # binary: ../target/release/mrc-check (workspace target dir)
cargo test                   # checker logic on synthetic histories + end-to-end runs against fake servers
```

## Quick start

With a coordinator, three storage nodes on ports 7001 to 7003 and two routers on
6379 and 6380 already running:

```bash
mrc-check run \
  --router 127.0.0.1:6379 --router 127.0.0.1:6380 \
  --node 127.0.0.1:7001 --node 127.0.0.1:7002 --node 127.0.0.1:7003 \
  --start-node './scripts/start-node.sh' \
  --ctl './target/release/cluster-ctl --coordinator 127.0.0.1:7000' \
  --router-pid @run/router-6380.pid
```

This runs the default schedule: wait 5s, `SIGSTOP` the router, add a node, wait
2s, `SIGCONT` the router, wait 5s, remove the oldest node, wait 5s. The workload
runs for 60s or until the schedule finishes, whichever is later. After that come
final reads and the check. The exit status is 0 for PASS, 1 for FAIL and 2 for a
setup error (for example, a router that doesn't answer `PING`).

Demo-day mode is the same command with `--schedule ""`. The checker then only
drives traffic, and the student runs `cluster-ctl` by hand, on cue, in another
terminal. Make `--duration` long enough to cover all the changes.

## What it needs from the cluster

| Flag | Contract |
|---|---|
| `--router HOST:PORT` (repeatable, required) | Speaks RESP2 and supports `PING`, `SET`, `GET`, `RPUSH`, `LRANGE key 0 -1` and `LLEN`. Clients are spread round-robin across routers, one TCP connection each. |
| `--start-node CMD` | Run with `sh -c`. It must start a **fresh, empty** storage node *in the background*, print the node's `HOST:PORT` as the **last line of stdout**, and exit. `MRC_NODE_SEQ` (1, 2, …) is set so the script can pick a port. The checker waits for the script to exit, not for its stdout to close, but it's still best to redirect the node's own output (`>node.log 2>&1 &`). |
| `--ctl CMD` | A command prefix. The checker runs `CMD add-node <addr>` and `CMD remove-node <addr>` (the address is passed as a separate, safely quoted argument) and expects exit status 0 **once the change has finished**. Non-zero exit, or no exit within `--ctl-timeout` (default 300s), fails the run. |
| `--stop-node CMD` (optional) | Run after a successful `remove-node`, with `MRC_NODE_ADDR` set, to shut the removed node down. A failure here is only a warning. |
| `--node HOST:PORT` (repeatable) | The storage nodes that are members at the start. A bare `remove` step picks the oldest known member (initial nodes first, then ones the checker added), and never the last one. |
| `--router-pid PID` or `--router-pid @FILE` (repeatable) | Routers that `stop` may `SIGSTOP`. With several, `stop` rotates through them. Every stopped router is sent `SIGCONT` at `cont`, at the end of the schedule, and on Ctrl-C. |

## Schedule

`--schedule` is a comma-separated list of steps, run one after another while the
workload runs:

| Step | Meaning |
|---|---|
| `sleep:<dur>` | Wait. Durations look like `5`, `2.5s`, `500ms` or `1m`. |
| `add` | Run `--start-node`, then `--ctl add-node <addr>`. |
| `remove` / `remove:<addr>` | Run `--ctl remove-node` on the oldest known member, or on the given address. Then run `--stop-node`. |
| `stop` | `SIGSTOP` the next `--router-pid`. Clients connected to it simply wait. |
| `cont` | `SIGCONT` every stopped router. |

The schedule stops at the first failing step (the workload keeps going). With
`--repeat`, it loops until `--duration` has passed. If no `--schedule` is given,
the default depends on which of `--start-node`/`--ctl` and `--router-pid` are present:

- both: `sleep:5,stop,add,sleep:2,cont,sleep:5,remove,sleep:5`
- only node commands: `sleep:5,add,sleep:5,remove,sleep:5`
- only router PIDs: `sleep:5,stop,sleep:5,cont,sleep:5`
- neither: no steps (traffic only)

The interesting case is `stop,add,...,cont`. The paused router wakes up with an
old view of the ring and requests already in hand. Its storage nodes have to
refuse them, and it has to catch up and retry. Keep `--op-timeout` (default 30s)
longer than any pause, so those requests are answered rather than abandoned. An
abandoned request is indeterminate and proves nothing.

## Workload

- `--string-keys` (100) keys get `SET`/`GET` (50/50). `--list-keys` (50) keys
  get `RPUSH`/`LRANGE 0 -1`/`LLEN` (50/30/20). `--list-frac` (0.5) of requests go to lists.
- Every written value is unique (`c<client>-<seq>`), so any value read
  identifies the write that produced it.
- Keys are `<prefix>:<run id>:s<i>` and `<prefix>:<run id>:l<i>` (`--key-prefix`, default `mrc`).
  The run id is fresh for each run, so earlier runs never interfere.
- `--clients` (16) clients share a total of `--rate` (2000) requests/s. Use `0` for
  unlimited. A list stops getting pushes after `--max-list-len` (300).
- When the workload stops, every router reads every key once more (final reads).
- `--seed` fixes the workload's random choices, though not the cluster's timing.

## History

Every request is recorded with its invocation and completion times (microseconds
since the start, monotonic clock) and its outcome. Schedule events are recorded
too. The history is written as JSON lines to `--history FILE` (default
`mrc-history-<run id>.jsonl`). `mrc-check check FILE` re-runs the analysis on a
saved history.

```json
{"rec":"event","what":"add-node 127.0.0.1:7004","start":5012345,"end":7120004,"ok":true}
{"rec":"op","id":17,"client":3,"router":"127.0.0.1:6379","key":"mrc:1a2b3c4d:s7","cmd":"set","value":"c3-5","inv":81234,"resp":81502,"outcome":{"type":"ok"}}
{"rec":"op","id":18,"client":4,"router":"127.0.0.1:6380","key":"mrc:1a2b3c4d:l2","cmd":"lrange","inv":81240,"resp":81611,"outcome":{"type":"list","items":["c1-3","c4-1"]},"final_read":true}
```

Outcome types are `ok`, `value`, `int`, `list`, `error` (a `-ERR` reply),
`protocol` (a reply of the wrong type) and `unknown` (timeout or dropped
connection).

## What is checked

Mini 3 doesn't require linearizability. What it requires is that **no
acknowledged write is lost and nothing is invented**, even while keys move.
That's what the checker enforces by default. With `--strict`, it also fails on
the stronger, linearizability-only checks. Without `--strict`, those are still
run and their count is printed as a note, which makes a useful hint while
debugging.

A write *definitely took effect* if it was acknowledged, or if any read returned
its value. Writes that got an error, a malformed reply or no reply are
**indeterminate**. They may have taken effect or not, so they are never held
against the cluster. Each recorded interval contains the real one, so "A
finished before B started" in the history is true in reality too.

*Required (always fail the run):*

- **lost write**, checked on the final reads, which happen once the cluster is quiet.
  - String key: the final `GET` must not return nil, or a value from write W,
    if another write that definitely took effect started after W was acknowledged.
  - List key: the final `LRANGE` must contain every push that definitely took
    effect. It must also be at least as long as any length seen during the run.
    Order doesn't matter here.
- **value never written**, **read from the future** (a value read before its
  write was even sent), **duplicate list item**, and **list longer than pushes so far**.
  No correct cluster of any kind produces these.
- **malformed or wrong-type reply**, for example `GET` answered with an integer.
- **availability**: the run fails if more than `--max-failed-ops` (default 0)
  requests got an error reply or no reply. A router that retries properly never
  shows its clients an error. Raise the limit if you expect some.
- **a schedule step failed**: `start-node`, `add-node` or `remove-node` failed or timed out.

*Strict only (`--strict`), per key, during the run:*

- **stale read**: a `GET` returned a value that had definitely been overwritten
  before the `GET` began. Or an `LRANGE` is missing a push that definitely took
  effect before it began.
- **lists diverge**: every `LRANGE` result must be a prefix of the longest one.
- **RPUSH length disagrees with list**: `RPUSH` returning k means its value is item k.
  Also **two RPUSHes returned the same length**.
- **list order contradicts real time**: a push that finished before another started
  sits after it.
- **list got shorter**: a length seen by `RPUSH`, `LRANGE` or `LLEN` is smaller than
  one seen by an operation that finished before it started.

A split brain, where two nodes serve the same key, usually shows up as a lost
write: the final reads find only one half's writes. The strict checks are sound
but not complete. A full linearizability search would catch more, and it isn't
implemented.

## Output

The run prints a summary: operations by command, outcomes, latency p50/p99/max
of successful requests (a pause shows up in the max), and the schedule with each
step's start and end times. Then it prints violations grouped by kind, with up to
`--max-examples` examples each. Every example names the operations involved, with
times, so you can match them against your own logs. Here's one from a `--strict` run:

```
STALE READ x3 on 2 key(s): A read returned a value that had definitely been overwritten ...
  key mrc:10f469a7:s0: GET starting at 0.025s returned "c5-48" (from #706, acknowledged at 0.014s),
  but SET #1456 of "c1-101" started after that and was acknowledged by 0.024s
      #1549 client 7 via 127.0.0.1:6380: GET mrc:10f469a7:s0 [0.025s .. 0.025s] -> "c5-48"
      #706 client 5 via 127.0.0.1:6379: SET mrc:10f469a7:s0 c5-48 [0.014s .. 0.014s] -> OK
      #1456 client 1 via 127.0.0.1:6379: SET mrc:10f469a7:s0 c1-101 [0.024s .. 0.024s] -> OK
```

## Layout

- `src/resp.rs`: minimal RESP2 client
- `src/history.rs`: history types, JSON lines, reply classification
- `src/check.rs`: the per-key checks (`src/check_tests.rs` holds their tests)
- `src/schedule.rs`: schedule parsing, shell commands, signals
- `src/run.rs`: workload, final reads, summary
- `tests/end_to_end.rs`: full runs against in-process fake servers. A correct
  server passes. Servers that lose writes, serve stale reads or reorder pushes,
  and a split brain, must fail.
