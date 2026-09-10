# Design principles: making utility state capturable

A standard for the SQLite-backed Rust utilities, so every one of them emits a clean change feed with no per-utility bespoke work.

Behaviours marked **\[verified\]** were tested against a live SQLite (3.37.2); the appendix has the raw output. Where a claim is inference rather than test, it says so.

---

## 0\. The contract

A utility is **capturable** when it satisfies five things. Everything else in this document is guidance on how to satisfy them cheaply.

1. It has one `_outbox` table with a monotonic, never-reused sequence.  
2. Every captured table has a **row identity that survives deletion**.  
3. Every mutation to a captured table produces exactly one outbox row, in the same transaction.  
4. Each event carries the full column set, named, plus an explicit extracted key.  
5. The utility announces itself to the collector and nudges it after commit.

If those hold, the collector needs no knowledge of the utility. That's the whole point: the cost of adding utility number twelve should be zero.

**Design rule of thumb:** the outbox is a *log of facts*, not a queue of instructions. If you find yourself wanting to put intent ("send an email") in it, you've crossed a line — that belongs in its own table, captured like anything else.

---

## 1\. Schema principles

### P1 — Every captured table needs a stable row identity

This is the one principle that actually constrains your schemas. An event must identify *the same logical row forever*, including after that row is deleted and something else takes its place.

| Table shape | Stable? | Action |
| :---- | :---- | :---- |
| Natural key — hostname, path, uuid, url | Yes | none |
| Composite of natural values — `(region, code)` | Yes | none |
| Composite containing a FK — `(job_id, step_no)` | **Only as stable as the parent** | fix the parent |
| Surrogate `INTEGER PRIMARY KEY`, rows never deleted | Yes | none |
| Surrogate `INTEGER PRIMARY KEY` \+ hard deletes | **No** — rowid reuse | soft-delete, or `AUTOINCREMENT` |
| No declared PK | Usable but weak | use `NEW.rowid`, or add a key |

**Instability propagates through foreign keys.** A child table with a perfectly good composite PK is still ambiguous if its parent's rowid was recycled — both children carry `p_id: 1` and nothing distinguishes them **\[verified\]**. Adding a surrogate autoincrement ID *to the child* does not fix this; it just carries the poisoned FK alongside a new number. **Fix parents; children inherit stability for free.**

Do not blanket-apply `AUTOINCREMENT` to every table. On junction tables it adds a meaningless surrogate and fixes nothing. Apply it where a surrogate integer key is the identity *and* rows are hard-deleted.

Note the constraints: `AUTOINCREMENT` is rejected on a TEXT primary key and on any `WITHOUT ROWID` table **\[verified\]**. That's fine — both are already stable.

### P2 — Prefer soft-delete over `DELETE` (the highest-leverage rule here)

Add `deleted_at INTEGER` and set it instead of issuing `DELETE`. One convention, applied at the utility template level, that retires the entire rowid-reuse class of problem across every table without auditing a single key **\[verified\]**: with soft-delete, a plain `INTEGER PRIMARY KEY` never reuses an id, because the row is never removed.

What else it buys:

- The DuckDB federation view still shows the row, so "what happened to X" is answerable without the event log.  
- Deletes become ordinary updates downstream — one less code path in every consumer.  
- No FK cascade surprises, and no orphaned children mid-cascade.  
- The truncate-optimization and REPLACE trigger hazards become irrelevant.

Honest costs:

- Tables grow. Needs a purge job for genuinely dead data, run with capture triggers detached (see §5).  
- Every query needs `WHERE deleted_at IS NULL`. Wrap it in a view per table if that gets tedious.  
- Uniqueness needs a partial index, not a plain `UNIQUE` **\[verified\]**:

```sql
CREATE UNIQUE INDEX h_live ON hosts(hostname) WHERE deleted_at IS NULL;
```

That correctly blocks two live rows with the same hostname while allowing a hostname to be retired and re-added.

Where soft-delete genuinely doesn't fit — high-churn ephemeral tables, caches, anything with a retention policy measured in hours — use `AUTOINCREMENT` and accept the growth in `sqlite_sequence` instead.

### P3 — Never use `INSERT OR REPLACE`

With the default `recursive_triggers=off`, a REPLACE emits **two INSERT events and no delete at all** — the replacement is invisible downstream **\[verified\]**. Turning `recursive_triggers` on changes it to delete-then-insert, which is noisy in a different way.

Use upsert, which produces the clean single `U`:

```sql
INSERT INTO t(id, v) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET v = excluded.v;
```

Add this to code review. It is the easiest way to silently corrupt a feed.

### P4 — Use `STRICT` tables

SQLite's flexible typing means an `INT` column can legally hold `'not-an-int'`, which then flows into your event JSON and panics a consumer. `STRICT` tables enforce declared types at write time and work normally with triggers **\[verified\]**:

```sql
CREATE TABLE jobs(
  id     INTEGER PRIMARY KEY,
  name   TEXT NOT NULL,
  state  TEXT NOT NULL CHECK (state IN ('queued','running','done','failed')),
  n      INTEGER
) STRICT;
```

`CHECK` constraints on enum-like columns are worth the two lines — they make the feed's value domain self-documenting, and they fail at the writer rather than three systems downstream. Requires SQLite 3.37+.

### P5 — Two clocks, two jobs (do not mix them)

SQLite has no date type. **Capture-layer** columns are **INTEGER Unix seconds, UTC**. **Fact** columns stay **TEXT** per [datetime.md](datetime.md) (`YYYY-MM-DD` calendar days, `YYYY-MM-DDTHH:MM:SSZ` instants). Do not convert fact clocks to epoch, and do not dual-write.

| Layer | Columns | Type | Role |
| :---- | :---- | :---- | :---- |
| Capture envelope | `_outbox.ts`, `deleted_at` | INTEGER Unix seconds (`strftime('%s','now')`) | When the mutation was recorded. NULL `deleted_at` = live. |
| Business facts | `as_of_date`, `valid_from`, `dep_ts`, `probed_at`, … | TEXT | When the fact was true. A calendar day has no time of day to store as Unix seconds. |

Ordering is `_outbox.seq`, not `ts`. Many rows in one ingest second is expected. Kafka record timestamps (milliseconds) and Debezium payload conversions are downstream concerns — they are not a reason to store FAA `valid_from` as epoch.

Portability note: `unixepoch()` is not available before 3.38 and `unixepoch('subsec')` needs 3.42 **\[verified — unavailable on 3.37.2\]**. `strftime('%s','now')` works everywhere. Use it in trigger defaults unless you've pinned a newer SQLite.

If you need sub-second resolution for ordering, don't — use the outbox `seq`, which is exact. Envelope timestamps are for humans and for time-bucketing; `seq` is for ordering.

### P6 — Booleans and NULLs

No boolean type: store `INTEGER` 0/1, declare it as `INTEGER` under STRICT, never `'true'`/`'false'` strings.

Be deliberate about NULL vs empty string. They're distinct values that look identical in most UIs, and a column that oscillates between them generates a stream of spurious change events that are invisible when you eyeball the data. Pick one to mean "absent" and add `CHECK (col <> '')` if the answer is NULL.

### P7 — The database is the state

If a utility keeps some state in the DB and some in a JSON sidecar, a log file, or in-memory-with-periodic-flush, the feed is a partial view and every consumer inherits the gap. Anything that matters goes in a table.

The corollary: anything in a table becomes visible. Don't put credentials, tokens, or PII in captured tables — or exclude those columns explicitly (§2).

### P8 — Don't capture derived state

If a column can be recomputed from other columns, it's noise in the feed and a source of spurious events. Either make it a `GENERATED` column — which triggers can read normally **\[verified\]** — or exclude it from capture. Generated columns are a good way to synthesise a stable capture key from natural columns without denormalising:

```sql
k TEXT GENERATED ALWAYS AS (a || ':' || b) VIRTUAL
```

---

## 2\. The capture layer

### Implementation: depend on this crate by git tag

Each utility is its own git repo. Pin this crate by **git tag** (crates.io later). Do **not** path-depend a sibling checkout (`path = "../capturable-state"`). That only builds if someone clones a parent folder of unrelated projects.

```toml
capturable-state = { git = "https://github.com/alexwoolford/capturable-state", tag = "v0.1.1" }
```

```rust
use capturable_state::{install, CaptureConfig, CaptureMode, TableSpec};

let nudge = install(
    &conn,
    &CaptureConfig::new("your-utility", sqlite_path, &[
        TableSpec::new("facts", CaptureMode::Full),
    ]),
)?;
// write, commit, then:
nudge.send(); // fire-and-forget; collector may be absent
```

Do not copy `src/*.rs` into the utility. Utility twelve does the same.

### Canonical outbox

Identical in every utility. Do not customise it.

```sql
CREATE TABLE _outbox (
  seq    INTEGER PRIMARY KEY AUTOINCREMENT,     -- non-negotiable; see below
  tbl    TEXT    NOT NULL,
  op     TEXT    NOT NULL CHECK (op IN ('I','U','D')),
  ts     INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  key    TEXT    NOT NULL,                       -- extracted PK, as JSON
  before TEXT,
  after  TEXT
) STRICT;
```

`AUTOINCREMENT` here is mandatory and is a different question from P1. Without it, draining the outbox by deleting resets `max(rowid)`, the next event gets seq 1, and the collector's watermark silently skips it **\[verified\]**. This fails quietly and is the most likely bug in the whole system.

**Never drop and recreate `_outbox`.** The counter lives in `sqlite_sequence` and resets on recreate **\[verified\]**, colliding with the collector's watermark. Since triggers are regenerated on every migration, make sure that code path touches triggers only.

### The `key` column earns its place

The generator knows each table's primary key, so it should emit it separately rather than making every consumer learn 30 table schemas:

```json
{"tbl":"steps","op":"U","key":{"job_id":41,"step_no":2},"before":{...},"after":{...}}
```

Downstream correlation becomes generic: entity identity is `(src_db, tbl, key)` for every table in every utility, with no per-table code.

### Triggers are generated, never hand-written

A trigger must name every column — there is no `NEW.*`. If you add a column and don't regenerate, the capture silently drops it: no error, no warning, permanent loss **\[verified\]**. Hand-maintained triggers across 30 tables will drift, and you won't find out until you need the data.

The generator reads `PRAGMA table_info(t)` and emits three triggers per table. Requirements:

- **Assert at startup** that each trigger's column list matches current `table_info`. This converts silent loss into a loud failure and is the single most valuable defensive check in the design.  
- **Don't blindly emit `NEW.rowid`** — it's rejected on `WITHOUT ROWID` tables (`no such column: NEW.rowid`) **\[verified\]**. Branch on whether a PK is declared.  
- **Capture is opt-in per table.** Static lookup tables don't need it. Take a list, not "everything".  
- **Support a column exclusion list** for large blobs, derived caches, and secrets.  
- **Support capture modes** per table: `full` (before+after), `after` (inserts/updates only), `key` (identity only). Chatty or wide tables should not default to full.  
- **Drop leftover `_cap_*` triggers** whose table is not in the current capture set. Removing a table from the list must stop emitting, without each utility listing retired names.  
- **`json_object` argument limits** (~50–60 columns at default SQLite caps). Current utilities are under that (FAA `aircraft` is ~37 payload columns). Do not rewrite payloads until a table actually hits the ceiling.

Triggers do exactly one thing: write to `_outbox`. A trigger with any other side effect makes the feed a liar. The startup assertion looks for quoted `NEW."col"` / `OLD."col"` so an excluded `v` is not confused with `valid_from`.

---

## 3\. Runtime conventions

### Pragmas, set identically on every connection

```
PRAGMA journal_mode = WAL;        -- once, persists in the file
PRAGMA synchronous  = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;
```

`synchronous=NORMAL` in WAL can lose the last transactions on power loss — but business data and its outbox rows are in the same transaction, so they're lost together and the feed stays consistent with the database. That's the property that matters. Use `FULL` only if losing recent state is unacceptable for that specific utility.

### Keep write transactions short

The outbox row is invisible until commit, which is correct. But a long write transaction also blocks the collector's prune, and in WAL mode a long *read* blocks checkpointing. Neither is fatal; both cause confusing latency.

### Post-commit nudge

After `commit()` returns, send a fire-and-forget datagram carrying the database identity:

```rust
let _ = sock.send_to(db_name.as_bytes(), "/run/state/collect.sock");
```

Ignore all errors — `ECONNREFUSED` when the collector is down is normal. Send the *identity*, so the collector wakes for one database rather than scanning all of them.

**The ping is a latency optimisation and nothing else.** Correctness comes entirely from the outbox plus the collector's periodic tick. Design it so that deleting the ping entirely degrades freshness and changes nothing else. Use systemd socket activation so the socket exists even when the daemon doesn't.

### Discovery

Each utility registers its database path and logical name — a small file in a known directory, or the first datagram on startup. Avoid making the collector glob the filesystem; utilities know where their own state is.

Announce env: `STATE_CAPTURE_ANNOUNCE_DIR` (default `/var/lib/state-capture/announce/{db_name}.json`). If that directory cannot be created, skip announce (`install` still succeeds — collector absent). If the directory exists but is not writable, `install` fails: the collector only reads that dir, not a sibling `.capturable.json`. Nudge env: `STATE_CAPTURE_SOCK` (default `/run/state/collect.sock`). Nudge send errors (`ECONNREFUSED`) stay ignored.

**Watch the writable file.** Capture follows the work / in-place sqlite (the file the utility mutates), not a published copy. Published `current/` copies (`VACUUM INTO` / `mv`) include `_outbox` and triggers; the collector must not watch them. Read-only consumers never fire triggers. Never drop `_outbox` on publish.

### One subtle safety property worth understanding

The outbox `seq` is assigned at *insert* time, not commit time. That would normally be a bug — a transaction holding seq 5 could commit after one holding seq 6, and a watermark advanced past 6 would skip 5 forever.

It's safe here only because **SQLite permits one write transaction at a time**, so seq order and commit order cannot diverge. (Inference from SQLite's WAL concurrency model, not directly tested.)

Write that assumption down, because it breaks the moment you adopt an engine with concurrent writers — `BEGIN CONCURRENT`, or Turso's MVCC. That is precisely why Turso's CDC v2 format added `change_txn_id` and explicit COMMIT records. If you migrate, the collector needs commit-ordered sequencing, not insert-ordered.

---

## 4\. The event envelope

What the collector writes to the central `events.db`:

```json
{
  "src_db": "job-runner",
  "seq": 45912,
  "tbl": "jobs",
  "op": "U",
  "ts": 1788756146,
  "key": {"id": 41},
  "before": {"id":41,"state":"running","attempts":2},
  "after":  {"id":41,"state":"failed","attempts":3}
}
```

Consumption rules, which hold for every utility:

- **Idempotency key** is `(src_db, seq)`. Insert with `INSERT OR IGNORE`; act only when `changes() > 0`. This turns at-least-once delivery into exactly-once alerting using a unique constraint alone.  
- **Entity identity** is `(src_db, tbl, key)`.  
- **Ordering** is a total order *within* a database, via `seq`. There is no cross-database order and no useful way to synthesise one — timestamps won't do it. Design consumers that don't need it.  
- **Soft-deletes arrive as `U` with `deleted_at` set,** not as `D`. That's honest — the row still exists. Let consumers interpret it; don't have the trigger lie about the operation.  
- Current state is reconstructed by replaying per entity key; history *is* the log.

Emit `before` as well as `after` on updates unless the table is excluded. Diffing at the consumer is what makes "alert when state transitions to failed" a one-liner instead of a stateful join.

---

## 5\. Retrofitting an existing utility

Ordered. Steps 1–4 are additive and safe; step 5 requires table rebuilds; 6–9 wire up capture.

1. **Inventory.** List tables; decide the capture set. Exclude lookups, caches, anything with secrets.  
2. **Audit identity** per captured table against the P1 table. Note which ones hard-delete.  
3. **Ban `INSERT OR REPLACE`.** Grep for it. Replace with `ON CONFLICT DO UPDATE`. Cheap, immediate, no schema change.  
4. **Introduce soft-delete** where feasible: add `deleted_at INTEGER`, convert `DELETE` call sites, add partial unique indexes, add `WHERE deleted_at IS NULL` to reads. This is the highest-value step and needs no table rebuild.  
5. **Rebuild only what's left** — tables still hard-deleting with a surrogate integer PK (needing `AUTOINCREMENT`), or tables you want `STRICT`. Neither can be added with `ALTER TABLE`; both need the create-copy-drop-rename procedure.  
6. **Add `_outbox`** using the canonical DDL.  
7. **Generate triggers** for the capture set; add the startup assertion.  
8. **Add the post-commit ping** and startup registration.  
9. **Verify** with the conformance checklist below before moving to the next utility.

### The rebuild hazard

If capture triggers exist on the new table while you copy rows into it, **every copied row generates a spurious capture event** — a 5-row test produced 5 bogus events **\[verified\]**. On a real table that's a migration flooding the feed with fake inserts that consumers will treat as real state changes.

Order matters:

```sql
PRAGMA foreign_keys = OFF;
BEGIN;
  CREATE TABLE jobs_new(...) STRICT;        -- no triggers yet
  INSERT INTO jobs_new SELECT ... FROM jobs;
  DROP TABLE jobs;
  ALTER TABLE jobs_new RENAME TO jobs;
  -- recreate indexes, then generate triggers LAST
COMMIT;
PRAGMA foreign_key_check;
PRAGMA foreign_keys = ON;
```

Same rule for bulk purges of soft-deleted rows: detach triggers, purge, reattach. Treat any migration as a break in the feed and let consumers re-snapshot from the live tables rather than trying to represent it as change events.

---

## 6\. Anti-patterns

| Pattern | Why it hurts capture |
| :---- | :---- |
| `INSERT OR REPLACE` | emits two INSERTs, no delete **\[verified\]** |
| `DELETE` \+ reinsert as an update idiom | destroys identity; guarantees rowid reuse |
| State in JSON sidecars or log files | feed is a partial view; consumers inherit the gap |
| One generic `kv(key, value)` table | captures fine but carries no schema; every consumer reimplements parsing |
| Large BLOBs in captured tables | outbox and event log bloat with data nobody queries |
| Derived/computed columns captured | spurious events; use `GENERATED` or exclude |
| Triggers with side effects beyond the outbox | the feed stops describing reality |
| Hand-maintained triggers | drift silently on schema change **\[verified\]** |
| Dropping and recreating `_outbox` | resets the sequence; collides with the watermark **\[verified\]** |
| Blanket `AUTOINCREMENT` on junction tables | meaningless surrogate; doesn't fix FK-propagated instability |

---

## 7\. Conformance checklist

Automate this and run it against each utility in CI. A utility passes when:

- [ ] `_outbox` exists, matches the canonical DDL, `seq` is `AUTOINCREMENT`  
- [ ] Every table in the capture set has all three triggers  
- [ ] Each trigger's column list matches current `PRAGMA table_info` — assert at startup, not just in CI  
- [ ] A synthetic insert/update/delete on each captured table produces exactly one outbox row each, with the right `op` and a non-empty `key`  
- [ ] A rolled-back transaction produces zero outbox rows **\[verified as a property of the design\]**  
- [ ] No `INSERT OR REPLACE` anywhere in the source  
- [ ] Every captured table satisfies P1 — encode the decision table as a test  
- [ ] Timestamps are INTEGER epoch seconds, UTC  
- [ ] Excluded columns really are excluded (secrets test)  
- [ ] Utility registers on startup and pings after commit  
- [ ] Killing the collector for 60s, then restarting, loses nothing

That last one is the real test. Everything else is a detail.

---

## Appendix — verified behaviours

Tested on SQLite 3.37.2. Re-run against your deployed version; the timestamp functions in particular are version-sensitive.

```
STRICT + triggers          : OK, and type violations rejected at write
                             -> "cannot store TEXT value in INT column j.n"
rebuild with triggers on   : 5 spurious capture events from a 5-row copy
partial unique index       : blocks duplicate live rows, allows re-add after soft-delete
soft-delete, plain PK      : ids [1,2] -- no rowid reuse without AUTOINCREMENT
unixepoch()                : UNAVAILABLE on 3.37.2 (needs 3.38+)
unixepoch('subsec')        : UNAVAILABLE on 3.37.2 (needs 3.42+)
strftime('%s','now')       : 1788756146  -- portable, use this
generated column in trigger: readable -> {"k":"x:y"}

from earlier testing:
INSERT OR REPLACE, rt=off  : ['I:1','I:1']     -- two inserts, no delete
INSERT OR REPLACE, rt=on   : ['I:1','D:1','I:1']
ON CONFLICT DO UPDATE      : ['I:1','U:1']     -- correct
outbox PK without AUTOINC  : drained hi=3, next id=1 -> event LOST
outbox PK with AUTOINC     : drained hi=3, next id=4 -> ok
json_object + new column   : column silently dropped from capture
DELETE FROM (no WHERE)     : fires all delete triggers (truncate opt disabled by triggers)
rolled-back transaction    : 0 outbox rows -- capture is atomic
PRAGMA data_version        : detects other connections' commits, ignores own
NEW.rowid on WITHOUT ROWID : rejected
AUTOINCREMENT on TEXT PK   : rejected
AUTOINCREMENT + WITHOUT ROWID: rejected
_outbox drop/recreate      : sequence resets to 1
```

**Capture overhead**, 20k inserts, one transaction, WAL, `synchronous=NORMAL`:

| Configuration | Throughput | Per row |
| :---- | :---- | :---- |
| plain PK, no capture | 2,129,302 rows/s | 0.5 µs |
| `AUTOINCREMENT`, no capture | 1,397,958 rows/s | 0.7 µs |
| plain PK \+ outbox trigger | 780,081 rows/s | 1.3 µs |
| `AUTOINCREMENT` \+ outbox trigger | 672,506 rows/s | 1.5 µs |

Capture costs roughly 3x on write throughput in relative terms and about 1 µs in absolute terms. At 670k rows/sec with everything enabled, this is not a constraint for utilities in this class — spend the microsecond and don't design around it.  
