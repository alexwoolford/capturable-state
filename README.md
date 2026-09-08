# capturable-state

Canonical SQLite capture layer for standalone utilities that can optionally join a mosaic.

This crate owns **only**:

- `_outbox` (monotonic `seq`, never dropped)
- generated `AFTER INSERT/UPDATE/DELETE` triggers
- a startup assertion that each trigger’s column list matches `PRAGMA table_info`
- an announce file and a fire-and-forget datagram nudge after `commit()`

It does **not** own business schemas, HTTP, scheduling, or the collector daemon.

Spec: [docs/design-principles.md](docs/design-principles.md). Clocks: [docs/datetime.md](docs/datetime.md).

## Depend by git tag (not a path)

Each utility is its own repo. Pin a tag. Do **not** `path = "../capturable-state"`.

```toml
capturable-state = { git = "https://github.com/alexwoolford/capturable-state", tag = "v0.1.0" }
```

```rust
use capturable_state::{install, CaptureConfig, CaptureMode, TableSpec};

let nudge = install(
    &conn,
    &CaptureConfig::new(
        "adsb-trip-journal",
        path,
        &[
            TableSpec::new("trips", CaptureMode::Full),
            TableSpec::new("flights_all_slice", CaptureMode::After),
        ],
    ),
)?;
// ... write in a transaction, commit ...
nudge.send(); // ignore-all-errors; collector may be down
```

Announce and nudge are no-ops when the collector is absent. A utility must stay useful with only its own SQLite file.

Env:

- `STATE_CAPTURE_ANNOUNCE_DIR` — default `/var/lib/state-capture/announce`
- `STATE_CAPTURE_SOCK` — default `/run/state/collect.sock`

If the announce dir cannot be created, the crate writes `{sqlite_dir}/.capturable.json`.

The collector watches the **writable** sqlite (work / in-place journal), never a published `current/` snapshot.

## Clocks (P5)

Two clocks, two jobs:

| Where | Type | Meaning |
| --- | --- | --- |
| Utility fact columns (`as_of_date`, `dep_ts`, `probed_at`, …) | TEXT `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SSZ` | When it was true. Calendar days cannot be Unix seconds without inventing midnight. |
| `_outbox.ts`, `deleted_at` | INTEGER Unix seconds via `strftime('%s','now')` | When the mutation was recorded. **Ordering is `seq`**, not `ts`. |

Do not dual-write. Do not convert fact columns to epoch.
