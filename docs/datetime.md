# Datetime convention

Shared rules for every capturable SQLite utility. Implement in each repo; do not keep dual formats or “old vs new” branches in code.

## SQLite

SQLite has no datetime type. Storage classes are INTEGER, REAL, TEXT, BLOB, NULL. Declaring `DATETIME` only sets NUMERIC affinity. Use **TEXT** for fact columns.

SQLite’s `date()` / `datetime()` / `julianday()` understand ISO-8601 text. Lexicographic `ORDER BY` / `MIN` / `MAX` on the formats below match chronology.

Do not store Unix epoch (seconds or millis) in **fact** columns.

**Capture envelope only:** `_outbox.ts` and `deleted_at` are INTEGER Unix seconds (`strftime('%s','now')`). That is the mutation clock, not an as-of date. Ordering is `_outbox.seq`. See [design-principles.md](design-principles.md) P5. Do not dual-write facts as epoch.

## Two kinds of value

**Date** — a UTC calendar day, no time.

```
YYYY-MM-DD
```

Example: `2026-09-01`

Use for as-of days, SCD2 validity, and other calendar-day facts. Each utility names its own fact columns; this crate does not.

**Instant** — a UTC timestamp at **second** resolution.

```
YYYY-MM-DDTHH:MM:SSZ
```

Example: `2026-09-01T21:19:58Z`

- Always UTC
- Always `Z` (never `+00:00`)
- No fractional seconds
- Separator is `T`

Wire formats (unix seconds, RFC3339 with offset, …) convert at the ingest boundary. Source resolution here is seconds; do not invent millis.

## Invariants

- One helper per crate for instants (`utc_iso`). Dates: `NaiveDate` / `date_naive()` then `to_string()` / `%Y-%m-%d`.
- Fact column type in SQL: `TEXT`. Capture envelope (`_outbox.ts`, `deleted_at`) is INTEGER Unix seconds, not a fact clock.
- Parquet date columns: Utf8 `YYYY-MM-DD`, same strings as sqlite.
- Analyst queries may use `WHERE dep_ts >= '2026-09-01'` or `date(dep_ts)`.

## Out of scope

- Time zones other than UTC
- `TIMESTAMP` / `DATETIME` column types for show
- Migrating mixed strings in place — wipe and regenerate the sqlite if needed
