//! Capture layer for SQLite-backed utilities.
//!
//! Owns `_outbox`, generated triggers, a startup column-list assertion, an
//! announce file, and a fire-and-forget datagram nudge. Business schemas stay
//! in each utility.
//!
//! **Clocks:** `_outbox.ts` and `deleted_at` (when a utility uses it) are INTEGER
//! Unix seconds. Fact columns in the utilities stay TEXT (`YYYY-MM-DD` /
//! `YYYY-MM-DDTHH:MM:SSZ`). See `docs/datetime.md`.

mod announce;
mod nudge;
mod outbox;
mod pragmas;
mod triggers;

use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use rusqlite::{Connection, OptionalExtension};

pub use announce::{announce, announce_path, DEFAULT_ANNOUNCE_DIR, ENV_ANNOUNCE_DIR};
pub use nudge::{Nudge, DEFAULT_SOCK, ENV_SOCK};
pub use outbox::{ensure_outbox, OUTBOX_DDL, OUTBOX_TABLE};
pub use pragmas::apply_runtime_pragmas;
pub use triggers::{assert_triggers, install_triggers, CaptureMode, TableSpec};

/// Logical database name plus the tables to capture.
pub struct CaptureConfig<'a> {
    pub db_name: &'a str,
    pub sqlite_path: &'a Path,
    pub tables: &'a [TableSpec<'a>],
    /// Override `STATE_CAPTURE_ANNOUNCE_DIR`. `None` uses env / default.
    pub announce_dir: Option<&'a Path>,
    /// Override `STATE_CAPTURE_SOCK`. `None` uses env / default.
    pub sock: Option<&'a Path>,
}

impl<'a> CaptureConfig<'a> {
    pub fn new(db_name: &'a str, sqlite_path: &'a Path, tables: &'a [TableSpec<'a>]) -> Self {
        Self {
            db_name,
            sqlite_path,
            tables,
            announce_dir: None,
            sock: None,
        }
    }
}

/// Install `_outbox`, regenerate capture triggers, assert column lists, announce.
///
/// Never drops `_outbox`. Returns a [`Nudge`] to call after `commit()`; send
/// failures are ignored (collector may be down).
pub fn install(conn: &Connection, cfg: &CaptureConfig<'_>) -> Result<Nudge> {
    validate_db_name(cfg.db_name)?;
    if cfg.tables.is_empty() {
        bail!("capture set is empty");
    }
    ensure_outbox(conn)?;
    install_triggers(conn, cfg.tables)?;
    assert_triggers(conn, cfg.tables)?;
    let _ = announce(cfg.db_name, cfg.sqlite_path, cfg.announce_dir);
    Ok(Nudge::new(cfg.db_name, cfg.sock))
}

/// Same pragmas as the design spec, plus the usual open timeout.
pub fn open_timeout() -> Duration {
    Duration::from_millis(5_000)
}

fn validate_db_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if ok {
        Ok(())
    } else {
        bail!("db_name must be ASCII alphanumeric / hyphen / underscore, got {name:?}")
    }
}

/// True when `sqlite_master.sql` for `table` contains `STRICT`.
pub fn table_is_strict(conn: &Connection, table: &str) -> Result<bool> {
    let sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        )
        .optional()
        .with_context(|| format!("sqlite_master sql for {table}"))?;
    Ok(sql.is_some_and(|s| s.to_ascii_uppercase().contains("STRICT")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::TempDir;

    fn cap_cfg<'a>(
        db_name: &'a str,
        sqlite_path: &'a Path,
        tables: &'a [TableSpec<'a>],
        dir: &'a TempDir,
    ) -> CaptureConfig<'a> {
        CaptureConfig {
            db_name,
            sqlite_path,
            tables,
            announce_dir: Some(dir.path()),
            sock: Some(dir.path()),
        }
    }

    fn open_mem() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        apply_runtime_pragmas(&conn).unwrap();
        conn
    }

    #[test]
    fn outbox_has_autoincrement() {
        let conn = open_mem();
        ensure_outbox(&conn).unwrap();
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name = '_outbox'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let u = sql.to_ascii_uppercase();
        assert!(u.contains("AUTOINCREMENT"));
        assert!(u.contains("STRICT"));
    }

    #[test]
    fn insert_update_delete_one_row_each() {
        let dir = TempDir::new().unwrap();
        let conn = open_mem();
        conn.execute_batch(
            "CREATE TABLE jobs (
               id INTEGER PRIMARY KEY,
               state TEXT NOT NULL
             ) STRICT;",
        )
        .unwrap();
        let tables = [TableSpec::new("jobs", CaptureMode::Full)];
        let db = dir.path().join("t.sqlite");
        let cfg = cap_cfg("test-jobs", &db, &tables, &dir);
        install(&conn, &cfg).unwrap();

        conn.execute("INSERT INTO jobs(id, state) VALUES (1, 'queued')", [])
            .unwrap();
        conn.execute("UPDATE jobs SET state = 'done' WHERE id = 1", [])
            .unwrap();
        conn.execute("DELETE FROM jobs WHERE id = 1", []).unwrap();

        let rows: Vec<(String, String, String)> = {
            let mut stmt = conn
                .prepare("SELECT tbl, op, key FROM _outbox ORDER BY seq")
                .unwrap();
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, "jobs");
        assert_eq!(rows[0].1, "I");
        assert_eq!(rows[1].1, "U");
        assert_eq!(rows[2].1, "D");
        for (_, _, key) in &rows {
            assert!(key.contains("\"id\""));
            assert!(!key.is_empty());
        }

        let (before, after): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT before, after FROM _outbox WHERE op = 'U'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(before.unwrap().contains("queued"));
        assert!(after.unwrap().contains("done"));
    }

    #[test]
    fn rollback_produces_zero_outbox_rows() {
        let dir = TempDir::new().unwrap();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Full)];
        let db = dir.path().join("t.sqlite");
        install(&conn, &cap_cfg("rollback-test", &db, &tables, &dir)).unwrap();
        let mut conn = conn;
        {
            let tx = conn.transaction().unwrap();
            tx.execute("INSERT INTO t(id, v) VALUES (1, 'x')", [])
                .unwrap();
            tx.rollback().unwrap();
        }
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn reinstall_does_not_reset_outbox_seq() {
        let dir = TempDir::new().unwrap();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Full)];
        let db = dir.path().join("t.sqlite");
        let cfg = cap_cfg("seq-test", &db, &tables, &dir);
        install(&conn, &cfg).unwrap();
        conn.execute("INSERT INTO t(id, v) VALUES (1, 'a')", [])
            .unwrap();
        conn.execute("INSERT INTO t(id, v) VALUES (2, 'b')", [])
            .unwrap();
        let hi: i64 = conn
            .query_row("SELECT MAX(seq) FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(hi, 2);
        install(&conn, &cfg).unwrap();
        conn.execute("INSERT INTO t(id, v) VALUES (3, 'c')", [])
            .unwrap();
        let next: i64 = conn
            .query_row("SELECT MAX(seq) FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(next, 3);
    }

    #[test]
    fn exclude_drops_column_from_payload_keeps_key() {
        let dir = TempDir::new().unwrap();
        let conn = open_mem();
        conn.execute_batch(
            "CREATE TABLE t (
               id INTEGER PRIMARY KEY,
               v TEXT,
               blob TEXT
             ) STRICT;",
        )
        .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Full).exclude(&["blob"])];
        let db = dir.path().join("t.sqlite");
        install(&conn, &cap_cfg("exclude-test", &db, &tables, &dir)).unwrap();
        conn.execute("INSERT INTO t(id, v, blob) VALUES (1, 'ok', 'secret')", [])
            .unwrap();
        let after: String = conn
            .query_row("SELECT after FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert!(after.contains("ok"));
        assert!(!after.contains("secret"));
        assert!(!after.contains("blob"));
    }

    #[test]
    fn after_mode_omits_before_on_update() {
        let dir = TempDir::new().unwrap();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::After)];
        let db = dir.path().join("t.sqlite");
        install(&conn, &cap_cfg("after-test", &db, &tables, &dir)).unwrap();
        conn.execute("INSERT INTO t(id, v) VALUES (1, 'a')", [])
            .unwrap();
        conn.execute("UPDATE t SET v = 'b' WHERE id = 1", [])
            .unwrap();
        let before: Option<String> = conn
            .query_row("SELECT before FROM _outbox WHERE op = 'U'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(before.is_none());
        let after: String = conn
            .query_row("SELECT after FROM _outbox WHERE op = 'U'", [], |r| r.get(0))
            .unwrap();
        assert!(after.contains("b"));
    }

    #[test]
    fn key_mode_payloads_null() {
        let dir = TempDir::new().unwrap();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Key)];
        let db = dir.path().join("t.sqlite");
        install(&conn, &cap_cfg("key-test", &db, &tables, &dir)).unwrap();
        conn.execute("INSERT INTO t(id, v) VALUES (1, 'a')", [])
            .unwrap();
        let (before, after): (Option<String>, Option<String>) = conn
            .query_row("SELECT before, after FROM _outbox", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert!(before.is_none());
        assert!(after.is_none());
        let key: String = conn
            .query_row("SELECT key FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert!(key.contains("\"id\""));
    }

    #[test]
    fn composite_key() {
        let dir = TempDir::new().unwrap();
        let conn = open_mem();
        conn.execute_batch(
            "CREATE TABLE trips (
               icao24 TEXT NOT NULL,
               dep_ts TEXT NOT NULL,
               ticker TEXT,
               PRIMARY KEY (icao24, dep_ts)
             ) STRICT;",
        )
        .unwrap();
        let tables = [TableSpec::new("trips", CaptureMode::Full)];
        let db = dir.path().join("t.sqlite");
        install(&conn, &cap_cfg("trips-test", &db, &tables, &dir)).unwrap();
        conn.execute(
            "INSERT INTO trips(icao24, dep_ts, ticker) VALUES ('abc','2026-09-01T00:00:00Z','X')",
            [],
        )
        .unwrap();
        let key: String = conn
            .query_row("SELECT key FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert!(key.contains("icao24"));
        assert!(key.contains("dep_ts"));
    }

    #[test]
    fn upsert_emits_insert_then_update() {
        let dir = TempDir::new().unwrap();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Full)];
        let db = dir.path().join("t.sqlite");
        install(&conn, &cap_cfg("upsert-test", &db, &tables, &dir)).unwrap();
        conn.execute(
            "INSERT INTO t(id, v) VALUES (1, 'a')
             ON CONFLICT(id) DO UPDATE SET v = excluded.v",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO t(id, v) VALUES (1, 'b')
             ON CONFLICT(id) DO UPDATE SET v = excluded.v",
            [],
        )
        .unwrap();
        let ops: Vec<String> = {
            let mut stmt = conn.prepare("SELECT op FROM _outbox ORDER BY seq").unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(ops, vec!["I", "U"]);
    }

    #[test]
    fn replace_is_two_inserts_no_delete() {
        let dir = TempDir::new().unwrap();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Full)];
        let db = dir.path().join("t.sqlite");
        install(&conn, &cap_cfg("replace-test", &db, &tables, &dir)).unwrap();
        conn.execute("INSERT INTO t(id, v) VALUES (1, 'a')", [])
            .unwrap();
        conn.execute("INSERT OR REPLACE INTO t(id, v) VALUES (1, 'b')", [])
            .unwrap();
        let ops: Vec<String> = {
            let mut stmt = conn.prepare("SELECT op FROM _outbox ORDER BY seq").unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(ops, vec!["I", "I"]);
    }

    #[test]
    fn table_is_strict_detects() {
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE a (id INTEGER PRIMARY KEY) STRICT;")
            .unwrap();
        conn.execute_batch("CREATE TABLE b (id INTEGER PRIMARY KEY);")
            .unwrap();
        assert!(table_is_strict(&conn, "a").unwrap());
        assert!(!table_is_strict(&conn, "b").unwrap());
        assert!(!table_is_strict(&conn, "missing").unwrap());
    }

    #[test]
    fn announce_file_written() {
        let dir = TempDir::new().unwrap();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Key)];
        let db = dir.path().join("feed.sqlite");
        std::fs::write(&db, b"").unwrap();
        install(&conn, &cap_cfg("announce-test", &db, &tables, &dir)).unwrap();
        let p = dir.path().join("announce-test.json");
        let body = std::fs::read_to_string(p).unwrap();
        assert!(body.contains("announce-test"));
        assert!(body.contains("feed.sqlite"));
    }

    #[test]
    fn validate_db_name_rejects_slash() {
        assert!(validate_db_name("adsb-trip-journal").is_ok());
        assert!(validate_db_name("no/slash").is_err());
    }

    #[test]
    fn pragmas_wal() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("p.sqlite");
        let conn = Connection::open(&path).unwrap();
        apply_runtime_pragmas(&conn).unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode.to_ascii_lowercase(), "wal");
    }
}
