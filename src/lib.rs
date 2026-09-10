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
    announce(cfg.db_name, cfg.sqlite_path, cfg.announce_dir)?;
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
    use std::sync::Mutex;
    use tempfile::TempDir;

    static ENV: Mutex<()> = Mutex::new(());

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

    #[test]
    fn leftover_capture_triggers_are_dropped() {
        let dir = TempDir::new().unwrap();
        let conn = open_mem();
        conn.execute_batch(
            "CREATE TABLE stay (id INTEGER PRIMARY KEY, v TEXT) STRICT;
             CREATE TABLE gone (id INTEGER PRIMARY KEY, v TEXT) STRICT;",
        )
        .unwrap();
        let db = dir.path().join("t.sqlite");
        let both = [
            TableSpec::new("stay", CaptureMode::After),
            TableSpec::new("gone", CaptureMode::After),
        ];
        install(&conn, &cap_cfg("orphan-test", &db, &both, &dir)).unwrap();
        conn.execute("INSERT INTO gone(id, v) VALUES (1, 'x')", [])
            .unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM _outbox WHERE tbl = 'gone'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(n, 1);

        let stay_only = [TableSpec::new("stay", CaptureMode::After)];
        install(&conn, &cap_cfg("orphan-test", &db, &stay_only, &dir)).unwrap();
        conn.execute("INSERT INTO gone(id, v) VALUES (2, 'y')", [])
            .unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM _outbox WHERE tbl = 'gone'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(n, 1);
        conn.execute("INSERT INTO stay(id, v) VALUES (1, 'z')", [])
            .unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM _outbox WHERE tbl = 'stay'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn exclude_prefix_of_another_column_still_asserts() {
        let dir = TempDir::new().unwrap();
        let conn = open_mem();
        conn.execute_batch(
            "CREATE TABLE t (
               id INTEGER PRIMARY KEY,
               v TEXT,
               valid_from TEXT
             ) STRICT;",
        )
        .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Full).exclude(&["v"])];
        let db = dir.path().join("t.sqlite");
        install(&conn, &cap_cfg("exclude-prefix", &db, &tables, &dir)).unwrap();
        conn.execute(
            "INSERT INTO t(id, v, valid_from) VALUES (1, 'secret', '2026-09-01')",
            [],
        )
        .unwrap();
        let after: String = conn
            .query_row("SELECT after FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert!(after.contains("valid_from"));
        assert!(after.contains("2026-09-01"));
        assert!(!after.contains("secret"));
        let obj: serde_json::Value = serde_json::from_str(&after).unwrap();
        assert!(obj.get("v").is_none());
    }

    #[test]
    fn ensure_outbox_rejects_non_autoincrement() {
        let conn = open_mem();
        conn.execute_batch(
            "CREATE TABLE _outbox (
               seq INTEGER PRIMARY KEY,
               tbl TEXT NOT NULL,
               op TEXT NOT NULL,
               ts INTEGER NOT NULL,
               key TEXT NOT NULL,
               before TEXT,
               after TEXT
             ) STRICT;",
        )
        .unwrap();
        let err = ensure_outbox(&conn).unwrap_err();
        assert!(err.to_string().contains("AUTOINCREMENT"), "{err}");
    }

    #[test]
    fn announce_skips_when_default_parent_cannot_be_created() {
        let _g = ENV.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let blocker = dir.path().join("not-a-dir");
        std::fs::write(&blocker, b"x").unwrap();
        let nested = blocker.join("announce");
        let prev = std::env::var(ENV_ANNOUNCE_DIR).ok();
        std::env::set_var(ENV_ANNOUNCE_DIR, &nested);
        let sqlite = dir.path().join("t.sqlite");
        std::fs::write(&sqlite, b"").unwrap();
        let got = announce("skip-test", &sqlite, None).unwrap();
        match prev {
            Some(v) => std::env::set_var(ENV_ANNOUNCE_DIR, v),
            None => std::env::remove_var(ENV_ANNOUNCE_DIR),
        }
        assert!(got.is_none());
        assert!(!sqlite.parent().unwrap().join(".capturable.json").exists());
    }

    #[cfg(unix)]
    #[test]
    fn announce_fails_if_dir_exists_but_unwritable() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let sqlite = dir.path().join("t.sqlite");
        std::fs::write(&sqlite, b"").unwrap();
        let ann = dir.path().join("announce");
        std::fs::create_dir(&ann).unwrap();
        let mut perms = std::fs::metadata(&ann).unwrap().permissions();
        perms.set_mode(0o555);
        std::fs::set_permissions(&ann, perms).unwrap();
        let err = announce("no-write", &sqlite, Some(&ann));
        let mut restore = std::fs::metadata(&ann).unwrap().permissions();
        restore.set_mode(0o755);
        std::fs::set_permissions(&ann, restore).unwrap();
        assert!(err.is_err(), "expected write failure, got {err:?}");
    }
}
