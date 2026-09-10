use anyhow::{bail, Context, Result};
use rusqlite::Connection;

pub const OUTBOX_TABLE: &str = "_outbox";

const OUTBOX_COLS: &[&str] = &["seq", "tbl", "op", "ts", "key", "before", "after"];

/// Canonical DDL. Do not customize. Never DROP this table.
pub const OUTBOX_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS _outbox (
  seq    INTEGER PRIMARY KEY AUTOINCREMENT,
  tbl    TEXT    NOT NULL,
  op     TEXT    NOT NULL CHECK (op IN ('I','U','D')),
  ts     INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  key    TEXT    NOT NULL,
  before TEXT,
  after  TEXT
) STRICT;
"#;

pub fn ensure_outbox(conn: &Connection) -> Result<()> {
    conn.execute_batch(OUTBOX_DDL).context("create _outbox")?;
    assert_outbox_ddl(conn)
}

fn assert_outbox_ddl(conn: &Connection) -> Result<()> {
    let sql: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [OUTBOX_TABLE],
            |row| row.get(0),
        )
        .context("_outbox missing after CREATE")?;
    let u = sql.to_ascii_uppercase();
    if !u.contains("AUTOINCREMENT") {
        bail!("_outbox must use INTEGER PRIMARY KEY AUTOINCREMENT (do not drop/recreate it)");
    }
    if !u.contains("STRICT") {
        bail!("_outbox must be STRICT");
    }

    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info(\"{OUTBOX_TABLE}\")"))
        .context("table_info _outbox")?;
    let cols: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<_>>()?;
    if cols
        .iter()
        .map(String::as_str)
        .ne(OUTBOX_COLS.iter().copied())
    {
        bail!("_outbox columns {cols:?} do not match {OUTBOX_COLS:?}");
    }
    Ok(())
}
