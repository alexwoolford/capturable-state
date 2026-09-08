use anyhow::{Context, Result};
use rusqlite::Connection;

pub const OUTBOX_TABLE: &str = "_outbox";

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
    Ok(())
}
