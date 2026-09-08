use anyhow::{Context, Result};
use rusqlite::Connection;

use crate::open_timeout;

/// WAL, synchronous=NORMAL, busy_timeout=5000, foreign_keys=ON.
pub fn apply_runtime_pragmas(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")
        .context("journal_mode WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .context("synchronous NORMAL")?;
    conn.busy_timeout(open_timeout()).context("busy_timeout")?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .context("foreign_keys ON")?;
    Ok(())
}
