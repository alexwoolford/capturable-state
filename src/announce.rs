use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;

pub const ENV_ANNOUNCE_DIR: &str = "STATE_CAPTURE_ANNOUNCE_DIR";
pub const DEFAULT_ANNOUNCE_DIR: &str = "/var/lib/state-capture/announce";

#[derive(Serialize)]
struct AnnounceFile<'a> {
    db_name: &'a str,
    sqlite_path: String,
}

pub fn announce_path(db_name: &str) -> PathBuf {
    let dir = std::env::var(ENV_ANNOUNCE_DIR).unwrap_or_else(|_| DEFAULT_ANNOUNCE_DIR.to_string());
    PathBuf::from(dir).join(format!("{db_name}.json"))
}

/// Write `{db_name, sqlite_path}`. Falls back to `{sqlite_dir}/.capturable.json`.
pub fn announce(db_name: &str, sqlite_path: &Path, announce_dir: Option<&Path>) -> Result<PathBuf> {
    let abs = std::fs::canonicalize(sqlite_path)
        .unwrap_or_else(|_| sqlite_path.to_path_buf())
        .display()
        .to_string();
    let body = serde_json::to_string_pretty(&AnnounceFile {
        db_name,
        sqlite_path: abs,
    })?;
    let primary = match announce_dir {
        Some(dir) => dir.join(format!("{db_name}.json")),
        None => announce_path(db_name),
    };
    if let Some(parent) = primary.parent() {
        if fs::create_dir_all(parent).is_ok() && fs::write(&primary, &body).is_ok() {
            return Ok(primary);
        }
    }
    let fallback = sqlite_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".capturable.json");
    if let Some(parent) = fallback.parent() {
        fs::create_dir_all(parent).ok();
    }
    fs::write(&fallback, body).with_context(|| format!("write {}", fallback.display()))?;
    Ok(fallback)
}
