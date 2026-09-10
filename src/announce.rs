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

/// Write `{db_name, sqlite_path}` into the collector announce dir.
///
/// `Ok(None)` only when the default/env parent cannot be created (collector
/// absent). An explicit `announce_dir`, or a dir that exists but is not
/// writable, is an error. The collector never reads a sibling sidecar.
pub fn announce(
    db_name: &str,
    sqlite_path: &Path,
    announce_dir: Option<&Path>,
) -> Result<Option<PathBuf>> {
    let abs = std::fs::canonicalize(sqlite_path)
        .unwrap_or_else(|_| sqlite_path.to_path_buf())
        .display()
        .to_string();
    let body = serde_json::to_string_pretty(&AnnounceFile {
        db_name,
        sqlite_path: abs,
    })?;
    let explicit = announce_dir.is_some();
    let primary = match announce_dir {
        Some(dir) => dir.join(format!("{db_name}.json")),
        None => announce_path(db_name),
    };
    let parent = primary.parent().unwrap_or_else(|| Path::new("."));
    match fs::create_dir_all(parent) {
        Ok(()) => {}
        Err(_) if !explicit => {
            // Parent path cannot be created (permissions, file in the way, …).
            return Ok(None);
        }
        Err(e) => {
            return Err(e).with_context(|| format!("create announce dir {}", parent.display()));
        }
    }
    fs::write(&primary, &body).with_context(|| format!("write {}", primary.display()))?;
    Ok(Some(primary))
}
