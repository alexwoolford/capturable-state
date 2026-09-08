use std::path::{Path, PathBuf};

pub const ENV_SOCK: &str = "STATE_CAPTURE_SOCK";
pub const DEFAULT_SOCK: &str = "/run/state/collect.sock";

/// Fire-and-forget datagram after commit. Errors are ignored.
#[derive(Debug, Clone)]
pub struct Nudge {
    db_name: String,
    sock: PathBuf,
}

impl Nudge {
    pub fn new(db_name: impl Into<String>, sock: Option<&Path>) -> Self {
        let sock = sock
            .map(PathBuf::from)
            .or_else(|| std::env::var(ENV_SOCK).ok().map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from(DEFAULT_SOCK));
        Self {
            db_name: db_name.into(),
            sock,
        }
    }

    /// Send `db_name` bytes. Never fails visibly — collector may be absent.
    pub fn send(&self) {
        #[cfg(unix)]
        {
            use std::os::unix::net::UnixDatagram;
            if let Ok(sock) = UnixDatagram::unbound() {
                let _ = sock.send_to(self.db_name.as_bytes(), &self.sock);
            }
        }
        #[cfg(not(unix))]
        {
            let _ = &self.sock;
        }
    }
}
