//! Where the server listens and the clients connect.

use std::path::PathBuf;

/// Unix socket on Linux/macOS, named pipe on Windows. Local-only by construction:
/// no TCP port is ever opened.
pub fn default_socket_path() -> PathBuf {
    #[cfg(windows)]
    {
        // Per-user so two logged-in users don't collide on one pipe name.
        let user = std::env::var("USERNAME").unwrap_or_default();
        PathBuf::from(format!(r"\\.\pipe\mail-cat-{user}"))
    }
    #[cfg(target_os = "linux")]
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(dir).join("mail-cat.sock");
    }
    #[cfg(unix)]
    {
        // macOS has no runtime dir; the data dir keeps us under its 104-byte sun_path limit.
        dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("mail-cat")
            .join("mail-cat.sock")
    }
}
