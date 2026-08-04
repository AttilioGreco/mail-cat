//! Open a URL in the OS default browser.
//!
//! Needed by every front-end that drives an OAuth login (CLI, desktop client)
//! and by the server's re-auth notification action. Best-effort: callers should
//! also surface the URL so the user can open it manually when there is no
//! desktop session.

/// Hand `url` to the platform's URL opener.
pub fn open_browser(url: &str) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        std::process::Command::new("xdg-open").arg(url).status()?;
    }
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open").arg(url).status()?;
    }
    #[cfg(target_os = "windows")]
    {
        std::process::Command::new("cmd")
            .args(["/C", "start", url])
            .status()?;
    }
    Ok(())
}
