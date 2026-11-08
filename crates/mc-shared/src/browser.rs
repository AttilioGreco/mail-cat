//! Open a URL in the OS default browser.
//!
//! Needed by every front-end that drives an OAuth login (CLI, desktop client)
//! and by the server's re-auth notification action. Best-effort: callers should
//! also surface the URL so the user can open it manually when there is no
//! desktop session.

/// Hand `url` to the platform's URL opener.
pub fn open_browser(url: &str) -> std::io::Result<()> {
    // The Windows opener would also launch local files or other schemes, and
    // URLs can come from untrusted calendar invites.
    let is_http = |p: &str| {
        url.get(..p.len())
            .is_some_and(|s| s.eq_ignore_ascii_case(p))
    };
    if !is_http("http://") && !is_http("https://") {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "refusing to open a non-http(s) URL",
        ));
    }
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
        // Not `cmd /C start`: cmd re-parses the URL, so `&`/`|`/`^` would run commands.
        // rundll32 receives it as one argument and never goes through a shell.
        std::process::Command::new("rundll32")
            .args(["url.dll,FileProtocolHandler", url])
            .status()?;
    }
    Ok(())
}
