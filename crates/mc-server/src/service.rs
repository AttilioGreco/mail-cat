//! `mail-cat-server service <install|uninstall|start|stop|status>`: registers the
//! server to start at login and restart on crash, per user, without admin rights.
//!
//! * Linux: systemd user unit (`mail-cat-server.service`)
//! * macOS: LaunchAgent (`com.mailcat.server`)
//! * Windows: Scheduled Task at logon (not a Windows Service: a service runs in
//!   another session and cannot reach the user's Credential Manager, where the
//!   OAuth tokens live)
//!
//! The registered command is always `std::env::current_exe()`, so it follows the
//! installed bundle wherever it lives. Every subcommand is idempotent.
//!
//! `status` prints `key=value` lines (`registered=yes|no`, `enabled=yes|no`,
//! `running=yes|no`, `exe=<registered executable>` when registered) and exits with
//! 0 = enabled and running, 1 = enabled but not running, 2 = not registered or
//! registered but not enabled at login (a deb/rpm ships its unit disabled), i.e.
//! `install` is needed.
//! Comparing `exe` with the app's own path tells the UI the app was moved or updated.
//! Any other failure exits 3 with the message on stderr.

use std::process::{Command, Output};

use anyhow::{Context, bail};

#[cfg(any(target_os = "linux", test))]
mod linux;
#[cfg(any(target_os = "macos", test))]
mod macos;
#[cfg(any(windows, test))]
mod windows;

#[cfg(target_os = "linux")]
use linux as os;
#[cfg(target_os = "macos")]
use macos as os;
#[cfg(windows)]
use windows as os;

pub const HELP: &str = "\
Usage: mail-cat-server service <COMMAND>

Commands:
  install    register the autostart for the current user and start the server
  uninstall  stop the server and remove the autostart
  start      start the registered server
  stop       stop the registered server
  status     print registered/enabled/running/exe as key=value lines

Exit codes of `status`: 0 = enabled and running, 1 = enabled but not running,
2 = not registered or not enabled at login. Any command exits 3 on error.";

/// What the OS service manager knows about the server.
pub struct Status {
    /// Executable the autostart points to; `None` when not registered.
    pub exe: Option<String>,
    /// Will start at login.
    pub enabled: bool,
    pub running: bool,
}

impl Status {
    fn report(&self) -> (String, i32) {
        let Some(exe) = &self.exe else {
            return ("registered=no\nenabled=no\nrunning=no\n".into(), 2);
        };
        let yn = |b| if b { "yes" } else { "no" };
        let code = match (self.enabled, self.running) {
            (false, _) => 2,
            (true, true) => 0,
            (true, false) => 1,
        };
        (
            format!(
                "registered=yes\nenabled={}\nrunning={}\nexe={exe}\n",
                yn(self.enabled),
                yn(self.running)
            ),
            code,
        )
    }
}

/// Runs a `service` subcommand and returns the process exit code.
pub fn run(args: &[String]) -> i32 {
    match dispatch(args) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("mail-cat-server service: {e:#}");
            3
        }
    }
}

fn dispatch(args: &[String]) -> anyhow::Result<i32> {
    match args.first().map(String::as_str) {
        Some("install") => {
            os::install(&std::env::current_exe().context("cannot locate own executable")?)?
        }
        Some("uninstall") => os::uninstall()?,
        Some("start") => os::start()?,
        Some("stop") => os::stop()?,
        Some("status") => {
            let (text, code) = os::status()?.report();
            print!("{text}");
            return Ok(code);
        }
        Some("--help" | "-h") | None => {
            println!("{HELP}");
            return Ok(0);
        }
        Some(other) => bail!("unknown command '{other}'\n\n{HELP}"),
    }
    Ok(0)
}

/// Runs a tool and returns its output; a non-zero exit becomes an error carrying
/// the tool's own message.
fn check(cmd: &mut Command) -> anyhow::Result<String> {
    let out = capture(cmd)?;
    if !out.status.success() {
        bail!(
            "{:?} failed ({}): {}",
            cmd.get_program(),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Like [`check`] but hands back the raw output so callers can inspect failures.
fn capture(cmd: &mut Command) -> anyhow::Result<Output> {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // The server is a GUI-subsystem binary: without this every child would
        // flash its own console window.
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    cmd.output()
        .with_context(|| format!("cannot run {:?}", cmd.get_program()))
}

#[cfg(any(target_os = "linux", windows, test))]
/// Value of `key` in `key=value` lines.
fn kv<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    text.lines()
        .find_map(|l| l.strip_prefix(key)?.strip_prefix('='))
        .map(str::trim)
}

/// The server is built for the Windows GUI subsystem so the scheduled task does
/// not open a console. When launched from a terminal for `service` or
/// `--version` the std handles are missing, so reattach to the parent console.
#[cfg(windows)]
pub fn attach_parent_console() {
    use std::ffi::c_void;
    use std::os::windows::io::IntoRawHandle;

    unsafe extern "system" {
        fn GetStdHandle(id: u32) -> *mut c_void;
        fn SetStdHandle(id: u32, handle: *mut c_void) -> i32;
        fn AttachConsole(pid: u32) -> i32;
    }
    const STD_OUTPUT: u32 = -11i32 as u32;
    const STD_ERROR: u32 = -12i32 as u32;
    const ATTACH_PARENT_PROCESS: u32 = u32::MAX;

    // SAFETY: plain Win32 calls with valid arguments; the opened handle is
    // leaked on purpose so it stays valid as stdout/stderr for the whole process.
    unsafe {
        // A redirected stdout (pipe/file, e.g. the UI capturing `status`) is
        // already valid and must be left alone.
        if !GetStdHandle(STD_OUTPUT).is_null() || AttachConsole(ATTACH_PARENT_PROCESS) == 0 {
            return;
        }
        let Ok(con) = std::fs::OpenOptions::new().write(true).open("CONOUT$") else {
            return;
        };
        let handle = con.into_raw_handle();
        SetStdHandle(STD_OUTPUT, handle);
        SetStdHandle(STD_ERROR, handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_report_and_exit_codes() {
        let s = |exe: Option<&str>, enabled, running| Status {
            exe: exe.map(Into::into),
            enabled,
            running,
        };
        assert_eq!(
            s(Some("/a/b"), true, true).report(),
            (
                "registered=yes\nenabled=yes\nrunning=yes\nexe=/a/b\n".into(),
                0
            )
        );
        assert_eq!(s(Some("/a/b"), true, false).report().1, 1);
        let disabled = s(Some("/a/b"), false, false).report();
        assert_eq!(disabled.1, 2);
        assert!(disabled.0.contains("enabled=no"));
        assert_eq!(
            s(None, false, false).report(),
            ("registered=no\nenabled=no\nrunning=no\n".into(), 2)
        );
    }

    #[test]
    fn unknown_command_is_an_error_not_a_panic() {
        assert_eq!(run(&["bogus".into()]), 3);
    }

    #[test]
    fn kv_finds_keys() {
        assert_eq!(kv("a=1\nstate= Running \n", "state"), Some("Running"));
        assert_eq!(kv("a=1\n", "b"), None);
    }
}
