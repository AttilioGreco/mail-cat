//! LaunchAgent in the user's GUI domain.
#![cfg_attr(not(target_os = "macos"), allow(dead_code))]

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, bail};

use super::{Status, capture, check};

const LABEL: &str = "com.mailcat.server";

fn home() -> anyhow::Result<PathBuf> {
    dirs::home_dir().context("cannot locate the home directory")
}

fn plist_path() -> anyhow::Result<PathBuf> {
    Ok(home()?
        .join("Library/LaunchAgents")
        .join(format!("{LABEL}.plist")))
}

fn domain() -> anyhow::Result<String> {
    let uid = check(Command::new("id").arg("-u"))?;
    Ok(format!("gui/{}", uid.trim()))
}

fn launchctl(args: &[&str]) -> anyhow::Result<String> {
    check(Command::new("launchctl").args(args))
}

fn is_loaded(domain: &str) -> anyhow::Result<bool> {
    Ok(
        capture(Command::new("launchctl").args(["print", &format!("{domain}/{LABEL}")]))?
            .status
            .success(),
    )
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn plist_text(exe: &Path, log_dir: &Path) -> String {
    let exe = xml_escape(&exe.display().to_string());
    let log = xml_escape(&log_dir.join("server.log").display().to_string());
    // KeepAlive/SuccessfulExit=false restarts only after a crash (the systemd
    // `on-failure` equivalent); a clean exit, e.g. port already in use, stays down.
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>{LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
    </array>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key><false/>
    </dict>
    <key>ThrottleInterval</key><integer>5</integer>
    <key>StandardOutPath</key><string>{log}</string>
    <key>StandardErrorPath</key><string>{log}</string>
</dict>
</plist>
"#
    )
}

fn parse_plist_exe(plist: &str) -> Option<String> {
    let after = plist.split_once("<key>ProgramArguments</key>")?.1;
    let exe = after.split_once("<string>")?.1.split_once("</string>")?.0;
    Some(
        exe.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&amp;", "&"),
    )
}

pub fn install(exe: &Path) -> anyhow::Result<()> {
    refuse_ephemeral(exe)?;
    let plist = plist_path()?;
    let log_dir = home()?.join("Library/Logs/mail-cat");
    std::fs::create_dir_all(&log_dir)?;
    std::fs::create_dir_all(plist.parent().context("plist path has no parent")?)?;
    let domain = domain()?;
    // bootstrap fails on an already loaded job, and a loaded one may run the old binary.
    if is_loaded(&domain)? {
        launchctl(&["bootout", &format!("{domain}/{LABEL}")])?;
    }
    std::fs::write(&plist, plist_text(exe, &log_dir))
        .with_context(|| format!("cannot write {}", plist.display()))?;
    bootstrap_after_bootout(&domain, &plist)
}

/// A disk image mount or App Translocation copy disappears: registering it would
/// leave a LaunchAgent pointing at nothing.
fn is_ephemeral(exe: &Path) -> bool {
    let p = exe.to_string_lossy();
    p.starts_with("/Volumes/") || p.contains("/AppTranslocation/")
}

fn refuse_ephemeral(exe: &Path) -> anyhow::Result<()> {
    if is_ephemeral(exe) {
        bail!(
            "{} runs from a disk image or a translocated copy; move mail-cat to the Applications folder and reopen it",
            exe.display()
        );
    }
    Ok(())
}

/// `bootout` returns before the job is torn down, so an immediate `bootstrap` can
/// fail with "Input/output error" (5).
// ponytail: 5 x 200 ms ceiling, only retried on that error; poll `launchctl print` if it ever isn't enough.
fn bootstrap_after_bootout(domain: &str, plist: &Path) -> anyhow::Result<()> {
    let args = ["bootstrap", domain, &plist.display().to_string()];
    for _ in 0..4 {
        let out = capture(Command::new("launchctl").args(args))?;
        if out.status.success() {
            return Ok(());
        }
        if !String::from_utf8_lossy(&out.stderr).contains("Input/output error") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    launchctl(&args).map(drop)
}

pub fn uninstall() -> anyhow::Result<()> {
    stop()?;
    let plist = plist_path()?;
    if plist.exists() {
        std::fs::remove_file(plist)?;
    }
    Ok(())
}

pub fn start() -> anyhow::Result<()> {
    let domain = domain()?;
    if is_loaded(&domain)? {
        return Ok(());
    }
    launchctl(&["bootstrap", &domain, &plist_path()?.display().to_string()]).map(drop)
}

/// Unloads the job: with KeepAlive a plain kill would be restarted. The plist
/// stays, so the next login (or `start`) brings the server back.
pub fn stop() -> anyhow::Result<()> {
    let domain = domain()?;
    if !is_loaded(&domain)? {
        return Ok(());
    }
    launchctl(&["bootout", &format!("{domain}/{LABEL}")]).map(drop)
}

pub fn status() -> anyhow::Result<Status> {
    let Ok(plist) = std::fs::read_to_string(plist_path()?) else {
        return Ok(Status {
            exe: None,
            enabled: false,
            running: false,
        });
    };
    let out =
        capture(Command::new("launchctl").args(["print", &format!("{}/{LABEL}", domain()?)]))?;
    Ok(Status {
        exe: parse_plist_exe(&plist),
        // launchd loads every plist in LaunchAgents at login.
        enabled: true,
        running: out.status.success()
            && String::from_utf8_lossy(&out.stdout).contains("state = running"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plist_roundtrips_exe_and_restarts_on_crash() {
        let exe = Path::new("/Applications/Mail & Cat.app/Contents/MacOS/mail-cat-server");
        let t = plist_text(exe, Path::new("/Users/me/Library/Logs/mail-cat"));
        assert!(t.contains("Mail &amp; Cat.app"));
        assert!(t.contains("<key>RunAtLoad</key><true/>"));
        assert!(t.contains("<key>SuccessfulExit</key><false/>"));
        assert!(t.contains("Logs/mail-cat/server.log"));
        assert_eq!(parse_plist_exe(&t).as_deref(), exe.to_str());
    }

    #[test]
    fn dmg_and_translocated_paths_are_ephemeral() {
        assert!(is_ephemeral(Path::new(
            "/Volumes/Mail Cat/Mail Cat.app/Contents/MacOS/x"
        )));
        assert!(is_ephemeral(Path::new(
            "/private/var/folders/a/AppTranslocation/B/d/Mail Cat.app/x"
        )));
        assert!(!is_ephemeral(Path::new(
            "/Applications/Mail Cat.app/Contents/MacOS/x"
        )));
        assert!(refuse_ephemeral(Path::new("/Volumes/x/y")).is_err());
    }
}
