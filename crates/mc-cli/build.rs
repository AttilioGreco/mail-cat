//! Build script – inietta metadati di build come variabili d'ambiente
//! disponibili a compile-time:
//!
//!   BUILD_GIT_SHA   – short commit hash (es. "abc1234")
//!   BUILD_DATE      – data dell'ultimo commit (es. "2026-03-28")
//!   BUILD_RUSTC     – versione del compilatore (es. "rustc 1.87.0 (...)")

use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    emit_git_rerun_triggers();

    let sha = git(&["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let date = git(&["log", "-1", "--format=%cd", "--date=short"])
        .unwrap_or_else(|| "unknown".into());
    let rustc = rustc_version().unwrap_or_else(|| "unknown".into());

    println!("cargo:rustc-env=BUILD_GIT_SHA={sha}");
    println!("cargo:rustc-env=BUILD_DATE={date}");
    println!("cargo:rustc-env=BUILD_RUSTC={rustc}");
}

/// Tell cargo to re-run this script only when the checked-out commit changes.
///
/// `rerun-if-changed` paths resolve against the *crate* root, not the workspace,
/// so a bare ".git/HEAD" points at a file that does not exist here — and cargo
/// re-runs the script on every single build when a declared path is missing.
/// That silently forced a full recompile of this crate on each `cargo build`.
///
/// Emitting nothing at all is the correct fallback outside a git checkout: it
/// pins the build script to its own source, rather than re-running forever.
fn emit_git_rerun_triggers() {
    let Some(git_dir) = find_git_dir() else {
        return;
    };

    let head = git_dir.join("HEAD");
    if !head.is_file() {
        return;
    }
    println!("cargo:rerun-if-changed={}", head.display());

    // HEAD alone only catches branch switches. Also watch the ref it points at
    // so a new commit on the current branch triggers a rebuild. Packed refs have
    // no per-branch file, hence the existence check.
    if let Ok(contents) = std::fs::read_to_string(&head)
        && let Some(reference) = contents.strip_prefix("ref:")
    {
        let ref_path = git_dir.join(reference.trim());
        if ref_path.is_file() {
            println!("cargo:rerun-if-changed={}", ref_path.display());
        }
    }
}

/// Walk up from this crate to the directory holding `.git`.
fn find_git_dir() -> Option<PathBuf> {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")?;
    let mut dir: &Path = Path::new(&manifest);
    loop {
        let candidate = dir.join(".git");
        // A worktree or submodule has `.git` as a file pointing elsewhere; the
        // metadata we inject still resolves via the git CLI, so skip those
        // rather than guessing at the indirection.
        if candidate.is_dir() {
            return Some(candidate);
        }
        dir = dir.parent()?;
    }
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if out.status.success() {
        Some(String::from_utf8_lossy(&out.stdout).trim().to_owned())
    } else {
        None
    }
}

fn rustc_version() -> Option<String> {
    let out = Command::new("rustc").arg("--version").output().ok()?;
    if out.status.success() {
        Some(String::from_utf8_lossy(&out.stdout).trim().to_owned())
    } else {
        None
    }
}
