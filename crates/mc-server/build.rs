//! Build script – inietta git SHA e data di build come variabili d'ambiente
//! disponibili a compile-time via `env!("BUILD_GIT_SHA")` e `env!("BUILD_DATE")`.

use std::process::Command;

fn main() {
    // Riesegui se il ref HEAD o i ref dei branch cambiano (nuovo commit / checkout).
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads/");

    let sha = git(&["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let date = git(&["log", "-1", "--format=%cd", "--date=short"])
        .unwrap_or_else(|| "unknown".into());

    println!("cargo:rustc-env=BUILD_GIT_SHA={sha}");
    println!("cargo:rustc-env=BUILD_DATE={date}");
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if out.status.success() {
        Some(String::from_utf8_lossy(&out.stdout).trim().to_owned())
    } else {
        None
    }
}
