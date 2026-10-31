//! The process's own memory use, for estimates shown to the user.

/// Resident set size in bytes; `None` where it cannot be read cheaply.
// ponytail: Linux only (/proc); macOS and Windows show no memory estimate.
pub fn resident_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * 4096)
}

/// Hands freed heap back to the OS. glibc keeps freed arenas mapped, so after
/// a model is dropped the process would otherwise stay at its peak size.
pub fn release_free_memory() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    // SAFETY: malloc_trim only walks the allocator's own free lists.
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    #[test]
    fn the_test_process_has_resident_memory() {
        assert!(super::resident_bytes().unwrap() > 0);
    }
}
