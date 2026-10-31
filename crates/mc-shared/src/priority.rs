//! OS priority of background threads, so that learning and embedding never
//! make the machine sluggish for whoever is using it.

use std::io;

/// Nice value of background work.
const BACKGROUND_NICE: i32 = 10;

/// Lowers the priority of the calling thread only, and of the threads it
/// creates afterwards (Linux nice is per thread and inherited). It cannot be
/// raised back, so call it on a thread that is not reused for other work.
#[cfg(target_os = "linux")]
pub fn lower_thread_priority() -> io::Result<()> {
    // SAFETY: gettid and setpriority take and return plain integers.
    let rc = unsafe {
        let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
        libc::setpriority(libc::PRIO_PROCESS, tid, BACKGROUND_NICE)
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// macOS: nice is process-wide there, so the thread takes the utility QoS
/// class (throttled CPU and I/O, still progressing) instead.
#[cfg(target_os = "macos")]
pub fn lower_thread_priority() -> io::Result<()> {
    let _ = BACKGROUND_NICE;
    // SAFETY: plain integer arguments.
    match unsafe { libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_UTILITY, 0) } {
        0 => Ok(()),
        code => Err(io::Error::from_raw_os_error(code)),
    }
}

/// Windows: background mode lowers the thread's CPU, I/O and memory priority.
// ponytail: Windows does not hand thread priority down to threads created
// later, so ONNX Runtime's intra-op pool keeps normal priority there. ort's
// `ThreadManager` (SessionBuilder::with_thread_manager) could set it per pool
// thread, but fastembed does not expose the session builder; until then
// Leggera (one thread, no pool) is the quiet choice on Windows.
#[cfg(windows)]
pub fn lower_thread_priority() -> io::Result<()> {
    use windows_sys::Win32::System::Threading::{
        GetCurrentThread, SetThreadPriority, THREAD_MODE_BACKGROUND_BEGIN,
    };
    let _ = BACKGROUND_NICE;
    // SAFETY: the pseudo handle of the calling thread is always valid.
    if unsafe { SetThreadPriority(GetCurrentThread(), THREAD_MODE_BACKGROUND_BEGIN) } != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub fn lower_thread_priority() -> io::Result<()> {
    let _ = BACKGROUND_NICE;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn lowering_the_priority_of_a_thread_succeeds() {
        std::thread::spawn(|| super::lower_thread_priority().unwrap())
            .join()
            .unwrap();
    }
}
