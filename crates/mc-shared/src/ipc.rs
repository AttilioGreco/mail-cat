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

/// Effective uid. Socket peers are trusted only when theirs matches: file
/// permissions alone are not enforced on every Unix and are easy to loosen.
#[cfg(unix)]
pub fn current_uid() -> u32 {
    // SAFETY: geteuid has no preconditions and cannot fail.
    unsafe { libc::geteuid() }
}

#[cfg(windows)]
pub mod windows {
    use std::ffi::c_void;
    use std::io;
    use std::os::windows::io::AsRawHandle;
    use std::ptr;

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
        SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{
        EqualSid, GetTokenInformation, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId;
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, OpenProcess, OpenProcessToken, PROCESS_QUERY_INFORMATION,
    };

    /// Lets a pipe server learn who the client is by impersonating it; the
    /// client opens with this so a squatted pipe cannot borrow its identity.
    pub use windows_sys::Win32::Storage::FileSystem::SECURITY_IDENTIFICATION;

    struct Handle(HANDLE);

    impl Drop for Handle {
        fn drop(&mut self) {
            // SAFETY: only built from handles this module owns.
            unsafe { CloseHandle(self.0) };
        }
    }

    /// TOKEN_USER plus the SID it points into. 128 bytes cover
    /// SECURITY_MAX_SID_SIZE (68); u64 elements give the pointer alignment.
    /// Boxed because the embedded SID pointer must survive moving `User`.
    struct User(Box<[u64; 16]>);

    impl User {
        fn sid(&self) -> *mut c_void {
            // SAFETY: filled by GetTokenInformation(TokenUser) in `token_user`.
            unsafe { (*self.0.as_ptr().cast::<TOKEN_USER>()).User.Sid }
        }
    }

    fn token_user(process: HANDLE) -> io::Result<User> {
        let mut token = ptr::null_mut();
        // SAFETY: plain out-pointer call; the handle is closed by the guard.
        if unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut token) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let token = Handle(token);
        let mut user = User(Box::new([0; 16]));
        let mut len = size_of::<[u64; 16]>() as u32;
        // SAFETY: the buffer is `len` bytes long.
        let ok = unsafe {
            GetTokenInformation(
                token.0,
                TokenUser,
                user.0.as_mut_ptr().cast(),
                len,
                &mut len,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(user)
    }

    /// Security attributes whose DACL grants access to the current user only
    /// (nothing for Everyone, Anonymous or other users). The default pipe DACL
    /// is readable by both.
    pub struct OwnerOnly(SECURITY_ATTRIBUTES);

    impl OwnerOnly {
        pub fn new() -> io::Result<Self> {
            // SAFETY: GetCurrentProcess is a pseudo-handle, never closed.
            let user = token_user(unsafe { GetCurrentProcess() })?;
            let mut sid = ptr::null_mut();
            // SAFETY: `user` holds a valid SID; the string is freed below.
            if unsafe { ConvertSidToStringSidW(user.sid(), &mut sid) } == 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: NUL-terminated string allocated by the call above.
            let sid_text = unsafe {
                let len = (0..).take_while(|&i| *sid.add(i) != 0).count();
                let text = String::from_utf16_lossy(std::slice::from_raw_parts(sid, len));
                LocalFree(sid.cast());
                text
            };
            // Protected DACL (no inherited ACEs), generic-all for that SID only.
            let sddl: Vec<u16> = format!("D:P(A;;GA;;;{sid_text})\0")
                .encode_utf16()
                .collect();
            let mut descriptor = ptr::null_mut();
            // SAFETY: `sddl` is NUL-terminated; the descriptor is freed in Drop.
            let ok = unsafe {
                ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    sddl.as_ptr(),
                    SDDL_REVISION_1,
                    &mut descriptor,
                    ptr::null_mut(),
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(Self(SECURITY_ATTRIBUTES {
                nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
                lpSecurityDescriptor: descriptor,
                bInheritHandle: 0,
            }))
        }

        /// For `ServerOptions::create_with_security_attributes_raw`; valid while `self` lives.
        pub fn as_mut_ptr(&mut self) -> *mut c_void {
            ptr::from_mut(&mut self.0).cast()
        }
    }

    impl Drop for OwnerOnly {
        fn drop(&mut self) {
            // SAFETY: allocated by ConvertStringSecurityDescriptorToSecurityDescriptorW.
            unsafe { LocalFree(self.0.lpSecurityDescriptor) };
        }
    }

    /// Whether the process serving the connected pipe runs as the current user.
    /// Pipe names are predictable, so another user can create the pipe first.
    /// Fails closed: a process we cannot inspect is not ours.
    pub fn pipe_server_is_current_user(pipe: &impl AsRawHandle) -> io::Result<bool> {
        let mut pid = 0;
        // SAFETY: the handle is owned by `pipe`, which outlives the call.
        if unsafe { GetNamedPipeServerProcessId(pipe.as_raw_handle(), &mut pid) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: plain call; a null result is handled.
        let process = unsafe { OpenProcess(PROCESS_QUERY_INFORMATION, 0, pid) };
        if process.is_null() {
            return Err(io::Error::last_os_error());
        }
        let process = Handle(process);
        let theirs = token_user(process.0)?;
        // SAFETY: GetCurrentProcess is a pseudo-handle, never closed.
        let ours = token_user(unsafe { GetCurrentProcess() })?;
        // SAFETY: both SIDs live in the buffers above.
        Ok(unsafe { EqualSid(ours.sid(), theirs.sid()) } != 0)
    }
}
