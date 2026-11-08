//! Local-socket listener: Unix domain socket, or a named pipe on Windows.

#[cfg(unix)]
use std::path::Path;

#[cfg(unix)]
use futures::Stream;

/// Bind the socket, or `None` when another server instance already owns it.
#[cfg(unix)]
pub fn listen(
    path: &Path,
) -> anyhow::Result<Option<impl Stream<Item = std::io::Result<tokio::net::UnixStream>> + use<>>> {
    use anyhow::Context;
    use std::os::unix::fs::{DirBuilderExt, FileTypeExt, PermissionsExt};
    use tokio::net::UnixListener;

    if let Some(dir) = path.parent() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .context("failed to create socket directory")?;
    }
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        anyhow::ensure!(
            meta.file_type().is_socket(),
            "{} exists and is not a socket",
            path.display()
        );
        // Connecting tells a live server from the leftover of a crash.
        if std::os::unix::net::UnixStream::connect(path).is_ok() {
            return Ok(None);
        }
        std::fs::remove_file(path).context("failed to remove stale socket")?;
    }
    let listener = UnixListener::bind(path).context("failed to bind socket")?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .context("failed to restrict socket permissions")?;
    Ok(Some(only_user(
        tokio_stream::wrappers::UnixListenerStream::new(listener),
        mc_shared::ipc::current_uid(),
    )))
}

/// The socket mode is not the only barrier: BSD/macOS may ignore it, and until
/// the chmod above the socket has the umask's mode. Checking the peer's uid
/// makes a loose directory or umask harmless.
#[cfg(unix)]
fn only_user(
    incoming: impl Stream<Item = std::io::Result<tokio::net::UnixStream>>,
    uid: u32,
) -> impl Stream<Item = std::io::Result<tokio::net::UnixStream>> {
    use futures::StreamExt;

    incoming.filter(move |conn| {
        let allowed = conn
            .as_ref()
            .map_or(true, |s| s.peer_cred().is_ok_and(|c| c.uid() == uid));
        if !allowed {
            tracing::warn!("rejected a socket connection from another user");
        }
        std::future::ready(allowed)
    })
}

#[cfg(unix)]
pub fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
}

#[cfg(windows)]
pub use windows::{cleanup, listen};

#[cfg(windows)]
mod windows {
    use std::io;
    use std::path::Path;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use anyhow::Context as _;
    use futures::Stream;
    use mc_shared::ipc::windows::OwnerOnly;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
    use tonic::transport::server::Connected;

    pub struct Pipe(NamedPipeServer);

    impl Connected for Pipe {
        type ConnectInfo = ();
        fn connect_info(&self) -> Self::ConnectInfo {}
    }

    impl AsyncRead for Pipe {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for Pipe {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }
        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }

    /// The default DACL lets Everyone and Anonymous open the pipe for reading,
    /// so every instance gets an explicit owner-only one.
    fn create(options: &mut ServerOptions, path: &Path) -> io::Result<NamedPipeServer> {
        let mut owner_only = OwnerOnly::new()?;
        // SAFETY: the pointer is a valid SECURITY_ATTRIBUTES that outlives the
        // call; CreateNamedPipe copies the descriptor.
        unsafe { options.create_with_security_attributes_raw(path, owner_only.as_mut_ptr()) }
    }

    pub fn listen(
        path: &Path,
    ) -> anyhow::Result<Option<impl Stream<Item = io::Result<Pipe>> + use<>>> {
        // first_pipe_instance fails when any other process already serves this
        // name: that is how a second server instance is detected.
        let first = match create(ServerOptions::new().first_pipe_instance(true), path) {
            Ok(server) => server,
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => return Ok(None),
            Err(e) => return Err(e).context("failed to create named pipe"),
        };
        let path = path.to_owned();
        Ok(Some(futures::stream::unfold(first, move |mut server| {
            let path = path.clone();
            async move {
                // The next instance must exist before this one is handed off,
                // or a client connecting in between finds no pipe.
                let conn = async {
                    server.connect().await?;
                    Ok(Pipe(std::mem::replace(
                        &mut server,
                        create(&mut ServerOptions::new(), &path)?,
                    )))
                }
                .await;
                Some((conn, server))
            }
        })))
    }

    pub fn cleanup(_: &Path) {}
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn only_the_owning_user_is_accepted() {
        use futures::StreamExt;
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uid.sock");
        let me = mc_shared::ipc::current_uid();
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let mut allowed = Box::pin(only_user(
            tokio_stream::wrappers::UnixListenerStream::new(listener),
            me,
        ));
        tokio::net::UnixStream::connect(&path).await.unwrap();
        assert!(allowed.next().await.unwrap().is_ok());

        let listener = tokio::net::UnixListener::bind(dir.path().join("other.sock")).unwrap();
        let mut denied = Box::pin(only_user(
            tokio_stream::wrappers::UnixListenerStream::new(listener),
            me + 1,
        ));
        tokio::net::UnixStream::connect(dir.path().join("other.sock"))
            .await
            .unwrap();
        let next = tokio::time::timeout(Duration::from_millis(200), denied.next()).await;
        assert!(next.is_err(), "a foreign uid must never reach the server");
    }

    #[test]
    fn a_regular_file_at_the_socket_path_is_not_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mail-cat.sock");
        std::fs::write(&path, "precious").unwrap();

        assert!(listen(&path).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "precious");
    }

    #[tokio::test]
    async fn client_reaches_server_over_socket_and_stale_file_is_replaced() {
        use mc_interfaces::grpc::proto::server_service_server::ServerServiceServer;
        use mc_interfaces::grpc::services::ServerServiceImpl;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run").join("test.sock");

        // A leftover file nobody listens on is replaced.
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
        let incoming = listen(&path)
            .unwrap()
            .expect("stale socket must be replaced");
        // A live listener makes a second instance back off.
        assert!(listen(&path).unwrap().is_none());

        let service = ServerServiceServer::new(ServerServiceImpl::new(Arc::new(Notify::new())));
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming),
        );

        let client = mc_client::MailCatClient::connect(&path).await.unwrap();
        assert!(client.server_info().await.unwrap().is_some());
    }
}
