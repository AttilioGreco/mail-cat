//! Local-socket listener: Unix domain socket, or a named pipe on Windows.

use std::path::Path;

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
    Ok(Some(tokio_stream::wrappers::UnixListenerStream::new(
        listener,
    )))
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

    pub fn listen(
        path: &Path,
    ) -> anyhow::Result<Option<impl Stream<Item = io::Result<Pipe>> + use<>>> {
        // first_pipe_instance fails when any other process already serves this
        // name: that is how a second server instance is detected.
        let first = match ServerOptions::new().first_pipe_instance(true).create(path) {
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
                        ServerOptions::new().create(&path)?,
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
