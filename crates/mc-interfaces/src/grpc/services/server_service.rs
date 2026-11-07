use std::sync::Arc;

use tokio::sync::Notify;
use tonic::{Request, Response, Status};

use crate::grpc::proto::{
    GetServerInfoRequest, GetServerInfoResponse, RestartRequest, RestartResponse,
    server_service_server::ServerService,
};

/// Exit status of a requested restart: sysexits EX_TEMPFAIL, non-zero so every
/// supervisor's restart-on-failure policy relaunches the server.
pub const RESTART_EXIT_CODE: i32 = 75;

pub struct ServerServiceImpl {
    started_at: i64,
    restart: Arc<Notify>,
}

impl ServerServiceImpl {
    /// `restart` is notified once a client asks for a restart; the caller owns
    /// the graceful shutdown and the exit code.
    pub fn new(restart: Arc<Notify>) -> Self {
        Self {
            started_at: chrono::Utc::now().timestamp(),
            restart,
        }
    }

    fn info(&self) -> std::io::Result<GetServerInfoResponse> {
        Ok(GetServerInfoResponse {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            executable_path: std::env::current_exe()?.display().to_string(),
            process_id: std::process::id(),
            started_at: self.started_at,
        })
    }
}

#[tonic::async_trait]
impl ServerService for ServerServiceImpl {
    async fn get_server_info(
        &self,
        _request: Request<GetServerInfoRequest>,
    ) -> Result<Response<GetServerInfoResponse>, Status> {
        self.info()
            .map(Response::new)
            .map_err(|e| Status::internal(format!("cannot locate the server executable: {e}")))
    }

    async fn restart(
        &self,
        _request: Request<RestartRequest>,
    ) -> Result<Response<RestartResponse>, Status> {
        // notify_one stores a permit, so the waiter cannot miss it; the graceful
        // shutdown then lets this very call finish before the server goes down.
        self.restart.notify_one();
        Ok(Response::new(RestartResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reports_this_build_and_restart_notifies() {
        let restart = Arc::new(Notify::new());
        let svc = ServerServiceImpl::new(restart.clone());

        let info = svc
            .get_server_info(Request::new(GetServerInfoRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(info.process_id, std::process::id());
        assert!(info.started_at > 0);

        svc.restart(Request::new(RestartRequest {})).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), restart.notified())
            .await
            .expect("restart must notify");
    }
}
