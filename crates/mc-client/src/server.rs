//! The server process itself: which build answers, and asking it to restart.

use crate::client::MailCatClient;
use crate::error::Result;
use crate::proto::mail_cat::v1::{self as pb, server_service_client::ServerServiceClient};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerInfo {
    pub version: String,
    pub executable_path: String,
    pub process_id: u32,
    /// Unix seconds.
    pub started_at: i64,
}

impl MailCatClient {
    /// `None` for a server older than the lifecycle service.
    pub async fn server_info(&self) -> Result<Option<ServerInfo>> {
        let resp = match ServerServiceClient::new(self.channel())
            .get_server_info(pb::GetServerInfoRequest {})
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(s) if s.code() == tonic::Code::Unimplemented => return Ok(None),
            Err(s) => return Err(s.into()),
        };
        Ok(Some(ServerInfo {
            version: resp.version,
            executable_path: resp.executable_path,
            process_id: resp.process_id,
            started_at: resp.started_at,
        }))
    }

    /// The server replies, then exits for its supervisor to relaunch it.
    pub async fn restart_server(&self) -> Result<()> {
        ServerServiceClient::new(self.channel())
            .restart(pb::RestartRequest {})
            .await?;
        Ok(())
    }
}
