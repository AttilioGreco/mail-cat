use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("failed to connect to server at {endpoint}: {reason}")]
    Connection { endpoint: String, reason: String },

    #[error("gRPC request failed: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("server returned incomplete data: {0}")]
    InvalidResponse(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;
