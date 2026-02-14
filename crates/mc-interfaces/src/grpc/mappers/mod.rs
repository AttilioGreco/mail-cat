//! Mappers between domain types and proto types.

pub mod common;
pub mod account;
pub mod mailbox;
pub mod message;

pub use common::*;
pub use account::*;
pub use mailbox::*;
pub use message::*;

use tonic::Status;
use mc_domain::error::DomainError;
use mc_application::error::ApplicationError;
use mc_shared::pagination::{PagedResult, Pagination as DomainPagination};

/// Convert DomainError to tonic Status.
pub fn domain_error_to_status(error: DomainError) -> Status {
    match error {
        DomainError::AccountNotFound(msg)
        | DomainError::MailboxNotFound(msg)
        | DomainError::MessageNotFound(msg) => {
            Status::not_found(msg)
        }
        DomainError::InvalidEmailAddress(msg)
        | DomainError::InvalidMailboxPath(msg)
        | DomainError::Validation(msg) => {
            Status::invalid_argument(msg)
        }
        DomainError::AuthenticationFailed(msg) => {
            Status::unauthenticated(msg)
        }
        DomainError::ConcurrencyConflict { expected, actual } => {
            Status::aborted(format!("Concurrency conflict: expected {}, found {}", expected, actual))
        }
        DomainError::Provider(msg)
        | DomainError::ConnectionFailed(msg)
        | DomainError::ConnectionTimeout(msg)
        | DomainError::EventStore(msg)
        | DomainError::Repository(msg)
        | DomainError::SearchIndex(msg)
        | DomainError::SearchQuery(msg)
        | DomainError::Serialization(msg) => {
            Status::internal(msg)
        }
        DomainError::NotSupported(msg) => {
            Status::unimplemented(msg)
        }
    }
}

/// Convert ApplicationError to tonic Status.
pub fn application_error_to_status(error: ApplicationError) -> Status {
    match error {
        ApplicationError::Domain(domain_err) => domain_error_to_status(domain_err),
        ApplicationError::Internal(msg) => Status::internal(msg),
        ApplicationError::NotFound(resource, id) => Status::not_found(format!("{} not found: {}", resource, id)),
        ApplicationError::PreconditionFailed(msg) => Status::failed_precondition(msg),
        ApplicationError::Unauthorized(msg) => Status::permission_denied(msg),
    }
}

/// Convert proto Pagination to domain Pagination.
pub fn proto_to_domain_pagination(proto: Option<crate::grpc::proto::Pagination>) -> DomainPagination {
    proto.map(|p| DomainPagination {
        page: p.page,
        page_size: p.page_size,
    }).unwrap_or_default()
}

/// Convert domain PagedResult to proto PageInfo.
pub fn paged_result_to_page_info<T>(paged: &PagedResult<T>) -> crate::grpc::proto::PageInfo {
    crate::grpc::proto::PageInfo {
        total_count: paged.total_count,
        page: paged.page,
        page_size: paged.page_size,
        has_next_page: paged.has_next_page(),
        total_pages: paged.total_pages(),
    }
}
