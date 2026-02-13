// Our traits are consumed via generics (`<P: EmailProvider>`), not dyn dispatch.
// `async fn` in traits is safe and correct in this context.
#![allow(async_fn_in_trait)]

pub mod error;
pub mod events;
pub mod model;
pub mod traits;
pub mod value_objects;

// Re-exports for ergonomics
pub use error::DomainError;
pub use events::{AggregateType, DomainEvent, EventEnvelope};
pub use model::account::{
    Account, AccountId, AccountStatus, AuthConfig, ConnectionConfig, ProviderType,
};
pub use model::attachment::{Attachment, AttachmentId};
pub use model::identity::Identity;
pub use model::mailbox::{Mailbox, MailboxId, MailboxType};
pub use model::message::{Address, Envelope, Message, OutgoingMessage};
pub use value_objects::email_address::EmailAddress;
pub use value_objects::flag::Flag;
pub use value_objects::mailbox_path::MailboxPath;
pub use value_objects::message_id::MessageId;
pub use value_objects::uid::ProviderUid;

// Trait re-exports
pub use traits::email_provider::{
    EmailProvider, FetchRange, MailboxStatus, MessageEnvelopeSummary, ProviderNotification,
};
pub use traits::event_store::EventStore;
pub use traits::read_model_projector::ReadModelProjector;
pub use traits::repository::{
    AccountReadRepository, AccountSummary, AccountWriteRepository, MailboxReadRepository,
    MessageReadRepository, MessageSummary, ProjectionCheckpoint,
};
pub use traits::search_index::{SearchHit, SearchIndex, SearchResults};
