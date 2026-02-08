pub mod error;
pub mod events;
pub mod model;
pub mod traits;
pub mod value_objects;

// Re-exports for ergonomics
pub use error::DomainError;
pub use events::{AggregateType, DomainEvent, EventEnvelope};
pub use model::account::{Account, AccountId, AccountStatus, AuthConfig, ConnectionConfig, ProviderType};
pub use model::attachment::{Attachment, AttachmentId};
pub use model::identity::Identity;
pub use model::mailbox::{Mailbox, MailboxId, MailboxType};
pub use model::message::{Address, Envelope, Message, OutgoingMessage};
pub use value_objects::email_address::EmailAddress;
pub use value_objects::flag::Flag;
pub use value_objects::mailbox_path::MailboxPath;
pub use value_objects::message_id::MessageId;
pub use value_objects::uid::ProviderUid;
