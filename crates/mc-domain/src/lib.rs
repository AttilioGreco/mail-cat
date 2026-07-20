pub mod error;
pub mod events;
pub mod model;
pub mod traits;
pub mod value_objects;

// Re-exports for ergonomics
pub use error::DomainError;
pub use events::{AggregateType, DomainEvent, EventEnvelope};
pub use model::account::{
    Account, AccountId, AccountStatus, AuthConfig, ConnectionConfig, OAuthProvider, ProviderType,
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
pub use traits::account_event_source::AccountEventSource;
pub use traits::email_provider::{
    EmailProvider, FetchRange, MailboxStatus, MessageBodyResult, MessageEnvelopeSummary,
    ProviderNotification, UidFlagEntry,
};
pub use traits::event_store::EventStore;
pub use traits::provider_registry::ProviderRegistry;
pub use traits::secret_store::{AccountSecrets, OAuthSecrets, SecretStore};
pub use traits::read_model_projector::ReadModelProjector;
pub use traits::repository::{
    AccountReadRepository, AccountSummary, AccountWriteRepository, BackfillEntry, BodyBackfillQueue,
    ContactReadRepository, ContactSummary, ContactWriteRepository, ConversationReadRepository,
    ConversationSummary, MailboxReadRepository, MessageReadRepository, MessageSummary,
    ProjectionCheckpoint,
};
pub use model::conversation::Conversation;
pub use value_objects::thread_id::ThreadId;
pub use model::contact::{Contact, ContactEmail, ContactEmailLabel, ContactSource};
pub use value_objects::contact_id::ContactId;
pub use traits::contact_index::{ContactIndex, ContactSearchHit};
pub use traits::search_index::{SearchHit, SearchIndex, SearchResults, SuggestScope, Suggestion};
