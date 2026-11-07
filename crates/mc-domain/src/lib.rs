pub mod error;
pub mod events;
pub mod formats;
pub mod model;
pub mod traits;
pub mod value_objects;

// Re-exports for ergonomics
pub use error::DomainError;
pub use events::{AggregateType, DomainEvent, EventEnvelope};
pub use model::account::{
    Account, AccountId, AccountStatus, AuthConfig, ConnectionConfig, IncomingServer, MailboxModel,
    OAuthProvider, ProviderType,
};
pub use model::attachment::{Attachment, AttachmentId};
pub use model::identity::Identity;
pub use model::mailbox::{Mailbox, MailboxId, MailboxType};
pub use model::message::{
    Address, Envelope, ListHeaders, Membership, Message, OutgoingAttachment, OutgoingMessage,
};
pub use value_objects::email_address::EmailAddress;
pub use value_objects::flag::Flag;
pub use value_objects::mailbox_path::MailboxPath;
pub use value_objects::message_id::MessageId;
pub use value_objects::message_key::MessageKey;
pub use value_objects::remote::{CollectionId, Etag, Href, Origin, RemoteBinding};
pub use value_objects::remote_id::RemoteId;
pub use value_objects::sync_cursor::SyncCursor;

// Trait re-exports
pub use model::collection::{
    AddressBook, Calendar, CalendarComponent, CalendarEvent, EventFields, EventStatus, Ownership,
};
pub use model::contact::{Contact, ContactEmail, ContactEmailLabel, ContactSource, RemoteCard};
pub use model::conversation::Conversation;
pub use model::discovery::{DiscoveryPoint, Proposal, ProposalId, ProposalSample, ProposalStatus};
pub use model::embedding::{EmbedPurpose, ModelId, ModelOption, ModelProfile, TargetOption};
pub use model::tagging::{
    AutoApplied, BackgroundProfile, BackgroundSpeed, Confidence, Example, GroupId, LabelExamples,
    LabelMode, LabelSettings, LabelStats, LabelledVector, Lesson, ModelStats, Scope, Score,
    SourceFilter, TagSource, TrainingGroup,
};
pub use traits::account_event_source::AccountEventSource;
pub use traits::contact_index::ContactSearchHit;
pub use traits::dav_provider::{
    CalendarProvider, CollectionChanges, ContactsProvider, DeleteOutcome, PutOutcome,
    RemoteResource, ResourcePut,
};
pub use traits::dav_repository::{
    DavReadRepository, Occurrence, PendingChange, PendingPush, RemoteContact,
};
pub use traits::email_provider::{
    EmailProvider, Existing, MailboxChanges, MailboxCounts, MessageBodyResult,
    MessageEnvelopeSummary, ProviderNotification, Reset, SendOutcome, SentCopy,
};
pub use traits::embedder::Embedder;
pub use traits::event_store::EventStore;
pub use traits::label_namer::{ClusterSummary, LabelNamer, ProposedName};
pub use traits::login_gate::{LoginGate, LoginSecret, login_with};
pub use traits::provider_registry::ProviderRegistry;
pub use traits::read_model_projector::ReadModelProjector;
pub use traits::repository::{
    AccountReadRepository, AccountSummary, AccountSyncStats, AccountWriteRepository, BackfillEntry,
    BodyBackfillQueue, ContactReadRepository, ContactSummary, ContactWriteRepository,
    ConversationReadRepository, ConversationSummary, MailboxReadRepository, MessageBodyStore,
    MessageReadRepository, MessageSummary, PendingRename, PendingRenameStore, ProjectionCheckpoint,
    SyncCursorStore,
};
pub use traits::search_index::{
    IndexExtras, IndexHit, IndexResults, SearchDirectory, SearchHit, SearchIndex, SearchResults,
    SuggestScope, Suggestion,
};
pub use traits::secret_store::{AccountSecrets, OAuthSecrets, SecretStore};
pub use traits::tagging_store::TaggingStore;
pub use value_objects::contact_id::ContactId;
pub use value_objects::thread_id::ThreadId;
