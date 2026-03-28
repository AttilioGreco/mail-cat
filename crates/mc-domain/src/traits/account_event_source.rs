use std::pin::Pin;

use futures::Stream;

use crate::error::DomainError;
use crate::traits::email_provider::ProviderNotification;
use crate::value_objects::mailbox_path::MailboxPath;

/// Provider-agnostic, server-side account event source.
///
/// One instance runs per account at server startup, independently of any
/// connected gRPC client. Implementations translate protocol-specific
/// change notifications into the unified [`ProviderNotification`] vocabulary:
///
/// - **IMAP**: IDLE sessions per monitored mailbox
/// - **Gmail** (future): push webhook subscriptions
/// - **Office365** (future): Microsoft Graph API change notifications
///
/// # Design
/// The trait is intentionally account-scoped rather than per-mailbox because
/// providers like Gmail and Office365 deliver notifications at account level.
/// IMAP implementations multiplex per-mailbox IDLE sessions internally.
///
/// The returned stream must never terminate on its own; implementations are
/// expected to reconnect internally on transient failures.
///
/// The `'static` bound on the return type means implementations must fully own
/// all state needed for the stream (e.g. via `Arc` cloning). This allows the
/// stream to be moved into `tokio::spawn` background tasks.
pub trait AccountEventSource: Send + Sync {
    /// Start streaming notifications for the given set of mailboxes.
    ///
    /// Returns a `'static` stream; implementations must not borrow from `self`
    /// in the returned value (clone or `Arc`-wrap what is needed).
    fn watch(
        &self,
        mailboxes: &[MailboxPath],
    ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + 'static>>;
}
