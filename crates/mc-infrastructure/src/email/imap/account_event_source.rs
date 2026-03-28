use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use tracing::{debug, warn};

use mc_domain::error::DomainError;
use mc_domain::traits::account_event_source::AccountEventSource;
use mc_domain::traits::email_provider::{EmailProvider, ProviderNotification};
use mc_domain::value_objects::mailbox_path::MailboxPath;

use super::ImapEmailProvider;

/// IMAP implementation of [`AccountEventSource`].
///
/// Watches multiple mailboxes concurrently using IMAP IDLE sessions.
/// One background task is spawned per mailbox; all notifications are
/// funnelled through a shared mpsc channel and yielded from the returned
/// stream.
///
/// Reconnection on transient IMAP errors is handled by
/// [`ImapEmailProvider::watch_mailbox`] internally (it loops on errors).
/// An additional outer reconnect loop here protects against the stream
/// ending unexpectedly.
pub struct ImapAccountEventSource {
    provider: Arc<ImapEmailProvider>,
}

impl ImapAccountEventSource {
    pub fn new(provider: Arc<ImapEmailProvider>) -> Self {
        Self { provider }
    }
}

impl AccountEventSource for ImapAccountEventSource {
    fn watch(
        &self,
        mailboxes: &[MailboxPath],
    ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + 'static>> {
        // Clone all owned data before passing to the static stream factory,
        // ensuring the returned stream has no lifetime dependency on &self.
        watch_mailboxes(Arc::clone(&self.provider), mailboxes.to_vec())
    }
}

/// Free function that creates a `'static` notification stream for the given
/// mailboxes. Each mailbox gets its own background task (IMAP IDLE session).
///
/// Separated from `ImapAccountEventSource::watch` so the `'static` lifetime
/// can be proven without the compiler tying the return value to `&self`.
fn watch_mailboxes(
    provider: Arc<ImapEmailProvider>,
    mailboxes: Vec<MailboxPath>,
) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + 'static>> {
    Box::pin(async_stream::stream! {
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<Result<ProviderNotification, DomainError>>(128);

        for mailbox in mailboxes {
            let p = Arc::clone(&provider);
            let t = tx.clone();
            tokio::spawn(async move {
                debug!(
                    "[AccountEventSource] Spawning IMAP IDLE watcher for {}",
                    mailbox.full_path()
                );
                loop {
                    let mut stream = p.watch_mailbox(&mailbox);
                    while let Some(notification) = stream.next().await {
                        if t.send(notification).await.is_err() {
                            // Receiver dropped — outer stream was closed
                            return;
                        }
                    }
                    // Stream ended unexpectedly; brief pause before restart
                    warn!(
                        "[AccountEventSource] Watch stream ended for {}, restarting in 5s",
                        mailbox.full_path()
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            });
        }
        // Drop the producer side so the channel closes when all tasks exit
        drop(tx);

        while let Some(notification) = rx.recv().await {
            yield notification;
        }
    })
}
