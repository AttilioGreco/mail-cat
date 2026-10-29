use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use tracing::{debug, warn};

use mc_domain::error::DomainError;
use mc_domain::traits::account_event_source::AccountEventSource;
use mc_domain::traits::email_provider::{EmailProvider, ProviderNotification};
use mc_domain::value_objects::mailbox_path::MailboxPath;

use super::AnyProvider;

/// [`AccountEventSource`] for whichever backend the account uses.
///
/// Mailboxes are watched concurrently, one IMAP IDLE session or JMAP event
/// stream each; all notifications are funnelled through a shared mpsc channel
/// and yielded from the returned stream.
pub struct AnyAccountEventSource {
    provider: Arc<AnyProvider>,
}

impl AnyAccountEventSource {
    pub fn new(provider: Arc<AnyProvider>) -> Self {
        Self { provider }
    }
}

impl AccountEventSource for AnyAccountEventSource {
    fn watch(
        &self,
        mailboxes: &[MailboxPath],
    ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + 'static>>
    {
        watch_mailboxes(Arc::clone(&self.provider), mailboxes.to_vec())
    }
}

/// Free function that creates a `'static` notification stream for the given
/// mailboxes. Each mailbox gets its own background task (IMAP IDLE session, JMAP event stream).
///
/// Separated from `AnyAccountEventSource::watch` so the `'static` lifetime
/// can be proven without the compiler tying the return value to `&self`.
fn watch_mailboxes(
    provider: Arc<AnyProvider>,
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
                    "[AccountEventSource] Spawning watcher for {}",
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
