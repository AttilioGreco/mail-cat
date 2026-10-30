use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
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

type Notifications = Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send>>;

impl AccountEventSource for AnyAccountEventSource {
    fn watch(&self, mailboxes: &[MailboxPath]) -> Notifications {
        let provider = Arc::clone(&self.provider);
        merge_watches(mailboxes.to_vec(), move |mailbox| {
            let provider = Arc::clone(&provider);
            Box::pin(async_stream::stream! {
                let mut stream = provider.watch_mailbox(&mailbox);
                while let Some(notification) = stream.next().await {
                    yield notification;
                }
            })
        })
    }
}

/// One notification stream over the mailboxes, each watched by a background
/// task (IMAP IDLE session, JMAP event stream) that `open` starts and that is
/// restarted when it ends. Dropping the returned stream stops every task and
/// so closes the sessions: they would otherwise only notice at their next send.
fn merge_watches<F>(mailboxes: Vec<MailboxPath>, open: F) -> Notifications
where
    F: Fn(MailboxPath) -> Notifications + Clone + Send + 'static,
{
    Box::pin(async_stream::stream! {
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<Result<ProviderNotification, DomainError>>(128);
        let stop = CancellationToken::new();
        let _stop_on_drop = stop.clone().drop_guard();

        for mailbox in mailboxes {
            let (open, tx, stop) = (open.clone(), tx.clone(), stop.clone());
            tokio::spawn(async move {
                debug!(
                    "[AccountEventSource] Spawning watcher for {}",
                    mailbox.full_path()
                );
                loop {
                    let mut stream = open(mailbox.clone());
                    loop {
                        let notification = tokio::select! {
                            _ = stop.cancelled() => return,
                            notification = stream.next() => notification,
                        };
                        let Some(notification) = notification else { break };
                        if tx.send(notification).await.is_err() {
                            return;
                        }
                    }
                    // Stream ended unexpectedly; brief pause before restart
                    warn!(
                        "[AccountEventSource] Watch stream ended for {}, restarting in 5s",
                        mailbox.full_path()
                    );
                    tokio::select! {
                        _ = stop.cancelled() => return,
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {}
                    }
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

#[cfg(test)]
mod tests;
