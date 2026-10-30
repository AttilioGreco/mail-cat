//! Which mailboxes the user sees and syncs. The server's own subscription
//! decides until the user does (`Mailbox::subscription_explicit`).

use std::sync::Arc;

use tracing::{info, warn};

use mc_domain::{
    AggregateType, EmailProvider, EventStore, Mailbox, MailboxId, MailboxType, ProviderRegistry,
};

use super::events::sync_failed;
use super::mailbox::left_mailbox;
use super::{SyncService, SyncStore};
use crate::error::ApplicationError;

impl<PR: ProviderRegistry, ES: EventStore, P: SyncStore> SyncService<PR, ES, P> {
    /// Follow the server's subscription for a mailbox the user has not
    /// decided on, as listed in `remote`.
    pub(super) async fn follow_server_subscription(
        &self,
        provider: &Arc<PR::Provider>,
        mailbox: &mut Mailbox,
        remote: &Mailbox,
    ) -> Result<(), ApplicationError> {
        if mailbox.subscription_explicit || mailbox.subscribed == remote.subscribed {
            return Ok(());
        }
        info!(path = %remote.path, subscribed = remote.subscribed, "Subscription changed on the server");
        let event = mailbox.set_subscribed(remote.subscribed, false);
        self.append_and_project(&mailbox.id.to_string(), AggregateType::Mailbox, event)
            .await?;
        if !mailbox.subscribed {
            self.drop_local_messages(provider, mailbox).await?;
        }
        Ok(())
    }

    /// Whether `mailbox` is still subscribed now: a pass that began before
    /// an unsubscribe must not go on downloading.
    pub(super) async fn is_subscribed(&self, mailbox: &Mailbox) -> Result<bool, ApplicationError> {
        Ok(self
            .projector
            .get_mailbox(&mailbox.id)
            .await
            .map_err(sync_failed("get_mailbox"))?
            .is_some_and(|m| m.subscribed))
    }

    /// The user's choice. Applied locally whatever the server says; `Some`
    /// is the warning when the server could not be told.
    pub async fn set_mailbox_subscribed(
        &self,
        mailbox_id: &MailboxId,
        subscribed: bool,
    ) -> Result<Option<String>, ApplicationError> {
        let mut mailbox = self
            .projector
            .get_mailbox(mailbox_id)
            .await
            .map_err(sync_failed("get_mailbox"))?
            .ok_or_else(|| ApplicationError::NotFound("Mailbox", mailbox_id.to_string()))?;
        if !subscribed && mailbox.mailbox_type == MailboxType::Inbox {
            return Err(ApplicationError::PreconditionFailed(
                "the Inbox is always subscribed".into(),
            ));
        }
        let provider = self.provider_registry.get_provider(&mailbox.account_id)?;

        // The event goes first and without the account lock: a sync of this
        // mailbox that is running sees it and stops, which is what releases
        // the lock below.
        let event = mailbox.set_subscribed(subscribed, true);
        self.append_and_project(&mailbox.id.to_string(), AggregateType::Mailbox, event)
            .await?;
        if !subscribed {
            let _exclusive = self.exclusive(&mailbox.account_id).await;
            self.drop_local_messages(&provider, &mailbox).await?;
        }

        let warning = match provider.set_subscribed(&mailbox.path, subscribed).await {
            Ok(()) => None,
            Err(e) => {
                warn!(path = %mailbox.path, error = %e, "Server not told of the subscription");
                Some(format!(
                    "{}: the server was not updated ({e}); the choice applies here only",
                    mailbox.display_path
                ))
            }
        };
        if subscribed && let Err(e) = self.sync_mailbox(&mailbox.account_id, &mailbox.path).await {
            warn!(path = %mailbox.path, error = %e, "First sync of a subscribed mailbox failed");
        }
        Ok(warning)
    }

    /// Take the messages that live only in `mailbox` out of the store, and the
    /// memberships of the rest, one event each so the search index follows.
    /// A Gmail label (a view over another mailbox) holds nothing of its own.
    async fn drop_local_messages(
        &self,
        provider: &Arc<PR::Provider>,
        mailbox: &Mailbox,
    ) -> Result<(), ApplicationError> {
        if provider.backed_by(&mailbox.path).is_some() {
            return Ok(());
        }
        let views = self.views_of(provider, mailbox).await?;
        let all = mc_shared::pagination::Pagination {
            page: 1,
            page_size: u32::MAX,
        };
        let messages = self
            .projector
            .list_messages(&mailbox.id, &all)
            .await
            .map_err(sync_failed("list_messages"))?
            .items;
        info!(path = %mailbox.path, count = messages.len(), "Unsubscribed, dropping local messages");
        for summary in &messages {
            self.append_and_project(
                &summary.id.to_string(),
                AggregateType::Message,
                left_mailbox(summary, mailbox.id, &views),
            )
            .await?;
        }
        Ok(())
    }
}
