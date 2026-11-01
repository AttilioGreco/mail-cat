//! What the search index needs from the read model beyond the message itself
//! (labels, List-Id), and the rebuild that fills a new index from it.

use std::collections::HashMap;

use tracing::info;

use mc_domain::model::account::MailboxModel;
use mc_domain::{
    AccountId, AccountReadRepository, EventStore, Flag, IndexExtras, MailboxId,
    MailboxReadRepository, MailboxType, Message, MessageId, MessageReadRepository,
    ProjectionCheckpoint, SearchIndex,
};
use mc_shared::pagination::Pagination;

use super::{PROJECTION_NAME, SearchProjectionHandler};
use crate::error::ApplicationError;

const REBUILD_PAGE: usize = 500;
const MAILBOX_PAGE: u32 = 500;

/// How an account's mail is labelled, loaded once per account per pass.
pub(super) struct Labeling {
    /// Gmail-style accounts label with mailboxes; the rest with keywords.
    by_mailbox: bool,
    custom_mailboxes: HashMap<MailboxId, String>,
}

pub(super) type LabelingCache = HashMap<AccountId, Labeling>;

impl<SI, R, C> SearchProjectionHandler<SI, R, C>
where
    SI: SearchIndex,
    R: MessageReadRepository + MailboxReadRepository + AccountReadRepository,
    C: ProjectionCheckpoint,
{
    /// Indexes `message` with its labels and List-Id.
    pub(super) async fn index(
        &self,
        message: &Message,
        cache: &mut LabelingCache,
    ) -> Result<(), ApplicationError> {
        let labeling = match cache.get(&message.account_id) {
            Some(labeling) => labeling,
            None => cache
                .entry(message.account_id)
                .or_insert(self.labeling(&message.account_id).await?),
        };
        let labels = if labeling.by_mailbox {
            message
                .memberships
                .iter()
                .filter_map(|m| labeling.custom_mailboxes.get(&m.mailbox_id))
                .cloned()
                .collect()
        } else {
            message
                .flags
                .iter()
                .filter_map(|flag| match flag {
                    // "$Forwarded" and the like are protocol state, not labels.
                    // Tagging writes a space as `_` (keyword_of); read it back the same way.
                    Flag::Custom(name) if !name.starts_with('$') => Some(name.replace('_', " ")),
                    _ => None,
                })
                .collect()
        };
        let extras = IndexExtras {
            labels,
            list_id: self.read_repo.list_id_of(&message.id).await?,
        };
        Ok(self.search_index.index_message(message, &extras).await?)
    }

    async fn labeling(&self, account_id: &AccountId) -> Result<Labeling, ApplicationError> {
        let by_mailbox = self
            .read_repo
            .get_account(account_id)
            .await?
            .is_some_and(|a| a.mailbox_model == MailboxModel::Labels);
        let custom_mailboxes = self
            .read_repo
            .list_mailboxes(account_id)
            .await?
            .into_iter()
            .filter(|m| m.mailbox_type == MailboxType::Custom)
            .map(|m| (m.id, m.display_path))
            .collect();
        Ok(Labeling {
            by_mailbox,
            custom_mailboxes,
        })
    }

    /// A mailbox's name or role changed, and with it the labels of every
    /// message in it.
    pub(super) async fn reindex_mailbox(
        &self,
        mailbox_id: &MailboxId,
    ) -> Result<(), ApplicationError> {
        let mut cache = LabelingCache::new();
        for page in 1.. {
            let found = self
                .read_repo
                .list_messages(mailbox_id, &Pagination::new(page, MAILBOX_PAGE))
                .await?;
            for summary in &found.items {
                if let Some(message) = self.read_repo.get_message(&summary.id).await? {
                    self.index(&message, &mut cache).await?;
                }
            }
            if found.items.len() < MAILBOX_PAGE as usize {
                break;
            }
        }
        Ok(())
    }

    /// Fills a new, empty index from the read model instead of replaying the
    /// event log, then moves the checkpoint to the log's head so later
    /// catch-ups only see newer events. Until it ends, `catch_up` does nothing
    /// (it would replay the whole log from the unmoved checkpoint), and a crash
    /// leaves that checkpoint where it was: the next boot replays the log.
    pub async fn rebuild<ES: EventStore>(&self, store: &ES) -> Result<u64, ApplicationError> {
        use futures::StreamExt;
        use std::sync::atomic::Ordering::Relaxed;

        struct Done<'a>(&'a std::sync::atomic::AtomicBool);
        impl Drop for Done<'_> {
            fn drop(&mut self) {
                self.0.store(false, Relaxed);
            }
        }
        self.rebuilding.store(true, Relaxed);
        let _done = Done(&self.rebuilding);

        let mut head = 0;
        let mut events = store.load_all_events(None, 1);
        while let Some(envelope) = events.next().await {
            head = envelope?.global_position;
        }
        drop(events);

        let (mut cache, mut count, mut after): (_, u64, Option<MessageId>) =
            (LabelingCache::new(), 0, None);
        loop {
            let ids = self
                .read_repo
                .message_ids_after(after.as_ref(), REBUILD_PAGE)
                .await?;
            let Some(last) = ids.last().copied() else {
                break;
            };
            for id in ids {
                if let Some(message) = self.read_repo.get_message(&id).await? {
                    self.index(&message, &mut cache).await?;
                }
                count += 1;
            }
            after = Some(last);
            self.search_index.flush().await?;
            info!(indexed = count, "search index rebuild progress");
        }
        self.checkpoint
            .save_checkpoint(PROJECTION_NAME, head)
            .await?;
        info!(count, "search index rebuild complete");
        Ok(count)
    }
}
