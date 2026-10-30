//! Bringing one mailbox up to date: ask the provider what changed since the
//! stored cursor, apply it, and only then move the cursor.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::{debug, info, instrument, warn};

use mc_domain::{
    AggregateType, DomainEvent, EmailProvider, EventStore, Existing, Flag, Mailbox, MailboxChanges,
    MailboxId, MailboxPath, MailboxType, Membership, MessageKey, MessageSummary, ProviderRegistry,
    RemoteId, Reset,
};

use super::events::sync_failed;
use super::{
    ENVELOPE_BATCH, RESTORE_CAP, SyncProgressEvent, SyncService, SyncState, SyncStore,
    stops_the_pass,
};
use crate::error::ApplicationError;

/// The event for a message the server no longer has in `mailbox_id`: the
/// mailbox leaves its memberships, and the message goes with its last one.
/// So do the `views` it was in, which exist only through that mailbox.
/// Built from the read model's view: the stream is appended as-is.
pub(super) fn left_mailbox(
    summary: &MessageSummary,
    mailbox_id: MailboxId,
    views: &[Mailbox],
) -> DomainEvent {
    let removed: Vec<MailboxId> = std::iter::once(mailbox_id)
        .chain(
            views
                .iter()
                .map(|v| v.id)
                .filter(|v| summary.membership(*v).is_some()),
        )
        .collect();
    if summary.memberships.len() > removed.len() {
        return DomainEvent::MessageMailboxesChanged {
            message_id: summary.id,
            added: vec![],
            removed,
        };
    }
    DomainEvent::MessageDeleted {
        message_id: summary.id,
        mailbox_ids: removed,
    }
}

/// What reconciling the messages already known found.
#[derive(Default)]
struct Reconciled {
    removed: u32,
    flags_changed: u32,
    /// Server messages no local row held, to bring back.
    missing: Vec<RemoteId>,
}

impl<PR: ProviderRegistry, ES: EventStore, P: SyncStore> SyncService<PR, ES, P> {
    /// Bring one mailbox up to date. The cursor is saved last, after
    /// everything it covers is applied: a crash or a failure in between redoes
    /// the sync from the old one, and every step is safe to repeat (stored
    /// messages are skipped by id). `(new, removed)`, or `None` when it
    /// stopped early (cancelled, or the server failed). Errors that would
    /// fail every other mailbox alike (login, connection, throttling) abort
    /// the whole pass instead.
    pub(super) async fn sync_one(
        &self,
        provider: &Arc<PR::Provider>,
        local: &Mailbox,
        progress: Option<&mut Vec<SyncProgressEvent>>,
    ) -> Result<Option<(u32, u32)>, ApplicationError> {
        if !self.is_subscribed(local).await? {
            return Ok(None);
        }
        let collection = local.id.to_string();
        let since = self
            .projector
            .get_cursor(&local.account_id, &collection)
            .await
            .map_err(sync_failed("get_cursor"))?;
        let changes = match provider.mailbox_changes(&local.path, since.as_ref()).await {
            Ok(changes) => changes,
            Err(e) if stops_the_pass(&e) => return Err(e.into()),
            Err(e) => {
                warn!(path = %local.path, error = %e, "Failed to get mailbox changes, skipping");
                return Ok(None);
            }
        };

        let ids_invalidated = changes.reset == Some(Reset::IdsInvalidated);
        // An invalidation settles its own waiting rows (drop_unclaimed).
        let waiting = if ids_invalidated {
            self.forget_remote_ids(local).await?;
            Vec::new()
        } else {
            self.projector
                .pending_messages(&local.id)
                .await
                .map_err(sync_failed("pending_messages"))?
        };

        let views = self.views_of(provider, local).await?;
        // `existing` was listed with `added`; ingesting can take minutes, in
        // which time user actions change what it describes, so apply it first.
        let reconciled = self.reconcile(local, &views, &changes).await?;
        let restored = self.restore(provider, local, &reconciled.missing).await?;
        let Some(ingested) = self
            .ingest_added(provider, local, &changes.added, &changes.keys, progress)
            .await?
        else {
            return Ok(None);
        };
        let new_count = ingested + restored;
        if ids_invalidated {
            self.drop_unclaimed(local).await?;
        }
        if !views.is_empty() {
            self.reconcile_views(local, &views, &changes.labels).await?;
        }
        let removed = reconciled.removed + self.settle_pending(provider, local, waiting).await?;
        if removed > 0 || reconciled.flags_changed > 0 || restored > 0 {
            info!(removed, flags_changed = reconciled.flags_changed, restored, path = %local.path, "Mailbox reconciled");
        }

        self.projector
            .put_cursor(&local.account_id, &collection, &changes.cursor)
            .await
            .map_err(sync_failed("put_cursor"))?;
        self.update_mailbox_counts(local, changes.counts.total, changes.counts.unread)
            .await?;
        if !views.is_empty() && (!changes.labels.is_empty() || removed > 0) {
            self.update_view_counts(&views).await?;
        }
        Ok(Some((new_count, removed)))
    }

    /// A label deleted or renamed on the server leaves a local view the
    /// layout no longer knows, so no sync would reconcile it. Only Gmail has
    /// them (its `provider` names a model), only `Custom` mailboxes are views
    /// (All Mail, Spam and Trash are real), and a LIST without `\All` is not
    /// to be trusted: it may be a failed or partial one.
    /// Deleting the mailbox drops its memberships; a renamed label's messages
    /// are labelled again by the full read the changed fingerprint forces.
    pub(super) async fn drop_gone_labels(
        &self,
        provider: &Arc<PR::Provider>,
        local: &[Mailbox],
        remote: &[Mailbox],
    ) -> Result<(), ApplicationError> {
        if provider.preferred_mailbox_model().is_none()
            || !remote.iter().any(|m| m.mailbox_type == MailboxType::All)
        {
            return Ok(());
        }
        let listed: HashSet<&MailboxPath> = remote.iter().map(|m| &m.path).collect();
        for gone in local
            .iter()
            .filter(|m| m.mailbox_type == MailboxType::Custom && !listed.contains(&m.path))
        {
            info!(path = %gone.path, "Label gone from the server, removing it");
            self.append_and_project(&gone.id.to_string(), AggregateType::Mailbox, gone.delete())
                .await?;
        }
        Ok(())
    }

    /// The mailboxes that are only views over `local` (Gmail labels over All
    /// Mail): they have no sync of their own.
    pub(super) async fn views_of(
        &self,
        provider: &Arc<PR::Provider>,
        local: &Mailbox,
    ) -> Result<Vec<Mailbox>, ApplicationError> {
        Ok(self
            .projector
            .list_mailboxes(&local.account_id)
            .await
            .map_err(sync_failed("list_mailboxes"))?
            .into_iter()
            .filter(|m| provider.backed_by(&m.path).as_ref() == Some(&local.path))
            .collect())
    }

    /// Put each message `labels` names in exactly those of `views`, leaving
    /// its other memberships alone. A view holds the id the message has in
    /// the mailbox backing it.
    async fn reconcile_views(
        &self,
        local: &Mailbox,
        views: &[Mailbox],
        labels: &HashMap<RemoteId, Vec<MailboxPath>>,
    ) -> Result<(), ApplicationError> {
        for (id, paths) in labels {
            let Some(summary) = self
                .projector
                .find_by_remote_id(&local.id, id)
                .await
                .map_err(sync_failed("find_by_remote_id"))?
            else {
                continue;
            };
            let wanted = |view: &Mailbox| paths.contains(&view.path);
            let added: Vec<Membership> = views
                .iter()
                .filter(|v| wanted(v) && summary.membership(v.id).is_none())
                .map(|v| Membership {
                    mailbox_id: v.id,
                    remote_id: Some(id.clone()),
                })
                .collect();
            let removed: Vec<MailboxId> = views
                .iter()
                .filter(|v| !wanted(v) && summary.membership(v.id).is_some())
                .map(|v| v.id)
                .collect();
            if added.is_empty() && removed.is_empty() {
                continue;
            }
            let event = DomainEvent::MessageMailboxesChanged {
                message_id: summary.id,
                added,
                removed,
            };
            self.append_and_project(&summary.id.to_string(), AggregateType::Message, event)
                .await?;
        }
        Ok(())
    }

    /// Views are never listed by the server, so their totals come from the
    /// read model (their unread counts follow memberships and flags already).
    async fn update_view_counts(&self, views: &[Mailbox]) -> Result<(), ApplicationError> {
        let Some(first) = views.first() else {
            return Ok(());
        };
        let fresh = self
            .projector
            .list_mailboxes(&first.account_id)
            .await
            .map_err(sync_failed("list_mailboxes"))?;
        let one = mc_shared::pagination::Pagination {
            page: 1,
            page_size: 1,
        };
        for view in views {
            let total = self
                .projector
                .list_messages(&view.id, &one)
                .await
                .map_err(sync_failed("list_messages"))?
                .total_count;
            let unread = fresh
                .iter()
                .find(|m| m.id == view.id)
                .map_or(0, |m| m.unread_messages);
            self.update_mailbox_counts(view, total as u32, unread)
                .await?;
        }
        Ok(())
    }

    /// Fetch and store what arrived, in batches, newest first. `None` when
    /// cancelled or the server failed part-way.
    async fn ingest_added(
        &self,
        provider: &Arc<PR::Provider>,
        local: &Mailbox,
        added: &[RemoteId],
        keys: &HashMap<RemoteId, MessageKey>,
        mut progress: Option<&mut Vec<SyncProgressEvent>>,
    ) -> Result<Option<u32>, ApplicationError> {
        let label = format!("Fetching messages in {}", local.display_path);
        let mut new_count = 0;
        for (index, batch) in added.chunks(ENVELOPE_BATCH).enumerate() {
            // Only a full sync (the one reporting progress) is cancellable.
            if progress.is_some() && self.is_cancelled() {
                return Ok(None);
            }
            if !self.is_subscribed(local).await? {
                return Ok(None);
            }
            if let Some(progress) = progress.as_deref_mut() {
                self.emit(
                    progress,
                    SyncProgressEvent {
                        account_id: local.account_id,
                        state: SyncState::FetchingEnvelopes {
                            mailbox_id: local.id,
                        },
                        processed: (index * ENVELOPE_BATCH) as u32,
                        total: added.len() as u32,
                        message: label.clone(),
                    },
                );
            }
            let wanted = self.unheld(local, batch).await?;
            let wanted = self.join_known(local, wanted, keys).await?;
            if wanted.is_empty() {
                continue;
            }
            let Some(envelopes) = self.fetch_envelopes(provider, local, &wanted).await? else {
                return Ok(None);
            };
            new_count += self.ingest(provider, local, &envelopes, false).await?;
        }
        Ok(Some(new_count))
    }

    async fn fetch_envelopes(
        &self,
        provider: &Arc<PR::Provider>,
        local: &Mailbox,
        ids: &[RemoteId],
    ) -> Result<Option<Vec<mc_domain::MessageEnvelopeSummary>>, ApplicationError> {
        match provider.fetch_envelopes(&local.path, ids).await {
            Ok(envelopes) => Ok(Some(envelopes)),
            Err(e) if stops_the_pass(&e) => Err(e.into()),
            Err(e) => {
                warn!(path = %local.path, error = %e, "Failed to fetch envelopes, resuming next cycle");
                Ok(None)
            }
        }
    }

    /// Compare the messages already known with the server's state: those it
    /// no longer has leave the mailbox, the rest get its flags. Server
    /// messages no local row holds are reported back, not fetched here.
    #[instrument(skip(self, local, views, changes), fields(mailbox_id = %local.id))]
    async fn reconcile(
        &self,
        local: &Mailbox,
        views: &[Mailbox],
        changes: &MailboxChanges,
    ) -> Result<Reconciled, ApplicationError> {
        let mut out = Reconciled::default();
        // Not in Drafts: a draft discarded or replaced here is deleted on the
        // server afterwards, and bringing it back in between would resurrect
        // it. A stray server copy is harmless there, a revived draft is not.
        let restorable = local.mailbox_type != MailboxType::Drafts;
        let arrived: HashSet<&RemoteId> = changes.added.iter().collect();

        match &changes.existing {
            Existing::Snapshot(entries) => {
                let listing = entries.iter().map(|(id, flags)| (id, Some(flags)));
                self.reconcile_listing(local, views, listing, restorable, &arrived, &mut out)
                    .await?;
            }
            Existing::Listed { present, changed } => {
                let changed: HashMap<&RemoteId, &Vec<Flag>> =
                    changed.iter().map(|(id, flags)| (id, flags)).collect();
                let listing = present.iter().map(|id| (id, changed.get(id).copied()));
                self.reconcile_listing(local, views, listing, restorable, &arrived, &mut out)
                    .await?;
            }
            Existing::Delta { changed, removed } => {
                for id in removed {
                    if let Some(summary) = self
                        .projector
                        .find_by_remote_id(&local.id, id)
                        .await
                        .map_err(sync_failed("find_by_remote_id"))?
                    {
                        self.leave(&summary, local, views, &mut out).await?;
                    }
                }
                for (id, flags) in changed {
                    match self
                        .projector
                        .find_by_remote_id(&local.id, id)
                        .await
                        .map_err(sync_failed("find_by_remote_id"))?
                    {
                        Some(summary) => self.reconcile_flags(&summary, flags, &mut out).await?,
                        None if restorable && !arrived.contains(id) => out.missing.push(id.clone()),
                        None => {}
                    }
                }
            }
        }
        Ok(out)
    }

    /// Reconcile against a listing of every message the mailbox has, newest
    /// first, with the flags of those that report them (`None`: unchanged).
    async fn reconcile_listing<'a>(
        &self,
        local: &Mailbox,
        views: &[Mailbox],
        listing: impl Iterator<Item = (&'a RemoteId, Option<&'a Vec<Flag>>)> + Clone,
        restorable: bool,
        arrived: &HashSet<&RemoteId>,
        out: &mut Reconciled,
    ) -> Result<(), ApplicationError> {
        let all = mc_shared::pagination::Pagination {
            page: 1,
            page_size: u32::MAX,
        };
        let local_messages = self
            .projector
            .list_messages(&local.id, &all)
            .await
            .map_err(sync_failed("list_messages"))?
            .items;
        let remote: HashMap<&RemoteId, Option<&Vec<Flag>>> = listing.clone().collect();
        let mut held = HashSet::new();
        for summary in &local_messages {
            let Some(id) = summary
                .membership(local.id)
                .and_then(|m| m.remote_id.as_ref())
            else {
                continue;
            };
            held.insert(id);
            match remote.get(id) {
                Some(Some(flags)) => self.reconcile_flags(summary, flags, out).await?,
                Some(None) => {}
                None => self.leave(summary, local, views, out).await?,
            }
        }
        if restorable {
            out.missing = listing
                .map(|(id, _)| id)
                .filter(|id| !held.contains(id) && !arrived.contains(id))
                .cloned()
                .collect();
        }
        Ok(())
    }

    async fn leave(
        &self,
        summary: &MessageSummary,
        local: &Mailbox,
        views: &[Mailbox],
        out: &mut Reconciled,
    ) -> Result<(), ApplicationError> {
        info!(message_id = %summary.id, "Message gone from the server, deleting locally");
        // De-indexing is owned by the search-index projection (MessageDeleted).
        self.append_and_project(
            &summary.id.to_string(),
            AggregateType::Message,
            left_mailbox(summary, local.id, views),
        )
        .await?;
        out.removed += 1;
        Ok(())
    }

    async fn reconcile_flags(
        &self,
        summary: &MessageSummary,
        remote: &[Flag],
        out: &mut Reconciled,
    ) -> Result<(), ApplicationError> {
        let flags_added: Vec<Flag> = remote
            .iter()
            .filter(|f| !summary.flags.contains(f))
            .cloned()
            .collect();
        let flags_removed: Vec<Flag> = summary
            .flags
            .iter()
            .filter(|f| !remote.contains(f))
            .cloned()
            .collect();
        if flags_added.is_empty() && flags_removed.is_empty() {
            return Ok(());
        }
        debug!(message_id = %summary.id, ?flags_added, ?flags_removed, "Reconciling flag diff");
        let event = DomainEvent::MessageFlagged {
            message_id: summary.id,
            flags_added,
            flags_removed,
        };
        self.append_and_project(&summary.id.to_string(), AggregateType::Message, event)
            .await?;
        out.flags_changed += 1;
        Ok(())
    }

    /// Ingest `ids` (newest first) like new mail, but quietly: they were here
    /// before, so nobody is notified of them again. At most [`RESTORE_CAP`] a
    /// sync; a batch the server fails on is skipped, the rest still come
    /// back, and the next sync tries the remainder.
    async fn restore(
        &self,
        provider: &Arc<PR::Provider>,
        local: &Mailbox,
        ids: &[RemoteId],
    ) -> Result<u32, ApplicationError> {
        let mut restored = 0;
        for batch in ids[..ids.len().min(RESTORE_CAP)].chunks(ENVELOPE_BATCH) {
            if self.is_cancelled() {
                break;
            }
            match self.fetch_envelopes(provider, local, batch).await? {
                Some(envelopes) => {
                    restored += self.ingest(provider, local, &envelopes, true).await?
                }
                None => {
                    warn!(path = %local.path, "Skipping the messages missing locally this sync")
                }
            }
        }
        Ok(restored)
    }
}
