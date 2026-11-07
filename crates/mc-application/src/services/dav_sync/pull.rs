//! The server's changes to local: fetch what differs, apply it, and move the
//! cursor last. A crash or a failure redoes the pass from the old cursor, and
//! every step is safe to repeat: a resource whose etag equals the local one
//! produces no event.

use std::collections::{HashMap, HashSet};

use tracing::warn;

use mc_domain::{
    DomainError, DomainEvent, Href, Origin, PendingChange, PendingPush, RemoteBinding, RemoteCard,
    RemoteResource,
};

use super::events::{modified_at, same_body, uid_of};
use super::{Ctx, DavSyncService, Key, Kind, Target};
use crate::error::ApplicationError;

/// Hrefs per multiget request.
const FETCH_CHUNK: usize = 50;

impl DavSyncService {
    pub(super) async fn pull(&self, ctx: &Ctx, t: &Target) -> Result<(), ApplicationError> {
        let cursor = t.cursor_key();
        let since = self.store.get_cursor(&ctx.account, &cursor).await?;
        let changes = ctx.changes(t, since.as_ref()).await?;
        let local = self.etags(ctx, t).await?;

        let stale: Vec<Href> = changes
            .changed
            .iter()
            .filter(|(href, etag)| local.get(href) != Some(etag))
            .map(|(href, _)| href.clone())
            .collect();
        // A reset lists everything the server has: what is bound locally and
        // absent from it is gone. Never-pushed resources have no etag, so a
        // local create is never taken for one.
        let gone: Vec<Href> = if changes.reset {
            let listed: HashSet<_> = changes.changed.iter().map(|(href, _)| href).collect();
            local
                .keys()
                .filter(|href| !listed.contains(href))
                .cloned()
                .collect()
        } else {
            // A resource deleted and created again may be reported as both.
            let changed: HashSet<_> = changes.changed.iter().map(|(href, _)| href).collect();
            changes
                .removed
                .iter()
                .filter(|href| local.contains_key(*href) && !changed.contains(href))
                .cloned()
                .collect()
        };

        // Sequences before the list of unpushed edits: an edit made after
        // either read makes the append below fail instead of being overwritten.
        let mut known = HashMap::new();
        for href in stale.iter().chain(&gone) {
            if let Some(key) = self.key_at(ctx, t, href, &local).await? {
                let sequence = self.events.current_sequence(&key.aggregate_id).await?;
                known.insert(href.clone(), (key, sequence));
            }
        }
        let pending = self.pending(ctx, t).await?;
        // What is still unpushed after the push phase wins over the pull: its
        // push settles it against the server's copy (a refused precondition).
        let unpushed: HashSet<_> = pending.iter().map(|p| &p.binding.href).collect();
        let mut creates = self.unacknowledged_creates(t, &pending);

        for href in gone.iter().filter(|href| !unpushed.contains(href)) {
            if let Some((key, sequence)) = known.get(href) {
                let removed = self.removed_event(ctx, key, href);
                self.append(key, Some(*sequence), vec![removed]).await?;
            }
        }
        for chunk in stale.chunks(FETCH_CHUNK) {
            for remote in ctx.fetch(t, chunk).await? {
                if unpushed.contains(&remote.href) {
                    continue;
                }
                let slot = known.get(&remote.href);
                if slot.is_none()
                    && let Some(create) =
                        uid_of(t.kind, &remote.raw).and_then(|uid| creates.remove(&uid))
                {
                    self.bind_to_create(ctx, t, create, &remote).await?;
                    continue;
                }
                let stored = self
                    .apply_remote(
                        ctx,
                        t,
                        slot.map(|(key, _)| key),
                        slot.map(|(_, sequence)| *sequence),
                        &remote,
                    )
                    .await;
                match stored {
                    Err(ApplicationError::Domain(e @ DomainError::Validation(_))) => {
                        warn!(href = %remote.href, "skipping an unreadable resource: {e}");
                    }
                    other => other?,
                }
            }
        }

        self.store
            .put_cursor(&ctx.account, &cursor, &changes.cursor)
            .await?;
        Ok(())
    }

    async fn key_at(
        &self,
        ctx: &Ctx,
        t: &Target,
        href: &Href,
        local: &HashMap<Href, mc_domain::Etag>,
    ) -> Result<Option<Key>, DomainError> {
        Ok(match t.kind {
            Kind::Contacts => self
                .store
                .contact_at(&ctx.account, href)
                .await?
                .map(Key::contact),
            Kind::Calendar => local
                .contains_key(href)
                .then(|| Key::event(ctx.account, href)),
        })
    }

    /// Contacts the user made that the server never acknowledged, by UID.
    /// Events are not matched: a create is a PUT at the href chosen here, so
    /// a retry finds it by href.
    fn unacknowledged_creates<'a>(
        &self,
        t: &Target,
        pending: &'a [PendingPush],
    ) -> HashMap<String, &'a PendingPush> {
        if t.kind != Kind::Contacts {
            return HashMap::new();
        }
        pending
            .iter()
            .filter(|p| p.binding.etag.is_none())
            .filter_map(|p| match &p.change {
                PendingChange::Put { raw } => Some((uid_of(t.kind, raw)?, p)),
                PendingChange::Delete => None,
            })
            .collect()
    }

    /// The server holds this create under its own href (Google picks one, and
    /// a crash lost the acknowledgement): the local contact is that resource,
    /// not a second one.
    async fn bind_to_create(
        &self,
        ctx: &Ctx,
        t: &Target,
        create: &PendingPush,
        remote: &RemoteResource,
    ) -> Result<(), ApplicationError> {
        let PendingChange::Put { raw } = &create.change else {
            return Ok(());
        };
        if same_body(&remote.raw, raw) {
            return self
                .acknowledge(
                    ctx,
                    t,
                    create,
                    remote.href.clone(),
                    remote.etag.clone(),
                    None,
                )
                .await;
        }
        // Edited after the create, or rewritten by the server: the local text
        // stays the one to push, now on top of the server's version.
        let key = Key::of_pending(t.kind, ctx.account, create)?;
        let event = DomainEvent::ContactUpdated {
            contact_id: key.contact.expect("a contact key"),
            display_name: None,
            emails: None,
            phones: None,
            organization: None,
            notes: None,
            remote: Some(RemoteCard {
                binding: RemoteBinding {
                    account: ctx.account,
                    collection: t.collection.clone(),
                    href: remote.href.clone(),
                    etag: Some(remote.etag.clone()),
                    remote_modified: modified_at(t.kind, &remote.raw),
                },
                raw: raw.clone(),
                origin: Origin::Local,
            }),
        };
        self.append(&key, None, vec![event]).await
    }
}
