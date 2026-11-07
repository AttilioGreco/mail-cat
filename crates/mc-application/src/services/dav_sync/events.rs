//! Writing the sync's results: events appended and projected, the payloads
//! behind them, and the transient push failure that is projected only.

use chrono::{DateTime, Utc};

use mc_domain::{
    AccountId, AggregateType, CalendarEvent, CollectionId, ContactId, ContactSource, DomainError,
    DomainEvent, Etag, EventEnvelope, Href, Origin, PendingChange, PendingPush, RemoteBinding,
    RemoteCard, RemoteResource, formats,
};

use super::{Ctx, DavSyncService, Key, Kind, Target};
use crate::error::ApplicationError;

impl Key {
    pub(crate) fn collection(ty: AggregateType, account: AccountId, id: &CollectionId) -> Self {
        Self {
            aggregate_id: ty.path_id(account, id.as_str()),
            ty,
            contact: None,
        }
    }
}

impl Ctx {
    pub(super) fn collection_removed(&self, t: &Target) -> DomainEvent {
        let (account_id, collection) = (self.account, t.collection.clone());
        match t.kind {
            Kind::Contacts => DomainEvent::AddressBookRemoved {
                account_id,
                collection,
            },
            Kind::Calendar => DomainEvent::CalendarRemoved {
                account_id,
                collection,
            },
        }
    }
}

/// The time the server's copy says it was last written, for last-write-wins.
pub(super) fn modified_at(kind: Kind, raw: &str) -> Option<DateTime<Utc>> {
    match kind {
        Kind::Contacts => formats::parse_vcard(raw).ok()?.revised,
        Kind::Calendar => formats::parse_event(raw).ok()?.last_modified,
    }
}

pub(super) fn uid_of(kind: Kind, raw: &str) -> Option<String> {
    match kind {
        Kind::Contacts => formats::parse_vcard(raw).ok()?.uid,
        Kind::Calendar => formats::parse_event(raw).ok()?.uid,
    }
}

/// The server and the local copy often differ only in line endings and a
/// trailing newline.
pub(super) fn same_body(a: &str, b: &str) -> bool {
    a.replace("\r\n", "\n").trim_end() == b.replace("\r\n", "\n").trim_end()
}

impl DavSyncService {
    /// Appends to the resource's stream and projects each event under its
    /// own sequence number. `expected` is the sequence the decision was taken
    /// at: a local edit made since fails the append instead of being
    /// overwritten. `None` appends at whatever the stream is at.
    pub(crate) async fn append(
        &self,
        key: &Key,
        expected: Option<u64>,
        events: Vec<DomainEvent>,
    ) -> Result<(), ApplicationError> {
        let expected = match expected {
            Some(sequence) => sequence,
            None => self.events.current_sequence(&key.aggregate_id).await?,
        };
        let last = self
            .events
            .append(&key.aggregate_id, key.ty, expected, events.clone())
            .await?;
        let first = last + 1 - events.len() as u64;
        for (sequence, event) in (first..).zip(events) {
            let envelope = EventEnvelope::new(key.aggregate_id.clone(), key.ty, sequence, event);
            self.store.project(&envelope).await?;
            if let Some(publisher) = &self.publisher {
                publisher.publish(envelope);
            }
        }
        Ok(())
    }

    /// Projected and announced, never stored: a refused push is retried every
    /// pass, and a stored record of each would grow the log without bound.
    pub(super) async fn record_push_failure(
        &self,
        ctx: &Ctx,
        kind: Kind,
        p: &PendingPush,
        reason: String,
    ) -> Result<(), ApplicationError> {
        let key = Key::of_pending(kind, ctx.account, p)?;
        let event = match key.contact {
            Some(contact_id) => DomainEvent::ContactPushFailed { contact_id, reason },
            None => DomainEvent::CalendarEventPushFailed {
                account_id: ctx.account,
                href: p.binding.href.clone(),
                reason,
            },
        };
        let envelope = EventEnvelope::new(key.aggregate_id, key.ty, 0, event);
        self.store.project(&envelope).await?;
        if let Some(publisher) = &self.publisher {
            publisher.publish(envelope);
        }
        Ok(())
    }

    /// The local edit `p` loses to the server's copy. A deletion has no
    /// payload to keep; the stream still holds the removal.
    pub(super) async fn record_loss(
        &self,
        ctx: &Ctx,
        kind: Kind,
        p: &PendingPush,
        expected: Option<u64>,
    ) -> Result<(), ApplicationError> {
        let key = Key::of_pending(kind, ctx.account, p)?;
        let local_raw = match &p.change {
            PendingChange::Put { raw } => raw.clone(),
            PendingChange::Delete => String::new(),
        };
        let event = match key.contact {
            Some(contact_id) => DomainEvent::ContactConflictLost {
                contact_id,
                local_raw,
            },
            None => DomainEvent::CalendarEventConflictLost {
                account_id: ctx.account,
                href: p.binding.href.clone(),
                local_raw,
            },
        };
        self.append(&key, expected, vec![event]).await
    }

    /// The stream's sequence while `p` is still the latest local edit, `None`
    /// once a newer one landed: an answer about an older edit must not
    /// replace it. Appending at the returned sequence closes the rest.
    pub(super) async fn still_pending(
        &self,
        ctx: &Ctx,
        t: &Target,
        p: &PendingPush,
    ) -> Result<Option<u64>, ApplicationError> {
        let latest = self
            .pending(ctx, t)
            .await?
            .into_iter()
            .find(|q| q.aggregate_id == p.aggregate_id);
        if latest.is_none_or(|q| q.sequence != p.sequence) {
            return Ok(None);
        }
        Ok(Some(self.events.current_sequence(&p.aggregate_id).await?))
    }

    /// The server acknowledged the local version at `sequence`.
    pub(super) fn pushed_event(
        &self,
        ctx: &Ctx,
        key: &Key,
        href: Href,
        etag: Etag,
        local_sequence: u64,
    ) -> DomainEvent {
        match key.contact {
            Some(contact_id) => DomainEvent::ContactPushedToServer {
                contact_id,
                href,
                etag,
                local_sequence,
            },
            None => DomainEvent::CalendarEventPushedToServer {
                account_id: ctx.account,
                href,
                etag,
                local_sequence,
            },
        }
    }

    /// The server removed the resource, or its copy replaces the local one.
    pub(super) fn removed_event(&self, ctx: &Ctx, key: &Key, href: &Href) -> DomainEvent {
        match key.contact {
            Some(contact_id) => DomainEvent::ContactRemoved {
                contact_id,
                origin: Origin::Remote,
            },
            None => DomainEvent::CalendarEventRemoved {
                account_id: ctx.account,
                href: href.clone(),
                origin: Origin::Remote,
            },
        }
    }

    /// Stores the server's copy over `existing`, or as a new resource.
    /// `expected` as in [`Self::append`]. A payload that does not parse is
    /// skipped with a warning: one broken card must not stall the collection.
    pub(super) async fn apply_remote(
        &self,
        ctx: &Ctx,
        t: &Target,
        existing: Option<&Key>,
        expected: Option<u64>,
        remote: &RemoteResource,
    ) -> Result<(), ApplicationError> {
        let binding = RemoteBinding {
            account: ctx.account,
            collection: t.collection.clone(),
            href: remote.href.clone(),
            etag: Some(remote.etag.clone()),
            remote_modified: modified_at(t.kind, &remote.raw),
        };
        let (key, events) =
            resource_events(t.kind, existing, binding, &remote.raw, Origin::Remote)?;
        self.append(&key, expected.filter(|_| existing.is_some()), events)
            .await
    }
}

/// A local edit landed between the decision and its append: it stays
/// pending, and the next pass decides again.
pub(super) fn superseded(result: Result<(), ApplicationError>) -> Result<(), ApplicationError> {
    match result {
        Err(ApplicationError::Domain(DomainError::ConcurrencyConflict { .. })) => Ok(()),
        result => result,
    }
}

/// The events that store `raw` as the resource's new state: over `existing`,
/// or as a new resource. The read model's fields come from the payload.
pub(crate) fn resource_events(
    kind: Kind,
    existing: Option<&Key>,
    binding: RemoteBinding,
    raw: &str,
    origin: Origin,
) -> Result<(Key, Vec<DomainEvent>), DomainError> {
    if kind == Kind::Calendar {
        let key = Key::event(binding.account, &binding.href);
        let event = CalendarEvent {
            fields: formats::parse_event(raw)?.fields,
            binding,
            raw: raw.to_owned(),
        };
        let event = match existing {
            Some(_) => DomainEvent::CalendarEventUpdated { event, origin },
            None => DomainEvent::CalendarEventAdded { event, origin },
        };
        return Ok((key, vec![event]));
    }
    let card = formats::parse_vcard(raw)?;
    let account = binding.account;
    let remote = RemoteCard {
        binding,
        raw: raw.to_owned(),
        origin,
    };
    if let Some(key) = existing {
        let event = DomainEvent::ContactUpdated {
            contact_id: key.contact.expect("a contact key"),
            display_name: Some(card.display_name),
            emails: Some(card.emails),
            phones: Some(card.phones),
            organization: Some(card.organization),
            notes: Some(card.notes),
            remote: Some(remote),
        };
        return Ok((key.clone(), vec![event]));
    }
    let contact_id = ContactId::new();
    let mut events = vec![DomainEvent::ContactAdded {
        contact_id,
        display_name: card.display_name,
        emails: card.emails,
        source: ContactSource::CardDav,
        external_id: None,
        account_scope: Some(account),
        remote: Some(remote),
    }];
    // ContactAdded carries no organization, which autocomplete shows.
    if card.organization.is_some() {
        events.push(DomainEvent::ContactUpdated {
            contact_id,
            display_name: None,
            emails: None,
            phones: Some(card.phones),
            organization: Some(card.organization),
            notes: Some(card.notes),
            remote: None,
        });
    }
    Ok((Key::contact(contact_id), events))
}
