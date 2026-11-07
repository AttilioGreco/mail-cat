//! Contacts and calendars sync: one pass per account over every address book
//! and calendar, local edits pushed first and the server's changes pulled
//! after (spec §3, §4). Both resource kinds go through the same steps; only
//! the port, the payload format and the events differ, which `Kind` selects.

mod collections;
mod events;
mod pull;
mod push;
#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

use tracing::warn;

pub(crate) use events::resource_events;

use mc_domain::{
    AccountId, AggregateType, CalendarProvider, CollectionChanges, CollectionId, ContactId,
    ContactsProvider, DavReadRepository, DeleteOutcome, DomainError, Etag, EventStore, Href,
    PendingPush, PutOutcome, ReadModelProjector, RemoteResource, ResourcePut, SyncCursor,
    SyncCursorStore,
};

use crate::bus::EventPublisher;
use crate::error::ApplicationError;
use crate::services::account_locks::AccountLocks;
use crate::services::sync_service::stops_the_pass;

/// What the sync reads, writes and projects: the SQLite repository.
pub trait DavStore: ReadModelProjector + DavReadRepository + SyncCursorStore {}

impl<T: ReadModelProjector + DavReadRepository + SyncCursorStore> DavStore for T {}

/// One account's server, on whatever protocol it speaks.
pub trait DavBackend: ContactsProvider + CalendarProvider {}

impl<T: ContactsProvider + CalendarProvider> DavBackend for T {}

/// The DAV backend of each account that has one.
#[derive(Default)]
pub struct DavProviders(RwLock<HashMap<AccountId, Arc<dyn DavBackend>>>);

impl DavProviders {
    pub fn register(&self, account: AccountId, backend: Arc<dyn DavBackend>) {
        self.0
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(account, backend);
    }

    pub fn remove(&self, account: &AccountId) {
        self.0
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(account);
    }

    fn get(&self, account: &AccountId) -> Result<Arc<dyn DavBackend>, ApplicationError> {
        self.0
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(account)
            .cloned()
            .ok_or_else(|| ApplicationError::NotFound("DAV provider", account.to_string()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Contacts,
    Calendar,
}

/// One collection of one kind.
#[derive(Debug, Clone)]
pub(crate) struct Target {
    pub kind: Kind,
    pub collection: CollectionId,
}

impl Target {
    pub(crate) fn new(kind: Kind, collection: CollectionId) -> Self {
        Self { kind, collection }
    }

    /// Prefixed so a path used by both an address book and a calendar cannot
    /// share a cursor.
    fn cursor_key(&self) -> String {
        let prefix = match self.kind {
            Kind::Contacts => "carddav",
            Kind::Calendar => "caldav",
        };
        format!("{prefix}:{}", self.collection)
    }
}

/// The event stream a resource's events go to.
#[derive(Debug, Clone)]
pub(crate) struct Key {
    pub aggregate_id: String,
    pub ty: AggregateType,
    /// Set for a contact, which is keyed by its own id; an event by its href.
    pub contact: Option<ContactId>,
}

impl Key {
    pub(crate) fn contact(id: ContactId) -> Self {
        Self {
            aggregate_id: id.to_string(),
            ty: AggregateType::Contact,
            contact: Some(id),
        }
    }

    pub(crate) fn event(account: AccountId, href: &Href) -> Self {
        Self {
            aggregate_id: AggregateType::CalendarEvent.path_id(account, href.as_str()),
            ty: AggregateType::CalendarEvent,
            contact: None,
        }
    }

    pub(crate) fn of_pending(
        kind: Kind,
        account: AccountId,
        p: &PendingPush,
    ) -> Result<Self, DomainError> {
        match kind {
            Kind::Contacts => uuid::Uuid::parse_str(&p.aggregate_id)
                .map(|id| Self::contact(ContactId::from_uuid(id)))
                .map_err(|e| DomainError::Validation(format!("contact id: {e}"))),
            Kind::Calendar => Ok(Self::event(account, &p.binding.href)),
        }
    }
}

/// What one pass works with.
struct Ctx {
    account: AccountId,
    backend: Arc<dyn DavBackend>,
}

impl Ctx {
    async fn changes(
        &self,
        t: &Target,
        since: Option<&SyncCursor>,
    ) -> Result<CollectionChanges, DomainError> {
        match t.kind {
            Kind::Contacts => {
                self.backend
                    .address_book_changes(&t.collection, since)
                    .await
            }
            Kind::Calendar => self.backend.calendar_changes(&t.collection, since).await,
        }
    }

    async fn fetch(&self, t: &Target, hrefs: &[Href]) -> Result<Vec<RemoteResource>, DomainError> {
        match t.kind {
            Kind::Contacts => self.backend.fetch_contacts(&t.collection, hrefs).await,
            Kind::Calendar => self.backend.fetch_events(&t.collection, hrefs).await,
        }
    }

    async fn fetch_one(
        &self,
        t: &Target,
        href: &Href,
    ) -> Result<Option<RemoteResource>, DomainError> {
        Ok(self
            .fetch(t, std::slice::from_ref(href))
            .await?
            .into_iter()
            .next())
    }

    async fn put(&self, t: &Target, put: ResourcePut) -> Result<PutOutcome, DomainError> {
        match t.kind {
            Kind::Contacts => self.backend.put_contact(&t.collection, put).await,
            Kind::Calendar => self.backend.put_event(&t.collection, put).await,
        }
    }

    async fn delete(
        &self,
        t: &Target,
        href: &Href,
        if_match: &Etag,
    ) -> Result<DeleteOutcome, DomainError> {
        match t.kind {
            Kind::Contacts => {
                self.backend
                    .delete_contact(&t.collection, href, if_match)
                    .await
            }
            Kind::Calendar => {
                self.backend
                    .delete_event(&t.collection, href, if_match)
                    .await
            }
        }
    }
}

/// An error that would fail every other collection the same way.
fn stops(e: &ApplicationError) -> bool {
    matches!(e, ApplicationError::Domain(e) if stops_the_pass(e))
}

pub struct DavSyncService {
    pub(crate) store: Arc<dyn DavStore>,
    pub(crate) events: Arc<dyn EventStore>,
    providers: Arc<DavProviders>,
    publisher: Option<Arc<dyn EventPublisher>>,
    /// Held while resources are pushed, so the fast path of a local edit and
    /// the sync pass never send the same one at once.
    push_locks: AccountLocks,
    /// Listings that came back empty while local data exists, keyed by
    /// account and listing. One is not believed; the next in a row is.
    empty_once: Mutex<HashSet<String>>,
}

impl DavSyncService {
    pub fn new(
        store: Arc<dyn DavStore>,
        events: Arc<dyn EventStore>,
        providers: Arc<DavProviders>,
    ) -> Self {
        Self {
            store,
            events,
            providers,
            publisher: None,
            push_locks: AccountLocks::default(),
            empty_once: Mutex::default(),
        }
    }

    /// Announces every stored event to in-process subscribers.
    #[must_use]
    pub fn with_event_publisher(mut self, publisher: Arc<dyn EventPublisher>) -> Self {
        self.publisher = Some(publisher);
        self
    }

    /// Drops the bookkeeping of a removed account.
    pub fn forget(&self, account: &AccountId) {
        self.push_locks.forget(account);
        self.providers.remove(account);
    }

    /// One pass over the account's address books and calendars. Failing
    /// login, connection or throttling stops it, with the cursors of the
    /// collections not reached untouched; any other failure skips just the
    /// collection it hit.
    pub async fn sync_account(&self, account: &AccountId) -> Result<(), ApplicationError> {
        let ctx = Ctx {
            account: *account,
            backend: self.providers.get(account)?,
        };
        for kind in [Kind::Contacts, Kind::Calendar] {
            let Some(listed) = self.reconcile_collections(&ctx, kind).await? else {
                continue;
            };
            for collection in listed.into_iter().filter(|c| c.syncs) {
                let target = Target::new(kind, collection.id);
                match self
                    .sync_collection(&ctx, &target, collection.read_only)
                    .await
                {
                    Err(e) if stops(&e) => return Err(e),
                    Err(e) => warn!(collection = %target.collection, "DAV sync skipped: {e}"),
                    Ok(()) => {}
                }
            }
        }
        Ok(())
    }

    /// Push before pull: a pull must never overwrite an edit the server has
    /// not seen. A read-only collection cannot take the edits, so they lose
    /// to the server's copy instead.
    async fn sync_collection(
        &self,
        ctx: &Ctx,
        target: &Target,
        read_only: bool,
    ) -> Result<(), ApplicationError> {
        // A create sent by POST that the server stored before a crash lost its
        // acknowledgement can only be told apart by UID, which the pull binds:
        // such creates wait until after it, or a retry would duplicate them.
        let creates_after_pull =
            matches!(target.kind, Kind::Contacts) && ctx.backend.creates_by_post();
        {
            let _pushing = self.push_locks.lock(&ctx.account).await;
            let mut pending = self.pending(ctx, target).await?;
            if read_only {
                self.discard_pending(ctx, target, pending).await?;
            } else {
                if creates_after_pull {
                    pending.retain(|p| p.binding.etag.is_some());
                }
                self.push_all(ctx, target, pending, None).await?;
            }
        }
        self.pull(ctx, target).await?;
        if read_only || !creates_after_pull {
            return Ok(());
        }
        let _pushing = self.push_locks.lock(&ctx.account).await;
        let creates = self
            .pending(ctx, target)
            .await?
            .into_iter()
            .filter(|p| p.binding.etag.is_none())
            .collect();
        self.push_all(ctx, target, creates, None).await
    }

    /// Pushes the edits of one collection now, without a pass: what a local
    /// edit does after it is stored. `only` names the one aggregate to push.
    /// Whatever fails is retried by the next pass.
    pub(crate) async fn push_now(
        &self,
        account: &AccountId,
        target: &Target,
        only: &str,
    ) -> Result<(), ApplicationError> {
        let ctx = Ctx {
            account: *account,
            backend: self.providers.get(account)?,
        };
        let _pushing = self.push_locks.lock(account).await;
        // Under the lock: a pass may have pushed it while this waited.
        let pending = self.pending(&ctx, target).await?;
        self.push_all(&ctx, target, pending, Some(only)).await
    }

    /// Whether an empty listing of `what` is to be acted on. A server that
    /// glitches answers empty once; acting on it would drop every local copy
    /// and, with the cursor moved, keep them dropped. Two in a row is a
    /// collection that really was emptied. Anything non-empty resets it.
    pub(super) fn believe_empty(&self, account: &AccountId, what: &str, empty: bool) -> bool {
        let key = format!("{account}:{what}");
        let mut seen = self.empty_once.lock().unwrap_or_else(|e| e.into_inner());
        if !empty {
            seen.remove(&key);
            return true;
        }
        !seen.insert(key)
    }

    async fn pending(&self, ctx: &Ctx, t: &Target) -> Result<Vec<PendingPush>, DomainError> {
        match t.kind {
            Kind::Contacts => {
                self.store
                    .pending_contacts(&ctx.account, &t.collection)
                    .await
            }
            Kind::Calendar => self.store.pending_events(&ctx.account, &t.collection).await,
        }
    }

    async fn etags(&self, ctx: &Ctx, t: &Target) -> Result<HashMap<Href, Etag>, DomainError> {
        match t.kind {
            Kind::Contacts => self.store.contact_etags(&ctx.account, &t.collection).await,
            Kind::Calendar => self.store.event_etags(&ctx.account, &t.collection).await,
        }
    }
}
