//! An in-memory DAV server behind both ports, with the semantics the sync
//! relies on (etags, preconditions, a delta cursor) and hooks to make it
//! misbehave on purpose.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

use async_trait::async_trait;

use mc_domain::{
    AccountId, AddressBook, Calendar, CalendarProvider, CollectionChanges, CollectionId,
    ContactsProvider, DeleteOutcome, DomainError, Etag, Href, PutOutcome, RemoteResource,
    ResourcePut, Subscribed, SyncCursor,
};

#[derive(Clone, Copy)]
pub enum Script {
    /// The call fails before the server does anything.
    Fail(fn() -> DomainError),
    /// The server stores the write and the answer is lost: the process
    /// dying before it records the acknowledgement looks the same.
    StoreThenFail(fn() -> DomainError),
}

struct Stored {
    etag: Etag,
    raw: String,
    version: u64,
}

#[derive(Default)]
struct State {
    account: Option<AccountId>,
    books: Vec<AddressBook>,
    calendars: Vec<Calendar>,
    resources: BTreeMap<Href, Stored>,
    tombstones: Vec<(u64, Href)>,
    version: u64,
    created: u64,
    /// The next cursor is refused as expired, once.
    expire_token: bool,
    /// Creates are POSTed and named by the server, which also rewrites bodies.
    google: bool,
    puts: VecDeque<Script>,
    changes_fail: Option<(CollectionId, fn() -> DomainError)>,
    calls: Vec<String>,
    /// What `subscribe` answers.
    subscription: Option<Subscribed>,
}

#[derive(Default)]
pub struct Fake(Mutex<State>);

impl Fake {
    pub fn for_account(account: AccountId) -> Self {
        Self(Mutex::new(State {
            account: Some(account),
            ..State::default()
        }))
    }

    fn with<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        f(&mut self.0.lock().unwrap())
    }

    pub fn add_book(&self, mut book: AddressBook) {
        self.with(|s| {
            book.account = s.account.unwrap();
            s.books.push(book);
        });
    }

    pub fn add_calendar(&self, mut calendar: Calendar) {
        self.with(|s| {
            calendar.account = s.account.unwrap();
            s.calendars.push(calendar);
        });
    }

    pub fn subscribes_to(&self, subscribed: Subscribed) {
        self.with(|s| s.subscription = Some(subscribed));
    }

    pub fn remove_book(&self, collection: &str) {
        self.with(|s| s.books.retain(|b| b.collection.as_str() != collection));
    }

    pub fn set_book_read_only(&self, collection: &str) {
        self.with(|s| {
            for b in s
                .books
                .iter_mut()
                .filter(|b| b.collection.as_str() == collection)
            {
                b.read_only = true;
            }
        });
    }

    /// Somebody else's write: stores `raw` and moves the etag.
    pub fn write(&self, href: &str, raw: &str) -> Etag {
        self.with(|s| s.store(&Href::new(href), raw.to_owned()))
    }

    pub fn remove(&self, href: &str) {
        self.with(|s| s.remove(&Href::new(href)));
    }

    pub fn raw(&self, href: &str) -> Option<String> {
        self.with(|s| s.resources.get(&Href::new(href)).map(|r| r.raw.clone()))
    }

    pub fn hrefs(&self) -> Vec<String> {
        self.with(|s| s.resources.keys().map(|h| h.to_string()).collect())
    }

    pub fn expire_token(&self) {
        self.with(|s| s.expire_token = true);
    }

    pub fn google(&self) {
        self.with(|s| s.google = true);
    }

    pub fn script_put(&self, script: Script) {
        self.with(|s| s.puts.push_back(script));
    }

    pub fn fail_changes(&self, collection: &str, error: fn() -> DomainError) {
        self.with(|s| s.changes_fail = Some((CollectionId::new(collection), error)));
    }

    pub fn calls(&self) -> Vec<String> {
        self.with(|s| s.calls.clone())
    }

    pub fn calls_to(&self, prefix: &str) -> usize {
        self.calls()
            .iter()
            .filter(|c| c.starts_with(prefix))
            .count()
    }

    async fn changes(
        &self,
        collection: &CollectionId,
        since: Option<&SyncCursor>,
    ) -> Result<CollectionChanges, DomainError> {
        self.with(|s| {
            s.calls.push(format!("changes {collection}"));
            if let Some((failing, error)) = &s.changes_fail
                && failing == collection
            {
                return Err(error());
            }
            let cursor = SyncCursor::new(format!("v{}", s.version));
            let seen = since
                .and_then(|c| c.as_str().strip_prefix('v')?.parse::<u64>().ok())
                .filter(|_| !std::mem::take(&mut s.expire_token));
            let inside = |h: &Href| h.as_str().starts_with(collection.as_str());
            let (changed, removed) = match seen {
                Some(seen) => (
                    s.resources
                        .iter()
                        .filter(|(h, r)| inside(h) && r.version > seen)
                        .map(|(h, r)| (h.clone(), r.etag.clone()))
                        .collect(),
                    s.tombstones
                        .iter()
                        .filter(|(v, h)| *v > seen && inside(h))
                        .map(|(_, h)| h.clone())
                        .collect(),
                ),
                None => (
                    s.resources
                        .iter()
                        .filter(|(h, _)| inside(h))
                        .map(|(h, r)| (h.clone(), r.etag.clone()))
                        .collect(),
                    Vec::new(),
                ),
            };
            Ok(CollectionChanges {
                cursor,
                changed,
                removed,
                reset: seen.is_none(),
            })
        })
    }

    async fn fetch(&self, hrefs: &[Href]) -> Vec<RemoteResource> {
        self.with(|s| {
            s.calls.push(format!("fetch {}", hrefs.len()));
            hrefs
                .iter()
                .filter_map(|h| {
                    let r = s.resources.get(h)?;
                    Some(RemoteResource {
                        href: h.clone(),
                        etag: r.etag.clone(),
                        raw: r.raw.clone(),
                    })
                })
                .collect()
        })
    }

    async fn put(
        &self,
        collection: &CollectionId,
        put: ResourcePut,
    ) -> Result<PutOutcome, DomainError> {
        // A round trip, long enough for a concurrent caller to get in.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        self.with(|s| {
            s.calls.push(format!("put {}", put.href));
            let script = s.puts.pop_front();
            if let Some(Script::Fail(error)) = script {
                return Err(error());
            }
            let href = if put.if_match.is_none() && s.google {
                s.created += 1;
                Href::new(format!("{collection}server-{}.vcf", s.created))
            } else {
                put.href.clone()
            };
            let current = s.resources.get(&href).map(|r| &r.etag);
            if current != put.if_match.as_ref() {
                return Ok(PutOutcome::PreconditionFailed);
            }
            let rewritten = s.google.then(|| rewrite(&put.raw));
            let etag = s.store(&href, rewritten.clone().unwrap_or(put.raw));
            match script {
                Some(Script::StoreThenFail(error)) => Err(error()),
                _ => Ok(PutOutcome::Stored {
                    href,
                    etag,
                    raw: rewritten,
                }),
            }
        })
    }

    fn delete_if_match(&self, href: &Href, if_match: &Etag) -> DeleteOutcome {
        self.with(|s| {
            s.calls.push(format!("delete {href}"));
            match s.resources.get(href) {
                None => DeleteOutcome::Gone,
                Some(r) if r.etag != *if_match => DeleteOutcome::PreconditionFailed,
                Some(_) => {
                    s.remove(href);
                    DeleteOutcome::Deleted
                }
            }
        })
    }
}

/// What a server that normalizes bodies does to one.
fn rewrite(raw: &str) -> String {
    raw.replace("END:VCARD", "X-SERVER:1\r\nEND:VCARD")
}

impl State {
    fn store(&mut self, href: &Href, raw: String) -> Etag {
        self.version += 1;
        let etag = Etag::new(format!("\"e{}\"", self.version));
        let stored = Stored {
            etag: etag.clone(),
            raw,
            version: self.version,
        };
        self.resources.insert(href.clone(), stored);
        // Recreated after a deletion: the server reports the resource, not its removal.
        self.tombstones.retain(|(_, h)| h != href);
        etag
    }

    fn remove(&mut self, href: &Href) {
        if self.resources.remove(href).is_some() {
            self.version += 1;
            self.tombstones.push((self.version, href.clone()));
        }
    }
}

#[async_trait]
impl ContactsProvider for Fake {
    fn creates_by_post(&self) -> bool {
        self.with(|s| s.google)
    }

    async fn list_address_books(&self) -> Result<Vec<AddressBook>, DomainError> {
        Ok(self.with(|s| {
            s.calls.push("list books".into());
            s.books.clone()
        }))
    }

    async fn address_book_changes(
        &self,
        book: &CollectionId,
        since: Option<&SyncCursor>,
    ) -> Result<CollectionChanges, DomainError> {
        self.changes(book, since).await
    }

    async fn fetch_contacts(
        &self,
        _book: &CollectionId,
        hrefs: &[Href],
    ) -> Result<Vec<RemoteResource>, DomainError> {
        Ok(self.fetch(hrefs).await)
    }

    async fn put_contact(
        &self,
        book: &CollectionId,
        put: ResourcePut,
    ) -> Result<PutOutcome, DomainError> {
        self.put(book, put).await
    }

    async fn delete_contact(
        &self,
        _book: &CollectionId,
        href: &Href,
        if_match: &Etag,
    ) -> Result<DeleteOutcome, DomainError> {
        Ok(self.delete_if_match(href, if_match))
    }
}

#[async_trait]
impl CalendarProvider for Fake {
    async fn list_calendars(&self) -> Result<Vec<Calendar>, DomainError> {
        Ok(self.with(|s| {
            s.calls.push("list calendars".into());
            s.calendars.clone()
        }))
    }

    async fn calendar_changes(
        &self,
        calendar: &CollectionId,
        since: Option<&SyncCursor>,
    ) -> Result<CollectionChanges, DomainError> {
        self.changes(calendar, since).await
    }

    async fn fetch_events(
        &self,
        _calendar: &CollectionId,
        hrefs: &[Href],
    ) -> Result<Vec<RemoteResource>, DomainError> {
        Ok(self.fetch(hrefs).await)
    }

    async fn put_event(
        &self,
        calendar: &CollectionId,
        put: ResourcePut,
    ) -> Result<PutOutcome, DomainError> {
        self.put(calendar, put).await
    }

    async fn delete_event(
        &self,
        _calendar: &CollectionId,
        href: &Href,
        if_match: &Etag,
    ) -> Result<DeleteOutcome, DomainError> {
        Ok(self.delete_if_match(href, if_match))
    }

    async fn subscribe(&self, _email: &str) -> Result<Subscribed, DomainError> {
        self.with(|s| s.subscription.clone())
            .ok_or_else(|| DomainError::NotFound("nobody".into()))
    }

    async fn unsubscribe(&self, calendar: &CollectionId) -> Result<(), DomainError> {
        self.with(|s| s.calls.push(format!("unsubscribe {calendar}")));
        Ok(())
    }
}
