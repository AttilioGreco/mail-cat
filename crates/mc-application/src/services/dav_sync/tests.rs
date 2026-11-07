//! The sync service against a real SQLite store and an in-memory server.

mod calendars;
mod commands;
mod conflicts;
mod fake;
mod pass;
mod radicale;

use std::sync::Arc;

use mc_domain::formats::{self, ContactEdit};
use mc_domain::{
    AccountId, AddressBook, Calendar, CalendarComponent, CollectionId, Contact, ContactId,
    DavReadRepository, DomainError, DomainEvent, EventStore, Origin, Ownership, PendingChange,
    PendingPush, RemoteBinding, SyncCursorStore,
};
use mc_infrastructure::{SqliteEventStore, SqliteRepository, persistence::SqliteDb};

use super::*;
use crate::handlers::dav::DavCommandHandler;
use fake::Fake;

pub(super) const BOOK: &str = "/book/";

pub(super) struct Env {
    _dir: tempfile::TempDir,
    pub svc: Arc<DavSyncService>,
    pub handler: DavCommandHandler,
    pub fake: Arc<Fake>,
    pub repo: Arc<SqliteRepository>,
    pub events: Arc<SqliteEventStore>,
    pub account: AccountId,
}

pub(super) fn env() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteDb::open(&dir.path().join("mail-cat.db")).unwrap();
    let events = Arc::new(SqliteEventStore::new(db.clone()));
    let repo = Arc::new(SqliteRepository::new(db, events.clone()));
    let account = AccountId::new();
    let fake = Arc::new(Fake::for_account(account));
    let providers = Arc::new(DavProviders::default());
    providers.register(account, fake.clone(), DavStatus::all_enabled());
    let svc = Arc::new(DavSyncService::new(repo.clone(), events.clone(), providers));
    Env {
        _dir: dir,
        handler: DavCommandHandler::new(svc.clone()),
        svc,
        fake,
        repo,
        events,
        account,
    }
}

pub(super) fn book(path: &str) -> AddressBook {
    AddressBook {
        account: AccountId::new(),
        collection: CollectionId::new(path),
        name: path.into(),
        description: None,
        read_only: false,
    }
}

pub(super) fn calendar(path: &str, components: Vec<CalendarComponent>) -> Calendar {
    Calendar {
        account: AccountId::new(),
        collection: CollectionId::new(path),
        name: path.into(),
        color: None,
        timezone: None,
        read_only: false,
        ownership: Ownership::Own,
        components,
    }
}

/// A card; `rev` is how the server dates it for last-write-wins.
pub(super) fn vcard(uid: &str, name: &str, rev: Option<&str>) -> String {
    let rev = rev.map(|r| format!("REV:{r}\r\n")).unwrap_or_default();
    format!(
        "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:{uid}\r\nFN:{name}\r\nEMAIL:{uid}@example.com\r\n{rev}END:VCARD\r\n"
    )
}

pub(super) const LONG_AGO: &str = "20200101T000000Z";
pub(super) const FAR_AHEAD: &str = "20990101T000000Z";

pub(super) fn connection_lost() -> DomainError {
    DomainError::ConnectionFailed("lost".into())
}

pub(super) fn server_error() -> DomainError {
    DomainError::Provider("500".into())
}

impl Env {
    /// An env whose server has the book, ready for a pass.
    pub fn with_book() -> Self {
        let env = env();
        env.fake.add_book(book(BOOK));
        env
    }

    pub async fn sync(&self) {
        self.svc.sync_account(&self.account).await.unwrap();
    }

    pub fn target(&self) -> Target {
        Target::new(Kind::Contacts, CollectionId::new(BOOK))
    }

    pub async fn pending(&self) -> Vec<PendingPush> {
        self.repo
            .pending_contacts(&self.account, &CollectionId::new(BOOK))
            .await
            .unwrap()
    }

    pub async fn contact_at(&self, href: &str) -> ContactId {
        self.repo
            .contact_at(&self.account, &Href::new(href))
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("no contact at {href}"))
    }

    pub async fn contact_count(&self) -> u64 {
        use mc_domain::ContactReadRepository;
        self.repo
            .list_contacts(&mc_shared::pagination::Pagination::new(1, 100))
            .await
            .unwrap()
            .total_count
    }

    pub async fn history(&self, id: ContactId) -> Vec<DomainEvent> {
        self.events
            .load_events(&id.to_string(), 0)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.payload)
            .collect()
    }

    pub async fn contact(&self, id: ContactId) -> Contact {
        Contact::rehydrate(&self.history(id).await).unwrap()
    }

    pub async fn lost(&self) -> u64 {
        self.repo.conflicts_lost(&self.account).await.unwrap()
    }

    pub async fn cursor(&self, key: &str) -> Option<String> {
        self.repo
            .get_cursor(&self.account, key)
            .await
            .unwrap()
            .map(|c| c.to_string())
    }

    /// What the user does: a stored `Local` edit, with no push.
    pub async fn edit_local(&self, id: ContactId, name: &str) {
        let history = self.events.load_events(&id.to_string(), 0).await.unwrap();
        let contact = self.contact(id).await;
        let raw = formats::edit_vcard(
            contact.raw.as_deref().unwrap(),
            &ContactEdit {
                display_name: Some(name.into()),
                ..ContactEdit::default()
            },
            chrono::Utc::now(),
        )
        .unwrap();
        let key = Key::contact(id);
        let (_, events) = resource_events(
            Kind::Contacts,
            Some(&key),
            contact.remote.unwrap(),
            &raw,
            Origin::Local,
        )
        .unwrap();
        self.svc
            .append(&key, Some(history.len() as u64), events)
            .await
            .unwrap();
    }

    /// A contact the user made, not yet sent anywhere.
    pub async fn create_local(&self, uid: &str, name: &str) -> ContactId {
        let binding = RemoteBinding {
            account: self.account,
            collection: CollectionId::new(BOOK),
            href: Href::new(format!("{BOOK}{uid}.vcf")),
            etag: None,
            remote_modified: None,
        };
        let (key, events) = resource_events(
            Kind::Contacts,
            None,
            binding,
            &vcard(uid, name, None),
            Origin::Local,
        )
        .unwrap();
        self.svc.append(&key, None, events).await.unwrap();
        key.contact.unwrap()
    }

    /// Waits for the fast path a command spawned.
    pub async fn settled(&self) {
        for _ in 0..200 {
            if self.pending().await.is_empty() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the fast path never pushed");
    }
}
