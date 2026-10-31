use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::events::DomainEvent;
use crate::model::account::AccountId;
use crate::value_objects::flag::Flag;
use crate::value_objects::mailbox_path::MailboxPath;

/// Unique mailbox identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MailboxId(Uuid);

impl MailboxId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for MailboxId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MailboxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for MailboxId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

/// Mailbox type classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MailboxType {
    Inbox,
    Sent,
    Drafts,
    Trash,
    Spam,
    Archive,
    /// RFC 6154 `\All` — virtual "everything" folder. Gmail ships this instead of
    /// an Archive folder, so it is the archive target when `Archive` is absent.
    All,
    /// Gmail's `\Important` label: copies of messages also in INBOX or `\All`.
    Important,
    /// `\Flagged` special-use: messages the user starred (Gmail's Starred).
    Starred,
    Custom,
}

impl MailboxType {
    /// Mail the user sends, drafts, receives or archives lives here: hiding
    /// the mailbox would hide that mail, so it is always subscribed.
    pub fn is_mandatory(self) -> bool {
        matches!(
            self,
            Self::Inbox | Self::Sent | Self::Drafts | Self::Archive | Self::All
        )
    }
}

/// Names servers give the system folders when they declare no role, lowercase.
/// The one place language lives: used only where the server said nothing.
const KNOWN_NAMES: &[(MailboxType, &[&str])] = &[
    (MailboxType::Inbox, &["posta in arrivo"]),
    (
        MailboxType::Sent,
        &[
            "sent",
            "sent items",
            "sent mail",
            "sent messages",
            "posta inviata",
            "inviata",
            "inviati",
            "elementi inviati",
            "messaggi inviati",
            "gesendet",
            "gesendete elemente",
            "gesendete objekte",
            "envoyé",
            "envoyés",
            "éléments envoyés",
            "messages envoyés",
            "enviado",
            "enviados",
            "elementos enviados",
            "correo enviado",
            "itens enviados",
            "mensagens enviadas",
            "verzonden",
            "verzonden items",
            "verzonden berichten",
        ],
    ),
    (
        MailboxType::Drafts,
        &[
            "draft",
            "drafts",
            "bozza",
            "bozze",
            "entwurf",
            "entwürfe",
            "brouillon",
            "brouillons",
            "borrador",
            "borradores",
            "rascunho",
            "rascunhos",
            "concept",
            "concepten",
        ],
    ),
    (
        MailboxType::Trash,
        &[
            "trash",
            "bin",
            "deleted",
            "deleted items",
            "deleted messages",
            "cestino",
            "posta eliminata",
            "elementi eliminati",
            "eliminati",
            "papierkorb",
            "gelöscht",
            "gelöschte elemente",
            "gelöschte objekte",
            "corbeille",
            "éléments supprimés",
            "papelera",
            "papelera de reciclaje",
            "elementos eliminados",
            "lixeira",
            "itens excluídos",
            "itens apagados",
            "prullenbak",
            "verwijderde items",
        ],
    ),
    (
        MailboxType::Spam,
        &[
            "spam",
            "junk",
            "junk e-mail",
            "junk email",
            "junk mail",
            "bulk mail",
            "posta indesiderata",
            "indesiderata",
            "indesiderati",
            "junk-e-mail",
            "pourriel",
            "pourriels",
            "courrier indésirable",
            "correo no deseado",
            "no deseado",
            "lixo eletrónico",
            "lixo eletrônico",
            "ongewenst",
            "ongewenste e-mail",
        ],
    ),
    (
        MailboxType::Archive,
        &[
            "archive",
            "archives",
            "archivio",
            "archiviati",
            "archiv",
            "archief",
            "archivo",
            "archivados",
            "arquivo",
            "arquivados",
        ],
    ),
];

impl MailboxType {
    /// The system role a folder name says (whole name, any case), for servers
    /// that declare none.
    pub fn from_name(name: &str) -> Option<Self> {
        let name = name.to_lowercase();
        KNOWN_NAMES
            .iter()
            .find(|(_, names)| names.contains(&name.as_str()))
            .map(|(kind, _)| *kind)
    }
}

pub fn leaf_name(display_path: &str, delimiter: char) -> &str {
    display_path
        .rsplit(delimiter)
        .next()
        .unwrap_or(display_path)
}

/// Mailbox aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mailbox {
    pub id: MailboxId,
    pub account_id: AccountId,
    /// Opaque provider identifier; never shown to people or parsed.
    pub path: MailboxPath,
    /// Human-readable hierarchy (sidebar tree, sorting, CLI); equals `path` for IMAP.
    pub display_path: String,
    pub display_name: String,
    pub mailbox_type: MailboxType,
    pub flags: Vec<Flag>,
    pub total_messages: u32,
    pub unread_messages: u32,
    /// Shown in the sidebar and synced. Unsubscribed mailboxes hold no local
    /// messages.
    pub subscribed: bool,
    /// The user chose `subscribed`: discovery no longer overrides it with the
    /// server's.
    pub subscription_explicit: bool,
}

impl Mailbox {
    /// Create a new mailbox discovered during sync and produce the event.
    pub fn discover(
        account_id: AccountId,
        path: MailboxPath,
        mailbox_type: MailboxType,
    ) -> (Self, DomainEvent) {
        let display_path = path.full_path();
        Self::discover_as(account_id, path, display_path, mailbox_type)
    }

    /// Like [`Self::discover`] for providers whose path is an opaque id.
    pub fn discover_as(
        account_id: AccountId,
        path: MailboxPath,
        display_path: String,
        mailbox_type: MailboxType,
    ) -> (Self, DomainEvent) {
        let id = MailboxId::new();
        let display_name = leaf_name(&display_path, path.delimiter()).to_string();

        let event = DomainEvent::MailboxDiscovered {
            mailbox_id: id,
            account_id,
            path: path.clone(),
            display_path: display_path.clone(),
            mailbox_type,
        };

        let mailbox = Self {
            id,
            account_id,
            path,
            display_path,
            display_name,
            mailbox_type,
            flags: Vec::new(),
            total_messages: 0,
            unread_messages: 0,
            subscribed: true,
            subscription_explicit: false,
        };

        (mailbox, event)
    }

    /// Fall back to the name for a mailbox the server gave no role, unless
    /// the server declared that role to another one (`declared`: the types the
    /// listing carries): a second "Sent" next to a declared Sent is the
    /// user's folder, and two Sent folders would make the target ambiguous.
    /// Only top-level folders and children of INBOX ("INBOX.Sent") qualify.
    pub fn infer_undeclared_type(&mut self, declared: &[MailboxType]) {
        if self.mailbox_type != MailboxType::Custom {
            return;
        }
        let Some(parent) = self.display_path.strip_suffix(&self.display_name) else {
            return;
        };
        if !(parent.is_empty() || parent.len() == 6 && parent.to_lowercase().starts_with("inbox")) {
            return;
        }
        if let Some(kind) = MailboxType::from_name(&self.display_name)
            && !declared.contains(&kind)
        {
            self.mailbox_type = kind;
        }
    }

    /// Update message counts and produce the event.
    pub fn update_counts(&mut self, total: u32, unread: u32) -> DomainEvent {
        self.total_messages = total;
        self.unread_messages = unread;

        DomainEvent::MailboxCountsUpdated {
            mailbox_id: self.id,
            total_messages: total,
            unread_messages: unread,
        }
    }

    /// Correct the special-use classification and produce the event.
    pub fn reclassify(&mut self, mailbox_type: MailboxType) -> DomainEvent {
        self.mailbox_type = mailbox_type;

        DomainEvent::MailboxTypeChanged {
            mailbox_id: self.id,
            mailbox_type,
        }
    }

    /// Record a subscription change and produce the event. `explicit` marks
    /// the user's own choice, which later discovery leaves alone.
    pub fn set_subscribed(&mut self, subscribed: bool, explicit: bool) -> DomainEvent {
        self.subscribed = subscribed;
        self.subscription_explicit |= explicit;

        DomainEvent::MailboxSubscriptionChanged {
            mailbox_id: self.id,
            subscribed,
            explicit,
        }
    }

    /// Rename the mailbox and produce the event.
    pub fn rename(
        &mut self,
        new_path: MailboxPath,
        display_path: String,
        display_name: String,
    ) -> DomainEvent {
        let old_path = std::mem::replace(&mut self.path, new_path.clone());
        self.display_path = display_path.clone();
        self.display_name = display_name.clone();

        DomainEvent::MailboxRenamed {
            mailbox_id: self.id,
            old_path,
            new_path,
            display_path,
            display_name,
        }
    }

    /// Produce a deletion event.
    pub fn delete(&self) -> DomainEvent {
        DomainEvent::MailboxDeleted {
            mailbox_id: self.id,
            account_id: self.account_id,
        }
    }

    // ── Event Sourcing rehydration ─────────────────────────────────────

    /// Apply a single domain event to mutate aggregate state.
    pub fn apply(&mut self, event: &DomainEvent) {
        match event {
            DomainEvent::MailboxDiscovered { .. } => {
                // Initial state is set by the constructor.
            }
            DomainEvent::MailboxCountsUpdated {
                total_messages,
                unread_messages,
                ..
            } => {
                self.total_messages = *total_messages;
                self.unread_messages = *unread_messages;
            }
            DomainEvent::MailboxTypeChanged { mailbox_type, .. } => {
                self.mailbox_type = *mailbox_type;
            }
            DomainEvent::MailboxSubscriptionChanged {
                subscribed,
                explicit,
                ..
            } => {
                self.subscribed = *subscribed;
                self.subscription_explicit |= *explicit;
                if !subscribed {
                    self.total_messages = 0;
                    self.unread_messages = 0;
                }
            }
            DomainEvent::MailboxRenamed {
                new_path,
                display_path,
                display_name,
                ..
            } => {
                self.display_name.clone_from(display_name);
                self.display_path.clone_from(display_path);
                self.path = new_path.clone();
            }
            DomainEvent::MailboxDeleted { .. } => {
                // Mailbox is logically deleted; upstream handles removal.
            }
            _ => {} // Ignore events belonging to other aggregates.
        }
    }

    /// Rehydrate a Mailbox from a sequence of domain events.
    ///
    /// The first event must be `MailboxDiscovered`. Returns `None` if the
    /// events slice is empty or doesn't start with a creation event.
    pub fn rehydrate(events: &[DomainEvent]) -> Option<Self> {
        let first = events.first()?;
        let DomainEvent::MailboxDiscovered {
            mailbox_id,
            account_id,
            path,
            display_path,
            mailbox_type,
        } = first
        else {
            return None;
        };

        let mut mailbox = Self {
            id: *mailbox_id,
            account_id: *account_id,
            path: path.clone(),
            display_path: display_path.clone(),
            display_name: leaf_name(display_path, path.delimiter()).to_string(),
            mailbox_type: *mailbox_type,
            flags: Vec::new(),
            total_messages: 0,
            unread_messages: 0,
            subscribed: true,
            subscription_explicit: false,
        };

        for event in &events[1..] {
            mailbox.apply(event);
        }

        Some(mailbox)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn names_fill_in_missing_roles_in_any_language() {
        for (name, kind) in [
            ("Sent", MailboxType::Sent),
            ("Posta inviata", MailboxType::Sent),
            ("Gesendet", MailboxType::Sent),
            ("Envoyés", MailboxType::Sent),
            ("Itens enviados", MailboxType::Sent),
            ("Verzonden", MailboxType::Sent),
            ("Cestino", MailboxType::Trash),
            ("Papierkorb", MailboxType::Trash),
            ("Papelera", MailboxType::Trash),
            ("Junk E-mail", MailboxType::Spam),
            ("Courrier indésirable", MailboxType::Spam),
            ("Bozze", MailboxType::Drafts),
            ("Archives", MailboxType::Archive),
            ("Arquivo", MailboxType::Archive),
            ("Presentations", MailboxType::Custom),
            ("Old Trash", MailboxType::Custom),
        ] {
            let found = MailboxType::from_name(name).unwrap_or(MailboxType::Custom);
            assert_eq!(found, kind, "{name}");
        }
    }

    fn custom(display_path: &str) -> Mailbox {
        let (mut m, _) = Mailbox::discover_as(
            AccountId::new(),
            MailboxPath::parse("x", '/').unwrap(),
            display_path.into(),
            MailboxType::Custom,
        );
        m.display_name = leaf_name(display_path, '/').into();
        m
    }

    #[test]
    fn the_name_fallback_skips_declared_roles_and_subfolders() {
        for (path, declared, expected) in [
            ("Cestino", vec![], MailboxType::Trash),
            ("INBOX/Sent", vec![], MailboxType::Sent),
            ("Sent", vec![MailboxType::Sent], MailboxType::Custom),
            ("Archives/Sent", vec![], MailboxType::Custom),
            ("Work/Trash", vec![], MailboxType::Custom),
        ] {
            let mut m = custom(path);
            m.infer_undeclared_type(&declared);
            assert_eq!(m.mailbox_type, expected, "{path}");
        }
    }

    use super::*;

    #[test]
    fn discover_mailbox_produces_event() {
        let (mailbox, event) =
            Mailbox::discover(AccountId::new(), MailboxPath::inbox(), MailboxType::Inbox);

        assert_eq!(mailbox.display_name, "INBOX");
        assert_eq!(mailbox.mailbox_type, MailboxType::Inbox);
        assert_eq!(mailbox.total_messages, 0);
        assert_eq!(event.event_name(), "MailboxDiscovered");
    }

    #[test]
    fn update_counts() {
        let (mut mailbox, _) =
            Mailbox::discover(AccountId::new(), MailboxPath::inbox(), MailboxType::Inbox);

        let event = mailbox.update_counts(150, 12);

        assert_eq!(mailbox.total_messages, 150);
        assert_eq!(mailbox.unread_messages, 12);
        assert_eq!(event.event_name(), "MailboxCountsUpdated");
    }

    #[test]
    fn rename_mailbox() {
        let (mut mailbox, _) = Mailbox::discover(
            AccountId::new(),
            MailboxPath::parse("Work", '/').unwrap(),
            MailboxType::Custom,
        );

        let event = mailbox.rename(
            MailboxPath::parse("Work-Archive", '/').unwrap(),
            "Work-Archive".into(),
            "Work-Archive".into(),
        );

        assert_eq!(mailbox.display_name, "Work-Archive");
        assert_eq!(mailbox.path.full_path(), "Work-Archive");
        assert_eq!(event.event_name(), "MailboxRenamed");
    }

    #[test]
    fn rehydrate_mailbox_from_events() {
        let id = MailboxId::new();
        let account_id = AccountId::new();
        let events = vec![
            DomainEvent::MailboxDiscovered {
                mailbox_id: id,
                account_id,
                path: MailboxPath::inbox(),
                display_path: MailboxPath::inbox().full_path(),
                mailbox_type: MailboxType::Inbox,
            },
            DomainEvent::MailboxCountsUpdated {
                mailbox_id: id,
                total_messages: 42,
                unread_messages: 5,
            },
            DomainEvent::MailboxRenamed {
                mailbox_id: id,
                old_path: MailboxPath::inbox(),
                new_path: MailboxPath::parse("Primary", '/').unwrap(),
                display_path: "Primary".into(),
                display_name: "Primary".into(),
            },
        ];

        let mailbox = Mailbox::rehydrate(&events).unwrap();
        assert_eq!(mailbox.total_messages, 42);
        assert_eq!(mailbox.unread_messages, 5);
        assert_eq!(mailbox.display_name, "Primary");
        assert_eq!(mailbox.path.full_path(), "Primary");
    }

    #[test]
    fn rehydrate_keeps_the_subscription_and_who_chose_it() {
        let (mut mailbox, discovered) = Mailbox::discover(
            AccountId::new(),
            MailboxPath::parse("Work", '/').unwrap(),
            MailboxType::Custom,
        );
        assert!(mailbox.subscribed && !mailbox.subscription_explicit);
        let by_server = mailbox.set_subscribed(false, false);
        let by_user = mailbox.set_subscribed(true, true);
        let server_again = mailbox.set_subscribed(false, false);

        let rebuilt = Mailbox::rehydrate(&[discovered, by_server, by_user, server_again]).unwrap();

        assert!(!rebuilt.subscribed);
        assert!(
            rebuilt.subscription_explicit,
            "a later server change is not a user's choice"
        );
    }

    #[test]
    fn rehydrate_empty_events_returns_none() {
        let result = Mailbox::rehydrate(&[]);
        assert!(result.is_none());
    }

    #[test]
    fn mailbox_serialization_roundtrip() {
        let (mailbox, _) = Mailbox::discover(
            AccountId::new(),
            MailboxPath::parse("Sent/2024", '/').unwrap(),
            MailboxType::Sent,
        );

        let json = serde_json::to_string(&mailbox).unwrap();
        let deserialized: Mailbox = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.display_name, "2024");
        assert_eq!(deserialized.mailbox_type, MailboxType::Sent);
    }
}
