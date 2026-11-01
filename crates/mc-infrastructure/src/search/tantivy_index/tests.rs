use super::*;
use chrono::{TimeZone, Utc};
use mc_domain::model::account::{AccountStatus, MailboxModel, ProviderType};
use mc_domain::model::message::{Address, Envelope, Membership};
use mc_domain::traits::repository::AccountSummary;
use mc_domain::value_objects::email_address::EmailAddress;
use tantivy::schema::{STORED, STRING, Schema};
use tempfile::TempDir;

fn create_test_message(id: &str, subject: &str, from: &str, body: &str) -> Message {
    let uuid = uuid::Uuid::parse_str(id).unwrap();
    Message {
        id: MessageId::from_uuid(uuid),
        account_id: AccountId::new(),
        memberships: vec![Membership {
            mailbox_id: MailboxId::new(),
            remote_id: None,
        }],
        message_rfc_id: None,
        in_reply_to: None,
        references: vec![],
        envelope: Envelope {
            subject: subject.to_string(),
            from: vec![Address {
                name: Some("Test User".to_string()),
                email: EmailAddress::new(from).unwrap(),
            }],
            to: vec![],
            cc: vec![],
            bcc: vec![],
            date: Utc::now(),
            reply_to: vec![],
        },
        body_text: Some(body.to_string()),
        body_html: None,
        body_truncated: false,
        flags: vec![],
        size_bytes: 1024,
        has_attachments: false,
        attachments: vec![],
        received_at: Utc::now(),
        internal_date: Utc::now(),
    }
}

#[tokio::test]
async fn test_index_and_search() {
    let temp_dir = TempDir::new().unwrap();
    let index = TantivySearchIndex::new(temp_dir.path()).unwrap();

    let msg = create_test_message(
        "01234567-89ab-cdef-0123-456789abcdef",
        "Test Subject",
        "test@example.com",
        "This is a test message body",
    );

    // Index message
    index
        .index_message(&msg, &IndexExtras::default())
        .await
        .unwrap();

    // Search
    let results = index
        .search(
            "test",
            None,
            None,
            &SearchDirectory::default(),
            &Pagination {
                page: 1,
                page_size: 10,
            },
        )
        .await
        .unwrap();

    assert_eq!(results.total_count, 1);
    assert_eq!(results.hits.len(), 1);
    assert_eq!(results.hits[0].message_id, msg.id);
}

#[tokio::test]
async fn test_search_by_sender_name() {
    let temp_dir = TempDir::new().unwrap();
    let index = TantivySearchIndex::new(temp_dir.path()).unwrap();

    let msg = create_test_message(
        "01234567-89ab-cdef-0123-456789abcdef",
        "Hello",
        "john@example.com",
        "Some content",
    );

    index
        .index_message(&msg, &IndexExtras::default())
        .await
        .unwrap();

    // Search by sender name (from_name field)
    let results = index
        .search(
            "Test User",
            None,
            None,
            &SearchDirectory::default(),
            &Pagination::default(),
        )
        .await
        .unwrap();
    assert_eq!(results.total_count, 1);
}

#[tokio::test]
async fn test_stemming_search() {
    let temp_dir = TempDir::new().unwrap();
    let index = TantivySearchIndex::new(temp_dir.path()).unwrap();

    let msg = create_test_message(
        "01234567-89ab-cdef-0123-456789abcdef",
        "Meeting invitation",
        "test@example.com",
        "We are running the weekly meetings every Tuesday",
    );

    index
        .index_message(&msg, &IndexExtras::default())
        .await
        .unwrap();

    // "run" should match "running" via stemming
    let results = index
        .search(
            "run",
            None,
            None,
            &SearchDirectory::default(),
            &Pagination::default(),
        )
        .await
        .unwrap();
    assert_eq!(results.total_count, 1);

    // "meeting" should match "meetings" via stemming
    let results = index
        .search(
            "meeting",
            None,
            None,
            &SearchDirectory::default(),
            &Pagination::default(),
        )
        .await
        .unwrap();
    assert_eq!(results.total_count, 1);
}

#[tokio::test]
async fn test_remove_message() {
    let temp_dir = TempDir::new().unwrap();
    let index = TantivySearchIndex::new(temp_dir.path()).unwrap();

    let msg1 = create_test_message(
        "01234567-89ab-cdef-0123-456789abcdef",
        "Test Subject",
        "test@example.com",
        "Test body",
    );

    let msg2 = create_test_message(
        "11234567-89ab-cdef-0123-456789abcdef",
        "Another Subject",
        "another@example.com",
        "Another body",
    );

    // Index two messages
    index
        .index_message(&msg1, &IndexExtras::default())
        .await
        .unwrap();
    index
        .index_message(&msg2, &IndexExtras::default())
        .await
        .unwrap();

    let results = index
        .search(
            "Subject",
            None,
            None,
            &SearchDirectory::default(),
            &Pagination::default(),
        )
        .await
        .unwrap();
    assert_eq!(results.hits.len(), 2);

    // Remove first message
    index.remove_message(&msg1.id).await.unwrap();

    let results = index
        .search(
            "Subject",
            None,
            None,
            &SearchDirectory::default(),
            &Pagination::default(),
        )
        .await
        .unwrap();

    // Should only find msg2
    assert_eq!(results.hits.len(), 1);
    assert_eq!(results.hits[0].message_id, msg2.id);
}

#[tokio::test]
async fn search_matches_cc_and_html_only_bodies_and_snippets_them() {
    let dir = TempDir::new().unwrap();
    let index = TantivySearchIndex::new(dir.path()).unwrap();
    let mut msg = create_test_message(
        "01234567-89ab-cdef-0123-456789abcdef",
        "Hello",
        "john@example.com",
        "",
    );
    msg.body_text = None;
    msg.body_html = Some("<p>The <b>quarterly</b> report is attached</p>".into());
    msg.envelope.cc = vec![Address {
        name: None,
        email: EmailAddress::new("boss@corp.example").unwrap(),
    }];
    index
        .index_message(&msg, &IndexExtras::default())
        .await
        .unwrap();

    for query in ["quarterly", "boss"] {
        let results = index
            .search(
                query,
                None,
                None,
                &SearchDirectory::default(),
                &Pagination::default(),
            )
            .await
            .unwrap();
        assert_eq!(results.total_count, 1, "{query}");
    }
    let snippet = index.snippet("quarterly", &msg).unwrap();
    assert!(snippet.contains("<b>quarterly</b>"), "{snippet}");
    assert!(
        index.snippet("hello", &msg).is_none(),
        "subject-only match has no body excerpt"
    );
}

#[tokio::test]
async fn scope_filters_restrict_hits() {
    let dir = TempDir::new().unwrap();
    let index = TantivySearchIndex::new(dir.path()).unwrap();
    let a = create_test_message(
        "01234567-89ab-cdef-0123-456789abcdef",
        "Invoice",
        "a@example.com",
        "x",
    );
    let b = create_test_message(
        "11234567-89ab-cdef-0123-456789abcdef",
        "Invoice",
        "b@example.com",
        "x",
    );
    index
        .index_message(&a, &IndexExtras::default())
        .await
        .unwrap();
    index
        .index_message(&b, &IndexExtras::default())
        .await
        .unwrap();

    let page = Pagination::default();
    let by_account = index
        .search(
            "invoice",
            Some(&a.account_id),
            None,
            &SearchDirectory::default(),
            &page,
        )
        .await
        .unwrap();
    assert_eq!(
        by_account
            .hits
            .iter()
            .map(|h| h.message_id)
            .collect::<Vec<_>>(),
        [a.id]
    );
    let by_mailbox = index
        .search(
            "invoice",
            None,
            Some(&b.memberships[0].mailbox_id),
            &SearchDirectory::default(),
            &page,
        )
        .await
        .unwrap();
    assert_eq!(
        by_mailbox
            .hits
            .iter()
            .map(|h| h.message_id)
            .collect::<Vec<_>>(),
        [b.id]
    );
}

#[tokio::test]
async fn a_message_is_found_in_every_mailbox_it_is_in() {
    let dir = TempDir::new().unwrap();
    let index = TantivySearchIndex::new(dir.path()).unwrap();
    let mut msg = create_test_message(
        "00000000-0000-0000-0000-0000000000a1",
        "Invoice",
        "a@example.com",
        "x",
    );
    let (first, second) = (msg.memberships[0].mailbox_id, MailboxId::new());
    msg.memberships.push(Membership {
        mailbox_id: second,
        remote_id: None,
    });
    index
        .index_message(&msg, &IndexExtras::default())
        .await
        .unwrap();

    let page = Pagination::default();
    for mailbox in [first, second] {
        let hits = index
            .search(
                "invoice",
                None,
                Some(&mailbox),
                &SearchDirectory::default(),
                &page,
            )
            .await
            .unwrap()
            .hits;
        assert_eq!(
            hits.iter().map(|h| h.message_id).collect::<Vec<_>>(),
            [msg.id]
        );
    }

    msg.memberships.remove(0);
    index
        .index_message(&msg, &IndexExtras::default())
        .await
        .unwrap();
    assert!(
        index
            .search(
                "invoice",
                None,
                Some(&first),
                &SearchDirectory::default(),
                &page
            )
            .await
            .unwrap()
            .hits
            .is_empty()
    );
    assert_eq!(
        index
            .search(
                "invoice",
                None,
                Some(&second),
                &SearchDirectory::default(),
                &page
            )
            .await
            .unwrap()
            .hits
            .len(),
        1
    );
}

#[test]
fn an_index_with_another_schema_is_recreated() {
    let dir = TempDir::new().unwrap();
    let mut old = Schema::builder();
    old.add_text_field("message_id", STRING | STORED);
    Index::create_in_dir(dir.path(), old.build()).unwrap();

    let index = TantivySearchIndex::new(dir.path()).unwrap();
    assert!(index.was_recreated());
    drop(index);
    assert!(!TantivySearchIndex::new(dir.path()).unwrap().was_recreated());
}

#[tokio::test]
async fn remove_account_drops_only_that_accounts_documents() {
    let temp_dir = TempDir::new().unwrap();
    let index = TantivySearchIndex::new(temp_dir.path()).unwrap();
    let gone = create_test_message(
        "01234567-89ab-cdef-0123-456789abcdef",
        "Invoice",
        "a@x.org",
        "pay",
    );
    let kept = create_test_message(
        "11234567-89ab-cdef-0123-456789abcdef",
        "Invoice",
        "b@x.org",
        "pay",
    );
    index
        .index_message(&gone, &IndexExtras::default())
        .await
        .unwrap();
    index
        .index_message(&kept, &IndexExtras::default())
        .await
        .unwrap();

    index.remove_account(&gone.account_id).await.unwrap();

    let results = index
        .search(
            "invoice",
            None,
            None,
            &SearchDirectory::default(),
            &Pagination::default(),
        )
        .await
        .unwrap();
    assert_eq!(results.hits.len(), 1);
    assert_eq!(results.hits[0].message_id, kept.id);
}

// ── Operators ───────────────────────────────────────────────────────────

use mc_domain::model::attachment::Attachment;
use mc_domain::model::mailbox::{Mailbox, MailboxType};
use mc_domain::value_objects::flag::Flag;
use mc_domain::value_objects::mailbox_path::MailboxPath;

const IDS: [&str; 4] = [
    "a1234567-89ab-cdef-0123-456789abcdef",
    "b1234567-89ab-cdef-0123-456789abcdef",
    "c1234567-89ab-cdef-0123-456789abcdef",
    "d1234567-89ab-cdef-0123-456789abcdef",
];

struct Fixture {
    _dir: TempDir,
    index: TantivySearchIndex,
    directory: SearchDirectory,
}

impl Fixture {
    /// Hits as indices into `IDS`, in result order.
    async fn found(&self, query: &str) -> Vec<usize> {
        let results = self
            .index
            .search(query, None, None, &self.directory, &Pagination::default())
            .await
            .unwrap();
        results
            .hits
            .iter()
            .map(|h| {
                IDS.iter()
                    .position(|id| *id == h.message_id.to_string())
                    .unwrap()
            })
            .collect()
    }

    async fn sorted(&self, query: &str) -> Vec<usize> {
        let mut found = self.found(query).await;
        found.sort();
        found
    }
}

fn mailbox(account: AccountId, path: &str, mailbox_type: MailboxType) -> Mailbox {
    let path = MailboxPath::new(vec![path.to_string()], '/').unwrap();
    Mailbox::discover(account, path, mailbox_type).0
}

fn attachment(name: &str) -> Attachment {
    Attachment {
        id: Default::default(),
        filename: Some(name.into()),
        mime_type: "application/pdf".into(),
        size_bytes: 10,
        content_id: None,
        is_inline: false,
    }
}

/// 0: unread GitHub mail in Inbox + label Dev/GitHub, with a PDF.
/// 1: read invoice from a subdomain in Archive + label Newsletter/The Space Cinema.
/// 2: starred, answered mail from 2024 on a mailing list.
/// 3: another account's mail.
async fn fixture() -> Fixture {
    let dir = TempDir::new().unwrap();
    let index = TantivySearchIndex::new(dir.path()).unwrap();
    let account = AccountId::new();
    let inbox = mailbox(account, "INBOX", MailboxType::Inbox);
    let archive = mailbox(account, "Archive", MailboxType::Archive);
    let stranger = AccountId::new();
    let other_inbox = mailbox(stranger, "INBOX", MailboxType::Inbox);

    let mut m0 = create_test_message(
        IDS[0],
        "Pull request merged",
        "noreply@github.com",
        "ci green",
    );
    m0.account_id = account;
    m0.memberships[0].mailbox_id = inbox.id;
    m0.has_attachments = true;
    m0.attachments = vec![attachment("fattura_marzo.pdf")];
    m0.envelope.date = Utc.with_ymd_and_hms(2026, 10, 30, 9, 0, 0).unwrap();

    let mut m1 = create_test_message(
        IDS[1],
        "Invoice March",
        "billing@remind.facile.it",
        "pay now",
    );
    m1.account_id = account;
    m1.memberships[0].mailbox_id = archive.id;
    m1.flags = vec![Flag::Seen];
    m1.envelope.date = Utc.with_ymd_and_hms(2026, 3, 5, 9, 0, 0).unwrap();

    let mut m2 = create_test_message(IDS[2], "Weekly digest", "list@example.org", "news");
    m2.account_id = account;
    m2.memberships[0].mailbox_id = inbox.id;
    m2.flags = vec![Flag::Seen, Flag::Flagged, Flag::Answered];
    m2.envelope.date = Utc.with_ymd_and_hms(2024, 1, 10, 9, 0, 0).unwrap();

    let mut m3 = create_test_message(IDS[3], "Invoice other", "x@y.com", "pay later");
    m3.account_id = stranger;
    m3.memberships[0].mailbox_id = other_inbox.id;
    m3.envelope.date = Utc.with_ymd_and_hms(2026, 10, 1, 9, 0, 0).unwrap();

    let labelled = |labels: &[&str], list_id: Option<&str>| IndexExtras {
        labels: labels.iter().map(ToString::to_string).collect(),
        list_id: list_id.map(Into::into),
    };
    index
        .index_message(&m0, &labelled(&["Dev/GitHub"], None))
        .await
        .unwrap();
    index
        .index_message(&m1, &labelled(&["Newsletter/The Space Cinema"], None))
        .await
        .unwrap();
    index
        .index_message(&m2, &labelled(&[], Some("<digest.lists.example.org>")))
        .await
        .unwrap();
    index
        .index_message(&m3, &IndexExtras::default())
        .await
        .unwrap();

    let summary = |id, name: &str, email: &str| AccountSummary {
        id,
        name: name.into(),
        email: EmailAddress::new(email).unwrap(),
        provider_type: ProviderType::Imap,
        mailbox_model: MailboxModel::Labels,
        supports_tags: None,
        status: AccountStatus::Active,
        total_unread: 0,
        status_error: None,
        status_since: None,
    };
    Fixture {
        _dir: dir,
        index,
        directory: SearchDirectory {
            accounts: vec![
                summary(account, "Work", "me@work.it"),
                summary(stranger, "Home", "me@home.it"),
            ],
            mailboxes: vec![inbox, archive, other_inbox],
        },
    }
}

#[tokio::test]
async fn from_matches_a_parent_domain_a_name_or_an_address() {
    let f = fixture().await;
    assert_eq!(f.found("from:facile.it").await, [1]);
    assert_eq!(f.found("from:billing@remind.facile.it").await, [1]);
    assert_eq!(f.found("from:\"Test User\"").await.len(), 4);
    assert_eq!(f.found("from:github").await, [0]);
}

#[tokio::test]
async fn label_is_case_insensitive_and_a_parent_matches_its_children() {
    let f = fixture().await;
    assert_eq!(f.found("label:dev/github").await, [0]);
    assert_eq!(f.found("label:DEV").await, [0]);
    assert_eq!(f.found(r#"label:"newsletter/the space cinema""#).await, [1]);
    assert_eq!(f.found("label:newsletter").await, [1]);
    assert!(f.found("label:dev/git").await.is_empty());
    assert_eq!(f.sorted("-label:dev").await, [1, 2, 3]);
}

#[tokio::test]
async fn in_and_account_resolve_names_through_the_directory() {
    let f = fixture().await;
    assert_eq!(f.sorted("in:inbox").await, [0, 2, 3]);
    assert_eq!(f.found("in:archive").await, [1]);
    assert_eq!(f.found("in:ARCHIVE").await, [1]);
    assert_eq!(f.sorted("in:inbox account:work").await, [0, 2]);
    assert_eq!(f.found("account:me@home.it").await, [3]);
    assert!(f.found("in:nowhere").await.is_empty());
    assert!(f.found("account:nobody").await.is_empty());
}

#[tokio::test]
async fn state_attachment_and_list_filters() {
    let f = fixture().await;
    assert_eq!(f.sorted("is:unread").await, [0, 3]);
    assert_eq!(f.sorted("is:read").await, [1, 2]);
    assert_eq!(f.found("is:starred").await, [2]);
    assert_eq!(f.found("is:answered").await, [2]);
    assert_eq!(f.found("has:attachment").await, [0]);
    assert_eq!(f.found("filename:fattura").await, [0]);
    assert_eq!(f.found("filename:pdf").await, [0]);
    assert_eq!(f.found("list:digest.lists.example.org").await, [2]);
    assert_eq!(f.found("list:example").await, [2]);
}

#[tokio::test]
async fn dates_bound_the_indexed_date() {
    let f = fixture().await;
    assert_eq!(f.found("before:2025-01-01").await, [2]);
    assert_eq!(f.sorted("after:2026-03-01 before:2026-04-01").await, [1]);
    assert_eq!(f.sorted("after:2026-10-01").await, [0, 3]);
    assert_eq!(f.sorted("older_than:1y").await, [2]);
    assert!(
        f.index
            .search(
                "after:soon",
                None,
                None,
                &f.directory,
                &Pagination::default()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn boolean_composition_and_ordering() {
    let f = fixture().await;
    assert_eq!(f.sorted("invoice -other").await, [1]);
    assert_eq!(f.sorted("invoice OR digest").await, [1, 2, 3]);
    assert_eq!(f.sorted("(invoice OR digest) is:read").await, [1, 2]);
    assert_eq!(f.sorted("-is:read -in:archive").await, [0, 3]);
    assert_eq!(f.sorted("foo:bar").await, Vec::<usize>::new());
    // Filters alone list newest first.
    assert_eq!(
        f.found("has:attachment OR is:read OR is:unread").await,
        [0, 3, 1, 2]
    );
}

#[tokio::test]
async fn the_old_prefix_syntax_still_works() {
    let f = fixture().await;
    assert_eq!(f.found("from: remind.facile.it").await, [1]);
    assert_eq!(f.found("sbj: digest").await, [2]);
    assert_eq!(f.found("subject:\"pull request\"").await, [0]);
}
