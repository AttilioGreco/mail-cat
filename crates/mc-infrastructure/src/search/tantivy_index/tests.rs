use super::*;
use chrono::Utc;
use mc_domain::model::message::{Address, Envelope};
use mc_domain::value_objects::email_address::EmailAddress;
use tempfile::TempDir;

fn create_test_message(id: &str, subject: &str, from: &str, body: &str) -> Message {
    let uuid = uuid::Uuid::parse_str(id).unwrap();
    Message {
        id: MessageId::from_uuid(uuid),
        account_id: AccountId::new(),
        mailbox_id: MailboxId::new(),
        provider_uid: None,
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
    index.index_message(&msg).await.unwrap();

    // Search
    let results = index
        .search(
            "test",
            None,
            None,
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

    index.index_message(&msg).await.unwrap();

    // Search by sender name (from_name field)
    let results = index
        .search("Test User", None, None, &Pagination::default())
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

    index.index_message(&msg).await.unwrap();

    // "run" should match "running" via stemming
    let results = index
        .search("run", None, None, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(results.total_count, 1);

    // "meeting" should match "meetings" via stemming
    let results = index
        .search("meeting", None, None, &Pagination::default())
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
    index.index_message(&msg1).await.unwrap();
    index.index_message(&msg2).await.unwrap();

    let results = index
        .search("Subject", None, None, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(results.hits.len(), 2);

    // Remove first message
    index.remove_message(&msg1.id).await.unwrap();

    let results = index
        .search("Subject", None, None, &Pagination::default())
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
    index.index_message(&msg).await.unwrap();

    for query in ["quarterly", "boss"] {
        let results = index
            .search(query, None, None, &Pagination::default())
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
    index.index_message(&a).await.unwrap();
    index.index_message(&b).await.unwrap();

    let page = Pagination::default();
    let by_account = index
        .search("invoice", Some(&a.account_id), None, &page)
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
        .search("invoice", None, Some(&b.mailbox_id), &page)
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
