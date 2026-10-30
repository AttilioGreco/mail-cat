use super::*;
use mc_domain::{
    AccountId, AccountStatus, AccountSummary, ContactSummary, DomainError, EmailAddress, Mailbox,
    MailboxId, Message, MessageId, MessageSummary, ProviderType, RemoteId,
};
use mc_shared::pagination::{PagedResult, Pagination};

// ── Mock AccountReadRepository ─────────────────────────────────────

struct MockAccountReadRepo;

#[async_trait]
impl AccountReadRepository for MockAccountReadRepo {
    async fn get_account(&self, id: &AccountId) -> Result<Option<AccountSummary>, DomainError> {
        Ok(Some(AccountSummary {
            id: *id,
            name: "Test Account".into(),
            email: EmailAddress::new("test@example.com").unwrap(),
            provider_type: ProviderType::Imap,
            mailbox_model: mc_domain::MailboxModel::Folders,
            supports_tags: None,
            status: AccountStatus::Active,
            total_unread: 5,
            status_error: None,
            status_since: None,
        }))
    }

    async fn list_accounts(&self) -> Result<Vec<AccountSummary>, DomainError> {
        Ok(vec![AccountSummary {
            id: AccountId::new(),
            name: "Account 1".into(),
            email: EmailAddress::new("a1@example.com").unwrap(),
            provider_type: ProviderType::Imap,
            mailbox_model: mc_domain::MailboxModel::Folders,
            supports_tags: None,
            status: AccountStatus::Active,
            total_unread: 3,
            status_error: None,
            status_since: None,
        }])
    }

    async fn account_stats(
        &self,
        _id: &AccountId,
    ) -> Result<mc_domain::AccountSyncStats, DomainError> {
        Ok(mc_domain::AccountSyncStats::default())
    }
}

// ── Mock MailboxReadRepository ─────────────────────────────────────

struct MockMailboxReadRepo;

#[async_trait]
impl MailboxReadRepository for MockMailboxReadRepo {
    async fn get_mailbox(&self, _id: &MailboxId) -> Result<Option<Mailbox>, DomainError> {
        Ok(None)
    }

    async fn list_mailboxes(&self, _account_id: &AccountId) -> Result<Vec<Mailbox>, DomainError> {
        Ok(vec![])
    }
}

// ── Mock MessageReadRepository ────────────────────────────────────

#[derive(Default)]
struct MockMessageReadRepo {
    messages: Vec<Message>,
}

#[async_trait]
impl MessageReadRepository for MockMessageReadRepo {
    async fn get_message(&self, id: &MessageId) -> Result<Option<Message>, DomainError> {
        Ok(self.messages.iter().find(|m| m.id == *id).cloned())
    }

    async fn list_messages(
        &self,
        _mailbox_id: &MailboxId,
        _pagination: &Pagination,
    ) -> Result<PagedResult<MessageSummary>, DomainError> {
        Ok(PagedResult {
            items: vec![],
            total_count: 0,
            page: 1,
            page_size: 20,
        })
    }

    async fn get_thread(&self, _id: &MessageId) -> Result<Vec<Message>, DomainError> {
        Ok(vec![])
    }

    async fn find_by_remote_id(
        &self,
        _mailbox_id: &MailboxId,
        _remote_id: &RemoteId,
    ) -> Result<Option<MessageSummary>, DomainError> {
        Ok(None)
    }

    async fn held_ids(
        &self,
        _mailbox_id: &MailboxId,
        _uids: &[mc_domain::RemoteId],
    ) -> Result<std::collections::HashSet<mc_domain::RemoteId>, DomainError> {
        Ok(Default::default())
    }
    async fn find_by_keys(
        &self,
        _account_id: &AccountId,
        _keys: &[mc_domain::MessageKey],
    ) -> Result<std::collections::HashMap<mc_domain::MessageKey, MessageId>, DomainError> {
        Ok(Default::default())
    }
    async fn pending_messages(
        &self,
        _mailbox_id: &MailboxId,
    ) -> Result<Vec<MessageId>, DomainError> {
        Ok(vec![])
    }
    async fn find_by_rfc_id(
        &self,
        _account_id: &AccountId,
        _rfc_id: &str,
    ) -> Result<Vec<MessageSummary>, DomainError> {
        Ok(vec![])
    }

    async fn find_unread_by_mailbox(
        &self,
        _mailbox_id: &MailboxId,
    ) -> Result<Vec<MessageSummary>, DomainError> {
        Ok(vec![])
    }

    async fn scan_all_messages(
        &self,
        _account_id: Option<&mc_domain::AccountId>,
    ) -> Result<Vec<mc_domain::Message>, DomainError> {
        Ok(vec![])
    }

    async fn suggest(
        &self,
        _prefix: &str,
        _scope: Option<mc_domain::SuggestScope>,
        _max_per_field: usize,
    ) -> Result<Vec<mc_domain::Suggestion>, DomainError> {
        Ok(vec![])
    }
}

// ── Mock ContactReadRepository ────────────────────────────────────

/// Returns `contacts` from every search, like a repository whose matches
/// are all of them.
#[derive(Default)]
struct MockContactRepo {
    contacts: Vec<ContactSummary>,
}

#[async_trait]
impl ContactReadRepository for MockContactRepo {
    async fn get_contact(
        &self,
        _id: &mc_domain::ContactId,
    ) -> Result<Option<ContactSummary>, DomainError> {
        Ok(None)
    }

    async fn list_contacts(
        &self,
        pagination: &Pagination,
    ) -> Result<PagedResult<ContactSummary>, DomainError> {
        Ok(PagedResult::new(vec![], 0, pagination))
    }

    async fn find_by_email(
        &self,
        _email: &EmailAddress,
    ) -> Result<Vec<ContactSummary>, DomainError> {
        Ok(vec![])
    }

    async fn find_by_email_in_account(
        &self,
        _email: &EmailAddress,
        _account_id: &AccountId,
    ) -> Result<Vec<ContactSummary>, DomainError> {
        Ok(vec![])
    }

    async fn search_contacts(
        &self,
        _query: &str,
        limit: usize,
    ) -> Result<Vec<ContactSummary>, DomainError> {
        Ok(self.contacts.iter().take(limit).cloned().collect())
    }

    async fn find_duplicate_candidates(
        &self,
        _account_id: Option<&AccountId>,
    ) -> Result<Vec<(ContactSummary, ContactSummary)>, DomainError> {
        Ok(vec![])
    }
}

// ── Mock SearchIndex ──────────────────────────────────────────────

/// Returns `hits` for every query, best first.
#[derive(Default)]
struct MockSearchIndex {
    hits: Vec<MessageId>,
}

#[async_trait]
impl SearchIndex for MockSearchIndex {
    async fn index_message(&self, _message: &Message) -> Result<(), DomainError> {
        Ok(())
    }

    async fn remove_message(&self, _id: &MessageId) -> Result<(), DomainError> {
        Ok(())
    }

    async fn remove_account(&self, _id: &AccountId) -> Result<(), DomainError> {
        Ok(())
    }

    async fn search(
        &self,
        _query: &str,
        _account_id: Option<&AccountId>,
        _mailbox_id: Option<&MailboxId>,
        _pagination: &Pagination,
    ) -> Result<mc_domain::IndexResults, DomainError> {
        Ok(mc_domain::IndexResults {
            hits: self
                .hits
                .iter()
                .map(|&message_id| mc_domain::IndexHit {
                    message_id,
                    score: 1.0,
                })
                .collect(),
            total_count: self.hits.len() as u64,
        })
    }

    fn snippet(&self, query: &str, message: &Message) -> Option<String> {
        message
            .body_text
            .as_ref()
            .map(|_| format!("<b>{query}</b>"))
    }

    async fn flush(&self) -> Result<(), DomainError> {
        Ok(())
    }
}

// ── Mock ConversationReadRepository ───────────────────────────────

struct MockConversationRepo;

#[async_trait]
impl ConversationReadRepository for MockConversationRepo {
    async fn get_conversation(
        &self,
        _thread_id: &mc_domain::ThreadId,
        _account_id: &AccountId,
    ) -> Result<Option<mc_domain::ConversationSummary>, DomainError> {
        Ok(None)
    }

    async fn list_conversations(
        &self,
        _account_id: &AccountId,
        _mailbox_id: Option<&mc_domain::MailboxId>,
        _pagination: &Pagination,
    ) -> Result<PagedResult<mc_domain::ConversationSummary>, DomainError> {
        Ok(PagedResult {
            items: vec![],
            total_count: 0,
            page: 1,
            page_size: 50,
        })
    }

    async fn rebuild_index(&self) -> Result<u64, DomainError> {
        Ok(0)
    }
}

// ── Tests ──────────────────────────────────────────────────────────

type TestHandler = QueryHandler<
    MockAccountReadRepo,
    MockMailboxReadRepo,
    MockMessageReadRepo,
    MockSearchIndex,
    MockContactRepo,
    MockConversationRepo,
>;

fn make_handler() -> TestHandler {
    handler_with_contacts(vec![])
}

fn handler_with_contacts(contacts: Vec<ContactSummary>) -> TestHandler {
    handler(
        MockMessageReadRepo::default(),
        MockSearchIndex::default(),
        contacts,
    )
}

fn handler(
    messages: MockMessageReadRepo,
    index: MockSearchIndex,
    contacts: Vec<ContactSummary>,
) -> TestHandler {
    QueryHandler::new(
        Arc::new(MockAccountReadRepo),
        Arc::new(MockMailboxReadRepo),
        Arc::new(messages),
        Arc::new(index),
        Arc::new(MockContactRepo { contacts }),
        Arc::new(MockConversationRepo),
    )
}

#[tokio::test]
async fn search_hydrates_hits_from_the_read_model_and_skips_stale_ones() {
    let date = chrono::DateTime::from_timestamp(1_760_000_000, 0).unwrap();
    let message = Message {
        id: MessageId::new(),
        account_id: AccountId::new(),
        memberships: vec![mc_domain::Membership {
            mailbox_id: MailboxId::new(),
            remote_id: None,
        }],
        message_rfc_id: None,
        in_reply_to: None,
        references: vec![],
        envelope: mc_domain::Envelope {
            from: vec![mc_domain::Address {
                name: Some("Ada".into()),
                email: EmailAddress::new("ada@example.com").unwrap(),
            }],
            to: vec![],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            subject: "Quarterly report".into(),
            date,
        },
        flags: vec![],
        size_bytes: 1,
        has_attachments: false,
        attachments: vec![],
        body_text: Some("the quarterly numbers".into()),
        body_html: None,
        body_truncated: false,
        received_at: date,
        internal_date: date,
    };
    let (id, gone) = (message.id, MessageId::new());
    let handler = handler(
        MockMessageReadRepo {
            messages: vec![message],
        },
        MockSearchIndex {
            hits: vec![gone, id],
        },
        vec![],
    );

    let result = handler
        .dispatch(Query::SearchMessages {
            query: "quarterly".into(),
            account_id: None,
            mailbox_id: None,
            pagination: Pagination::new(1, 20),
        })
        .await
        .unwrap();

    let results = result.into_search_results().unwrap();
    assert_eq!(results.hits.len(), 1);
    let hit = &results.hits[0];
    assert_eq!(hit.message_id, id);
    assert_eq!(hit.subject, "Quarterly report");
    assert_eq!(hit.from_name.as_deref(), Some("Ada"));
    assert_eq!(hit.from_email, "ada@example.com");
    assert_eq!(hit.date, 1_760_000_000);
    assert_eq!(hit.snippet.as_deref(), Some("<b>quarterly</b>"));
}

fn contact(raw: &str, scope: Option<AccountId>) -> ContactSummary {
    let email = EmailAddress::new(raw).unwrap();
    ContactSummary {
        id: mc_domain::ContactId::new(),
        display_name: raw.into(),
        primary_email: email.clone(),
        emails: vec![mc_domain::ContactEmail::primary(email)],
        organization: None,
        source: mc_domain::ContactSource::Local,
        account_scope: scope,
    }
}

#[tokio::test]
async fn contact_search_puts_own_account_first_and_fills_with_the_rest() {
    let (mine, other) = (AccountId::new(), AccountId::new());
    let handler = handler_with_contacts(vec![
        contact("x1@example.com", Some(other)),
        contact("ME1@example.com", Some(other)),
        contact("global@example.com", None),
        contact("me1@example.com", Some(mine)),
        contact("me2@example.com", Some(mine)),
    ]);
    let search = |limit| Query::SearchContacts {
        query: "e".into(),
        limit,
        requesting_account_id: Some(mine),
    };

    let QueryResult::ContactHits(hits) = handler.dispatch(search(3)).await.unwrap() else {
        panic!("expected contact hits");
    };
    let shown: Vec<_> = hits
        .iter()
        .map(|h| (h.primary_email.as_str(), h.is_cross_account))
        .collect();
    assert_eq!(
        shown,
        [
            ("me1@example.com", false),
            ("me2@example.com", false),
            ("x1@example.com", true)
        ]
    );

    // An address both accounts know is suggested once, as the own one.
    let QueryResult::ContactHits(hits) = handler.dispatch(search(10)).await.unwrap() else {
        panic!("expected contact hits");
    };
    let emails: Vec<_> = hits.iter().map(|h| h.primary_email.as_str()).collect();
    assert_eq!(
        emails,
        [
            "me1@example.com",
            "me2@example.com",
            "x1@example.com",
            "global@example.com"
        ]
    );

    let QueryResult::ContactHits(hits) = handler.dispatch(search(1)).await.unwrap() else {
        panic!("expected contact hits");
    };
    assert_eq!(hits[0].primary_email, "me1@example.com");
}

#[tokio::test]
async fn get_account_returns_summary() {
    let handler = make_handler();
    let account_id = AccountId::new();

    let result = handler
        .dispatch(Query::GetAccount { account_id })
        .await
        .unwrap();

    let account = result.into_account().unwrap().unwrap();
    assert_eq!(account.name, "Test Account");
    assert_eq!(account.total_unread, 5);
}

#[tokio::test]
async fn list_accounts_returns_vec() {
    let handler = make_handler();

    let result = handler.dispatch(Query::ListAccounts).await.unwrap();

    let accounts = result.into_accounts().unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].name, "Account 1");
}

#[tokio::test]
async fn list_messages_returns_paged() {
    let handler = make_handler();

    let result = handler
        .dispatch(Query::ListMessages {
            mailbox_id: MailboxId::new(),
            pagination: Pagination::default(),
        })
        .await
        .unwrap();

    let paged = result.into_messages().unwrap();
    assert_eq!(paged.total_count, 0);
    assert!(paged.items.is_empty());
}

#[tokio::test]
async fn search_returns_results() {
    let handler = make_handler();

    let result = handler
        .dispatch(Query::SearchMessages {
            query: "hello".into(),
            account_id: None,
            mailbox_id: None,
            pagination: Pagination::default(),
        })
        .await
        .unwrap();

    let search = result.into_search_results().unwrap();
    assert_eq!(search.total_count, 0);
}
