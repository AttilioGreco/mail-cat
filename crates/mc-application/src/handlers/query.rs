//! Query handler — dispatches read-side queries to projected read models.

#[allow(unused_imports)]
use async_trait::async_trait;
use std::sync::Arc;

use mc_domain::{
    AccountId, AccountReadRepository, ContactReadRepository, ContactSearchHit,
    ConversationReadRepository, MailboxReadRepository,
    MessageReadRepository, SearchIndex,
};

use crate::error::ApplicationError;
use crate::queries::{Query, QueryResult};

/// Handles all read-side queries by delegating to the appropriate read
/// repositories and the search index.
pub struct QueryHandler<AR, MBR, MR, SI, CTR, CR>
where
    AR: AccountReadRepository,
    MBR: MailboxReadRepository,
    MR: MessageReadRepository,
    SI: SearchIndex,
    CTR: ContactReadRepository,
    CR: ConversationReadRepository,
{
    account_repo: Arc<AR>,
    mailbox_repo: Arc<MBR>,
    message_repo: Arc<MR>,
    search_index: Arc<SI>,
    contact_repo: Arc<CTR>,
    conversation_repo: Arc<CR>,
}

impl<AR, MBR, MR, SI, CTR, CR> QueryHandler<AR, MBR, MR, SI, CTR, CR>
where
    AR: AccountReadRepository,
    MBR: MailboxReadRepository,
    MR: MessageReadRepository,
    SI: SearchIndex,
    CTR: ContactReadRepository,
    CR: ConversationReadRepository,
{
    pub fn new(
        account_repo: Arc<AR>,
        mailbox_repo: Arc<MBR>,
        message_repo: Arc<MR>,
        search_index: Arc<SI>,
        contact_repo: Arc<CTR>,
        conversation_repo: Arc<CR>,
    ) -> Self {
        Self {
            account_repo,
            mailbox_repo,
            message_repo,
            search_index,
            contact_repo,
            conversation_repo,
        }
    }

    /// Dispatch a query to the appropriate handler.
    pub async fn dispatch(&self, query: Query) -> Result<QueryResult, ApplicationError>
    where
        CR: ConversationReadRepository,
    {
        match query {
            Query::GetAccount { account_id } => {
                let account = self.account_repo.get_account(&account_id).await?;
                Ok(QueryResult::Account(account))
            }
            Query::ListAccounts => {
                let accounts = self.account_repo.list_accounts().await?;
                Ok(QueryResult::Accounts(accounts))
            }
            Query::GetAccountStats { account_id } => {
                let stats = self.account_repo.account_stats(&account_id).await?;
                Ok(QueryResult::AccountStats(stats))
            }
            Query::GetMailbox { mailbox_id } => {
                let mailbox = self.mailbox_repo.get_mailbox(&mailbox_id).await?;
                Ok(QueryResult::Mailbox(mailbox))
            }
            Query::ListMailboxes { account_id } => {
                let mailboxes = self.mailbox_repo.list_mailboxes(&account_id).await?;
                Ok(QueryResult::Mailboxes(mailboxes))
            }
            Query::GetMessage { message_id } => {
                let message = self.message_repo.get_message(&message_id).await?;
                Ok(QueryResult::Message(Box::new(message)))
            }
            Query::ListMessages {
                mailbox_id,
                pagination,
            } => {
                let messages = self
                    .message_repo
                    .list_messages(&mailbox_id, &pagination)
                    .await?;
                Ok(QueryResult::Messages(messages))
            }
            Query::GetThread { message_id } => {
                let thread = self.message_repo.get_thread(&message_id).await?;
                Ok(QueryResult::Thread(Box::new(thread)))
            }
            Query::SearchMessages {
                query,
                account_id,
                mailbox_id,
                pagination,
            } => {
                let results = self
                    .search_index
                    .search(
                        &query,
                        account_id.as_ref(),
                        mailbox_id.as_ref(),
                        &pagination,
                    )
                    .await?;
                Ok(QueryResult::SearchResults(results))
            }
            Query::SuggestMessages {
                prefix,
                scope,
                max_per_field,
            } => {
                let suggestions = self
                    .search_index
                    .suggest(&prefix, scope, max_per_field)
                    .await?;
                Ok(QueryResult::Suggestions(suggestions))
            }
            Query::SearchContacts { query, limit, requesting_account_id } => {
                Ok(QueryResult::ContactHits(
                    self.search_contacts(&query, limit, requesting_account_id.as_ref()).await?,
                ))
            }
            Query::FindContactDuplicates { account_id } => {
                let pairs = self
                    .contact_repo
                    .find_duplicate_candidates(account_id.as_ref())
                    .await?
                    .into_iter()
                    .map(|(a, b)| {
                        (ContactSearchHit::from_summary(a, None), ContactSearchHit::from_summary(b, None))
                    })
                    .collect();
                Ok(QueryResult::ContactDuplicates(pairs))
            }
            Query::ListConversations { account_id, mailbox_id, pagination } => {
                let convs = self
                    .conversation_repo
                    .list_conversations(&account_id, mailbox_id.as_ref(), &pagination)
                    .await?;
                Ok(QueryResult::Conversations(convs))
            }
            Query::GetConversation { thread_id, account_id } => {
                let conv = self
                    .conversation_repo
                    .get_conversation(&thread_id, &account_id)
                    .await?;
                Ok(QueryResult::ConversationDetail(conv))
            }
            Query::RebuildConversationIndex => {
                let count = self.conversation_repo.rebuild_index().await?;
                Ok(QueryResult::IndexRebuilt(count))
            }
        }
    }

    /// Own-account contacts first; others only fill the remaining slots.
    async fn search_contacts(
        &self,
        query: &str,
        limit: usize,
        requesting_account: Option<&AccountId>,
    ) -> Result<Vec<ContactSearchHit>, ApplicationError> {
        if query.trim().is_empty() {
            return Ok(Vec::new());
        }
        // Over-fetch so cross-account matches can't crowd out own-account ones.
        let (mut own, cross): (Vec<_>, Vec<_>) = self
            .contact_repo
            .search_contacts(query.trim(), limit.saturating_mul(4))
            .await?
            .into_iter()
            .map(|c| ContactSearchHit::from_summary(c, requesting_account))
            .partition(|hit| !hit.is_cross_account);
        own.truncate(limit);
        let room = limit - own.len();
        own.extend(cross.into_iter().take(room));
        Ok(own)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mc_domain::{
        AccountId, AccountStatus, AccountSummary, ContactSummary, DomainError, EmailAddress, Mailbox,
        MailboxId, Message, MessageId, MessageSummary, ProviderType, SearchResults,
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
                status: AccountStatus::Active,
                total_unread: 5,
            }))
        }

        async fn list_accounts(&self) -> Result<Vec<AccountSummary>, DomainError> {
            Ok(vec![AccountSummary {
                id: AccountId::new(),
                name: "Account 1".into(),
                email: EmailAddress::new("a1@example.com").unwrap(),
                provider_type: ProviderType::Imap,
                status: AccountStatus::Active,
                total_unread: 3,
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

        async fn list_mailboxes(
            &self,
            _account_id: &AccountId,
        ) -> Result<Vec<Mailbox>, DomainError> {
            Ok(vec![])
        }
    }

    // ── Mock MessageReadRepository ────────────────────────────────────

    struct MockMessageReadRepo;

    #[async_trait]
    impl MessageReadRepository for MockMessageReadRepo {
        async fn get_message(&self, _id: &MessageId) -> Result<Option<Message>, DomainError> {
            Ok(None)
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

        async fn find_by_provider_uid(
            &self,
            _mailbox_id: &MailboxId,
            _provider_uid: u32,
        ) -> Result<Option<MessageSummary>, DomainError> {
            Ok(None)
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

    struct MockSearchIndex;

    #[async_trait]
    impl SearchIndex for MockSearchIndex {
        async fn index_message(&self, _message: &Message) -> Result<(), DomainError> {
            Ok(())
        }

        async fn remove_message(&self, _id: &MessageId) -> Result<(), DomainError> {
            Ok(())
        }

        async fn search(
            &self,
            _query: &str,
            _account_id: Option<&AccountId>,
            _mailbox_id: Option<&MailboxId>,
            _pagination: &Pagination,
        ) -> Result<SearchResults, DomainError> {
            Ok(SearchResults {
                hits: vec![],
                total_count: 0,
            })
        }

        async fn suggest(
            &self,
            _prefix: &str,
            _scope: Option<mc_domain::SuggestScope>,
            _max_per_field: usize,
        ) -> Result<Vec<mc_domain::Suggestion>, DomainError> {
            Ok(vec![])
        }

        async fn rebuild(&self, _messages: Vec<Message>) -> Result<u64, DomainError> {
            Ok(0)
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
            Ok(PagedResult { items: vec![], total_count: 0, page: 1, page_size: 50 })
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
        QueryHandler::new(
            Arc::new(MockAccountReadRepo),
            Arc::new(MockMailboxReadRepo),
            Arc::new(MockMessageReadRepo),
            Arc::new(MockSearchIndex),
            Arc::new(MockContactRepo { contacts }),
            Arc::new(MockConversationRepo),
        )
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
        let shown: Vec<_> = hits.iter().map(|h| (h.primary_email.as_str(), h.is_cross_account)).collect();
        assert_eq!(shown, [("me1@example.com", false), ("me2@example.com", false), ("x1@example.com", true)]);

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
}
