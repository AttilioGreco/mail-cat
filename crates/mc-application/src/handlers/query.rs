//! Query handler — dispatches read-side queries to projected read models.

#[allow(unused_imports)]
use async_trait::async_trait;
use std::sync::Arc;

use mc_domain::{
    AccountReadRepository, ContactIndex, ConversationReadRepository, MailboxReadRepository,
    MessageReadRepository, SearchIndex,
};

use crate::error::ApplicationError;
use crate::queries::{Query, QueryResult};

/// Handles all read-side queries by delegating to the appropriate read
/// repositories and the search index.
pub struct QueryHandler<AR, MBR, MR, SI, CI, CR>
where
    AR: AccountReadRepository,
    MBR: MailboxReadRepository,
    MR: MessageReadRepository,
    SI: SearchIndex,
    CI: ContactIndex,
    CR: ConversationReadRepository,
{
    account_repo: Arc<AR>,
    mailbox_repo: Arc<MBR>,
    message_repo: Arc<MR>,
    search_index: Arc<SI>,
    contact_index: Arc<CI>,
    conversation_repo: Arc<CR>,
}

impl<AR, MBR, MR, SI, CI, CR> QueryHandler<AR, MBR, MR, SI, CI, CR>
where
    AR: AccountReadRepository,
    MBR: MailboxReadRepository,
    MR: MessageReadRepository,
    SI: SearchIndex,
    CI: ContactIndex,
    CR: ConversationReadRepository,
{
    pub fn new(
        account_repo: Arc<AR>,
        mailbox_repo: Arc<MBR>,
        message_repo: Arc<MR>,
        search_index: Arc<SI>,
        contact_index: Arc<CI>,
        conversation_repo: Arc<CR>,
    ) -> Self {
        Self {
            account_repo,
            mailbox_repo,
            message_repo,
            search_index,
            contact_index,
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
                let hits = self
                    .contact_index
                    .search_contacts(&query, limit, requesting_account_id.as_ref())
                    .await?;
                Ok(QueryResult::ContactHits(hits))
            }
            Query::FindContactDuplicates { account_id } => {
                let pairs = self
                    .contact_index
                    .find_duplicate_candidates(account_id.as_ref())
                    .await?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use mc_domain::{
        AccountId, AccountStatus, AccountSummary, ContactIndex, DomainError, EmailAddress, Mailbox,
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

        async fn scan_all_messages(
            &self,
            _account_id: Option<&mc_domain::AccountId>,
        ) -> Result<Vec<mc_domain::Message>, DomainError> {
            Ok(vec![])
        }
    }

    // ── Mock ContactIndex ─────────────────────────────────────────────

    struct MockContactIndex;

    #[async_trait]
    impl ContactIndex for MockContactIndex {
        async fn index_contact(
            &self,
            _summary: &mc_domain::ContactSummary,
        ) -> Result<(), DomainError> {
            Ok(())
        }

        async fn remove_contact(&self, _id: &mc_domain::ContactId) -> Result<(), DomainError> {
            Ok(())
        }

        async fn search_contacts(
            &self,
            _query: &str,
            _limit: usize,
            _requesting_account_id: Option<&mc_domain::AccountId>,
        ) -> Result<Vec<mc_domain::ContactSearchHit>, DomainError> {
            Ok(vec![])
        }

        async fn find_duplicate_candidates(
            &self,
            _account_id: Option<&mc_domain::AccountId>,
        ) -> Result<Vec<(mc_domain::ContactSearchHit, mc_domain::ContactSearchHit)>, DomainError> {
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

    fn make_handler() -> QueryHandler<
        MockAccountReadRepo,
        MockMailboxReadRepo,
        MockMessageReadRepo,
        MockSearchIndex,
        MockContactIndex,
        MockConversationRepo,
    > {
        QueryHandler::new(
            Arc::new(MockAccountReadRepo),
            Arc::new(MockMailboxReadRepo),
            Arc::new(MockMessageReadRepo),
            Arc::new(MockSearchIndex),
            Arc::new(MockContactIndex),
            Arc::new(MockConversationRepo),
        )
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
