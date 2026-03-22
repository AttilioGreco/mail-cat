//! Query handler — dispatches read-side queries to projected read models.

#[allow(unused_imports)]
use async_trait::async_trait;
use std::sync::Arc;

use mc_domain::{AccountReadRepository, MailboxReadRepository, MessageReadRepository, SearchIndex};

use crate::error::ApplicationError;
use crate::queries::{Query, QueryResult};

/// Handles all read-side queries by delegating to the appropriate read
/// repositories and the search index.
pub struct QueryHandler<AR, MBR, MR, SI>
where
    AR: AccountReadRepository,
    MBR: MailboxReadRepository,
    MR: MessageReadRepository,
    SI: SearchIndex,
{
    account_repo: Arc<AR>,
    mailbox_repo: Arc<MBR>,
    message_repo: Arc<MR>,
    search_index: Arc<SI>,
}

impl<AR, MBR, MR, SI> QueryHandler<AR, MBR, MR, SI>
where
    AR: AccountReadRepository,
    MBR: MailboxReadRepository,
    MR: MessageReadRepository,
    SI: SearchIndex,
{
    pub fn new(
        account_repo: Arc<AR>,
        mailbox_repo: Arc<MBR>,
        message_repo: Arc<MR>,
        search_index: Arc<SI>,
    ) -> Self {
        Self {
            account_repo,
            mailbox_repo,
            message_repo,
            search_index,
        }
    }

    /// Dispatch a query to the appropriate handler.
    pub async fn dispatch(&self, query: Query) -> Result<QueryResult, ApplicationError> {
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mc_domain::{
        AccountId, AccountStatus, AccountSummary, DomainError, EmailAddress, Mailbox, MailboxId,
        Message, MessageId, MessageSummary, ProviderType, SearchResults,
    };
    use mc_shared::pagination::{PagedResult, Pagination};

    // ── Mock AccountReadRepository ─────────────────────────────────────

    struct MockAccountReadRepo;

    #[async_trait]
    impl AccountReadRepository for MockAccountReadRepo {
        async fn get_account(&self, id: &AccountId) -> Result<Option<AccountSummary>, DomainError> {
            Ok(Some(AccountSummary {
                id: id.clone(),
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

        async fn rebuild(&self, _messages: Vec<Message>) -> Result<u64, DomainError> {
            Ok(0)
        }
    }

    // ── Tests ──────────────────────────────────────────────────────────

    fn make_handler()
    -> QueryHandler<MockAccountReadRepo, MockMailboxReadRepo, MockMessageReadRepo, MockSearchIndex>
    {
        QueryHandler::new(
            Arc::new(MockAccountReadRepo),
            Arc::new(MockMailboxReadRepo),
            Arc::new(MockMessageReadRepo),
            Arc::new(MockSearchIndex),
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
