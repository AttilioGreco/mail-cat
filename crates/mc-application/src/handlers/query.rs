//! Query handler — dispatches read-side queries to projected read models.

#[allow(unused_imports)]
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;

use mc_domain::{
    AccountId, AccountReadRepository, ContactReadRepository, ContactSearchHit,
    ConversationReadRepository, IndexHit, MailboxReadRepository, Message, MessageReadRepository,
    SearchHit, SearchIndex, SearchResults,
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
                let found = self
                    .search_index
                    .search(&query, account_id.as_ref(), mailbox_id.as_ref(), &pagination)
                    .await?;
                let mut hits = Vec::with_capacity(found.hits.len());
                for IndexHit { message_id, score } in found.hits {
                    // The index lags the read model; a message deleted since is skipped.
                    let Some(message) = self.message_repo.get_message(&message_id).await? else {
                        continue;
                    };
                    let snippet = self.search_index.snippet(&query, &message);
                    hits.push(search_hit(&message, score, snippet));
                }
                Ok(QueryResult::SearchResults(SearchResults { hits, total_count: found.total_count }))
            }
            Query::SuggestMessages {
                prefix,
                scope,
                max_per_field,
            } => {
                let suggestions = self.message_repo.suggest(&prefix, scope, max_per_field).await?;
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

    /// Own-account addresses first; others only fill the remaining slots, and
    /// never repeat an address already suggested.
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
        let (own, cross): (Vec<_>, Vec<_>) = self
            .contact_repo
            .search_contacts(query.trim(), limit.saturating_mul(4))
            .await?
            .into_iter()
            .map(|c| ContactSearchHit::from_summary(c, requesting_account))
            .partition(|hit| !hit.is_cross_account);
        let mut seen = HashSet::new();
        Ok(own
            .into_iter()
            .chain(cross)
            .filter(|hit| seen.insert(hit.primary_email.to_lowercase()))
            .take(limit)
            .collect())
    }
}

fn search_hit(message: &Message, score: f32, snippet: Option<String>) -> SearchHit {
    let from = &message.envelope.from;
    let names: Vec<&str> = from.iter().filter_map(|a| a.name.as_deref()).collect();
    SearchHit {
        message_id: message.id,
        mailbox_ids: message.memberships.iter().map(|m| m.mailbox_id).collect(),
        score,
        snippet,
        subject: message.envelope.subject.clone(),
        from_name: (!names.is_empty()).then(|| names.join(" ")),
        from_email: from.iter().map(|a| a.email.as_str()).collect::<Vec<_>>().join(" "),
        date: message.envelope.date.timestamp(),
    }
}

#[cfg(test)]
mod tests;
