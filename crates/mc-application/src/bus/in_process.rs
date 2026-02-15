//! Concrete in-process implementations of [`CommandBus`] and [`QueryBus`].
//!
//! These wire together the handler modules and dispatch commands/queries
//! to the appropriate handler methods.

use async_trait::async_trait;
use std::sync::Arc;

use mc_domain::{
    AccountReadRepository, AccountWriteRepository, EventStore, MailboxReadRepository,
    MessageReadRepository, ProviderRegistry, SearchIndex,
};

use crate::commands::{Command, CommandResult};
use crate::error::ApplicationError;
use crate::handlers::account::AccountCommandHandler;
use crate::handlers::message::MessageCommandHandler;
use crate::handlers::query::QueryHandler;
use crate::queries::{Query, QueryResult};

use super::CommandBus;
use super::QueryBus;
use super::event_bus::EventPublisher;

/// In-process command bus that dispatches to Account and Message handlers.
pub struct InProcessCommandBus<AW, ES, PR, B>
where
    AW: AccountWriteRepository,
    ES: EventStore,
    PR: ProviderRegistry,
    B: EventPublisher,
{
    account_handler: AccountCommandHandler<AW, B>,
    message_handler: MessageCommandHandler<ES, PR, B>,
}

impl<AW, ES, PR, B> InProcessCommandBus<AW, ES, PR, B>
where
    AW: AccountWriteRepository,
    ES: EventStore,
    PR: ProviderRegistry,
    B: EventPublisher,
{
    pub fn new(
        account_write_repo: Arc<AW>,
        event_store: Arc<ES>,
        provider_registry: Arc<PR>,
        event_bus: Arc<B>,
    ) -> Self {
        Self {
            account_handler: AccountCommandHandler::new(account_write_repo, event_bus.clone()),
            message_handler: MessageCommandHandler::new(event_store, provider_registry, event_bus),
        }
    }
}

#[async_trait]
impl<AW, ES, PR, B> CommandBus for InProcessCommandBus<AW, ES, PR, B>
where
    AW: AccountWriteRepository,
    ES: EventStore,
    PR: ProviderRegistry,
    B: EventPublisher,
{
    async fn dispatch(&self, command: Command) -> Result<CommandResult, ApplicationError> {
        match command {
            Command::AddAccount {
                name,
                email,
                provider_type,
                connection_config,
                identity,
            } => {
                self.account_handler
                    .handle_add(name, email, provider_type, connection_config, identity)
                    .await
            }
            Command::UpdateAccount {
                account_id,
                name,
                connection_config,
            } => {
                self.account_handler
                    .handle_update(account_id, name, connection_config)
                    .await
            }
            Command::RemoveAccount { account_id } => {
                self.account_handler.handle_remove(account_id).await
            }
            Command::SyncAccount { account_id: _ } => {
                // SyncAccount is a complex multi-step operation handled by
                // SyncService (not yet implemented). For now, return SyncStarted.
                Ok(CommandResult::SyncStarted)
            }
            Command::FetchMessageBody {
                message_id,
                mailbox_path,
            } => {
                self.message_handler
                    .handle_fetch_body(message_id, mailbox_path)
                    .await
            }
            Command::SetMessageFlags {
                message_id,
                mailbox_path,
                flags_to_add,
                flags_to_remove,
            } => {
                self.message_handler
                    .handle_set_flags(message_id, mailbox_path, flags_to_add, flags_to_remove)
                    .await
            }
            Command::MoveMessage {
                message_id,
                from_mailbox_id,
                to_mailbox_id,
                from_mailbox_path,
                to_mailbox_path,
            } => {
                self.message_handler
                    .handle_move(
                        message_id,
                        from_mailbox_id,
                        to_mailbox_id,
                        from_mailbox_path,
                        to_mailbox_path,
                    )
                    .await
            }
            Command::DeleteMessage {
                message_id,
                mailbox_id,
                mailbox_path,
                permanently,
            } => {
                self.message_handler
                    .handle_delete(message_id, mailbox_id, mailbox_path, permanently)
                    .await
            }
            Command::SendMessage {
                account_id,
                outgoing,
            } => self.message_handler.handle_send(account_id, outgoing).await,
            Command::SaveDraft {
                account_id: _,
                outgoing: _,
            } => {
                // Draft saving will be implemented with full infrastructure.
                Ok(CommandResult::DraftSaved {
                    message_id: mc_domain::MessageId::new(),
                })
            }
            Command::DiscardDraft { message_id } => {
                self.message_handler.handle_discard_draft(message_id).await
            }
        }
    }
}

/// In-process query bus that dispatches to the query handler.
pub struct InProcessQueryBus<AR, MBR, MR, SI>
where
    AR: AccountReadRepository,
    MBR: MailboxReadRepository,
    MR: MessageReadRepository,
    SI: SearchIndex,
{
    query_handler: QueryHandler<AR, MBR, MR, SI>,
}

impl<AR, MBR, MR, SI> InProcessQueryBus<AR, MBR, MR, SI>
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
            query_handler: QueryHandler::new(
                account_repo,
                mailbox_repo,
                message_repo,
                search_index,
            ),
        }
    }
}

#[async_trait]
impl<AR, MBR, MR, SI> QueryBus for InProcessQueryBus<AR, MBR, MR, SI>
where
    AR: AccountReadRepository,
    MBR: MailboxReadRepository,
    MR: MessageReadRepository,
    SI: SearchIndex,
{
    async fn dispatch(&self, query: Query) -> Result<QueryResult, ApplicationError> {
        self.query_handler.dispatch(query).await
    }
}
