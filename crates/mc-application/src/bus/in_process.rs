//! Concrete in-process implementations of [`CommandBus`] and [`QueryBus`].
//!
//! These wire together the handler modules and dispatch commands/queries
//! to the appropriate handler methods.

use async_trait::async_trait;
use std::sync::Arc;

use mc_domain::{
    Account, AccountReadRepository, AccountStatus, AccountWriteRepository, ContactReadRepository,
    ContactWriteRepository, ConversationReadRepository, EventStore, MailboxReadRepository,
    MessageBodyStore, MessageReadRepository, ProviderRegistry, ReadModelProjector, SearchIndex,
};
use std::collections::HashSet;

use crate::commands::{Command, CommandResult};
use crate::error::ApplicationError;
use crate::handlers::account::AccountCommandHandler;
use crate::handlers::contact::ContactCommandHandler;
use crate::handlers::message::MessageCommandHandler;
use crate::handlers::query::QueryHandler;
use crate::queries::{Query, QueryResult};

use super::CommandBus;
use super::QueryBus;
use super::event_bus::EventPublisher;

/// In-process command bus that dispatches to Account, Message, and Contact handlers.
pub struct InProcessCommandBus<AW, CW, ES, PR, MR, B>
where
    AW: AccountWriteRepository + AccountReadRepository,
    CW: ContactWriteRepository + ContactReadRepository,
    ES: EventStore,
    PR: ProviderRegistry,
    MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore,
    B: EventPublisher,
{
    account_handler: AccountCommandHandler<AW, B>,
    contact_handler: ContactCommandHandler<CW, B>,
    message_handler: MessageCommandHandler<ES, PR, MR, B>,
    /// Direct access to the message repo for contact backfill scanning.
    message_repo: Arc<MR>,
    /// Compose needs every account's identity (sender choice, own addresses).
    account_repo: Arc<AW>,
}

impl<AW, CW, ES, PR, MR, B> InProcessCommandBus<AW, CW, ES, PR, MR, B>
where
    AW: AccountWriteRepository + AccountReadRepository,
    CW: ContactWriteRepository + ContactReadRepository,
    ES: EventStore + 'static,
    PR: ProviderRegistry + 'static,
    MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore + 'static,
    B: EventPublisher + 'static,
{
    pub fn new(
        account_write_repo: Arc<AW>,
        contact_write_repo: Arc<CW>,
        event_store: Arc<ES>,
        provider_registry: Arc<PR>,
        message_read_repo: Arc<MR>,
        event_bus: Arc<B>,
    ) -> Self {
        Self {
            account_handler: AccountCommandHandler::new(account_write_repo.clone(), event_bus.clone()),
            contact_handler: ContactCommandHandler::new(contact_write_repo, event_bus.clone()),
            message_handler: MessageCommandHandler::new(
                event_store,
                provider_registry,
                message_read_repo.clone(),
                event_bus,
            ),
            message_repo: message_read_repo,
            account_repo: account_write_repo,
        }
    }

    /// Where message commands ask for a mailbox sync when their local record
    /// fell behind what the server did.
    #[must_use]
    pub fn with_resync_requests(
        mut self,
        resync: tokio::sync::mpsc::UnboundedSender<(mc_domain::AccountId, mc_domain::MailboxPath)>,
    ) -> Self {
        self.message_handler = self.message_handler.with_resync_requests(resync);
        self
    }

    /// Full aggregates of the active accounts, in listing order.
    async fn accounts(&self) -> Result<Vec<Account>, ApplicationError> {
        let mut accounts = Vec::new();
        for summary in self.account_repo.list_accounts().await? {
            if summary.status == AccountStatus::Disabled {
                continue;
            }
            accounts.extend(self.account_repo.load(&summary.id).await?);
        }
        Ok(accounts)
    }
}

#[async_trait]
impl<AW, CW, ES, PR, MR, B> CommandBus for InProcessCommandBus<AW, CW, ES, PR, MR, B>
where
    AW: AccountWriteRepository + AccountReadRepository,
    CW: ContactWriteRepository + ContactReadRepository,
    ES: EventStore + 'static,
    PR: ProviderRegistry + 'static,
    MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore + 'static,
    B: EventPublisher + 'static,
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
            Command::MarkMailboxRead {
                mailbox_id,
                mailbox_path,
            } => {
                self.message_handler
                    .handle_mark_mailbox_read(mailbox_id, mailbox_path)
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
            Command::DeleteMessages {
                message_ids,
                permanently,
            } => {
                self.message_handler
                    .handle_delete_many(message_ids, permanently)
                    .await
            }
            Command::MoveMessages {
                message_ids,
                to_mailbox_id,
                to_mailbox_path,
            } => {
                self.message_handler
                    .handle_move_many(message_ids, to_mailbox_id, to_mailbox_path)
                    .await
            }
            Command::PrepareCompose {
                mode,
                message_id,
                account_id,
            } => {
                let accounts = self.accounts().await?;
                self.message_handler
                    .handle_prepare_compose(&accounts, mode, message_id, account_id)
                    .await
            }
            Command::SendCompose { input } => {
                let accounts = self.accounts().await?;
                self.message_handler.handle_send_compose(&accounts, input).await
            }
            Command::SaveDraft { input } => {
                let accounts = self.accounts().await?;
                self.message_handler.handle_save_draft(&accounts, input).await
            }
            Command::DiscardDraft { message_id } => {
                self.message_handler.handle_discard_draft(message_id).await
            }
            Command::ApplyRemoteFlagChange {
                mailbox_id,
                remote_id,
                new_flags,
            } => {
                self.message_handler
                    .handle_apply_remote_flag_change(mailbox_id, remote_id, new_flags)
                    .await
            }
            Command::AddContact {
                display_name,
                emails,
                phones,
                organization,
                notes,
                source,
                external_id,
                account_scope,
            } => {
                self.contact_handler
                    .handle_add(
                        display_name,
                        emails,
                        phones,
                        organization,
                        notes,
                        source,
                        external_id,
                        account_scope,
                    )
                    .await
            }
            Command::EnsureContact { email, display_name, account_id } => {
                self.contact_handler
                    .handle_ensure(email, display_name, account_id)
                    .await
            }
            Command::UpdateContact {
                contact_id,
                display_name,
                emails,
                phones,
                organization,
                notes,
            } => {
                self.contact_handler
                    .handle_update(contact_id, display_name, emails, phones, organization, notes)
                    .await
            }
            Command::RemoveContact { contact_id } => {
                self.contact_handler.handle_remove(contact_id).await
            }
            Command::MergeContacts { keep_id, discard_id } => {
                self.contact_handler.handle_merge(keep_id, discard_id).await
            }
            Command::RebuildContactIndex { account_id } => {
                self.contact_handler.handle_rebuild_index(account_id).await
            }
            Command::BackfillContactsFromMessages { account_id } => {
                // Scan all stored messages, deduplicate addresses in-memory,
                // then call EnsureContact for each unique (email, account) pair.
                let messages = self
                    .message_repo
                    .scan_all_messages(account_id.as_ref())
                    .await?;

                // Deduplicate: (account_id_str, email_str) → (email, name, account_id)
                let mut seen: HashSet<(String, String)> = HashSet::new();
                let mut to_ensure: Vec<(mc_domain::EmailAddress, Option<String>, mc_domain::AccountId)> =
                    Vec::new();

                for msg in messages {
                    let aid = msg.account_id;
                    let all_addrs = msg
                        .envelope
                        .from
                        .iter()
                        .chain(msg.envelope.to.iter())
                        .chain(msg.envelope.cc.iter());

                    for addr in all_addrs {
                        let key = (aid.to_string(), addr.email.to_string());
                        if seen.insert(key) {
                            to_ensure.push((addr.email.clone(), addr.name.clone(), aid));
                        }
                    }
                }

                let mut new_contacts = 0u32;
                let mut already_existed = 0u32;

                for (email, display_name, acct_id) in to_ensure {
                    if let CommandResult::ContactEnsured { created, .. } = self
                        .contact_handler
                        .handle_ensure(email, display_name, acct_id)
                        .await? {
                        if created {
                            new_contacts += 1;
                        } else {
                            already_existed += 1;
                        }
                    }
                }

                Ok(CommandResult::ContactsBackfilled { new_contacts, already_existed })
            }
        }
    }
}

/// In-process query bus that dispatches to the query handler.
pub struct InProcessQueryBus<AR, MBR, MR, SI, CTR, CR>
where
    AR: AccountReadRepository,
    MBR: MailboxReadRepository,
    MR: MessageReadRepository,
    SI: SearchIndex,
    CTR: ContactReadRepository,
    CR: ConversationReadRepository,
{
    query_handler: QueryHandler<AR, MBR, MR, SI, CTR, CR>,
}

impl<AR, MBR, MR, SI, CTR, CR> InProcessQueryBus<AR, MBR, MR, SI, CTR, CR>
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
            query_handler: QueryHandler::new(
                account_repo,
                mailbox_repo,
                message_repo,
                search_index,
                contact_repo,
                conversation_repo,
            ),
        }
    }
}

#[async_trait]
impl<AR, MBR, MR, SI, CTR, CR> QueryBus for InProcessQueryBus<AR, MBR, MR, SI, CTR, CR>
where
    AR: AccountReadRepository,
    MBR: MailboxReadRepository,
    MR: MessageReadRepository,
    SI: SearchIndex,
    CTR: ContactReadRepository,
    CR: ConversationReadRepository,
{
    async fn dispatch(&self, query: Query) -> Result<QueryResult, ApplicationError> {
        self.query_handler.dispatch(query).await
    }
}
