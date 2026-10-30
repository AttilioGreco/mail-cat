use tokio_stream::Stream;
use tonic::transport::Channel;

use crate::{
    convert::*,
    error::{ClientError, Result},
    proto::mail_cat::v1::{
        self as pb, account_service_client::AccountServiceClient,
        contact_service_client::ContactServiceClient, mailbox_service_client::MailboxServiceClient,
        message_service_client::MessageServiceClient, search_service_client::SearchServiceClient,
    },
    types::*,
};

/// Compose calls carry attachments; tonic's 4 MB default would reject them.
/// Must match the server's limit.
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// The main entry point for frontends. Wraps tonic clients and exposes clean async methods.
#[derive(Clone)]
pub struct MailCatClient {
    channel: Channel,
}

impl MailCatClient {
    /// Connect to a running mc-server instance.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| ClientError::Connection {
                endpoint: endpoint.clone(),
                reason: e.to_string(),
            })?
            .connect()
            .await
            .map_err(|e| ClientError::Connection {
                endpoint,
                reason: e.to_string(),
            })?;

        Ok(Self { channel })
    }

    // ── Accounts ──────────────────────────────────────────────────────────────

    pub async fn list_accounts(&self) -> Result<Vec<Account>> {
        let mut client = AccountServiceClient::new(self.channel.clone());
        let resp = client
            .list_accounts(pb::ListAccountsRequest {
                pagination: Some(pb::Pagination {
                    page: 1,
                    page_size: 100,
                }),
            })
            .await?
            .into_inner();

        Ok(resp.accounts.into_iter().map(account_from_proto).collect())
    }

    /// Switch how the account's mailboxes are presented; stored data is untouched.
    pub async fn set_account_mailbox_model(
        &self,
        account_id: &str,
        model: MailboxModel,
    ) -> Result<()> {
        let model = match model {
            MailboxModel::Folders => pb::MailboxModel::Folders,
            MailboxModel::Labels => pb::MailboxModel::Labels,
        };
        AccountServiceClient::new(self.channel.clone())
            .set_account_mailbox_model(pb::SetAccountMailboxModelRequest {
                account_id: account_id.to_owned(),
                model: model as i32,
            })
            .await?;
        Ok(())
    }

    /// Add an IMAP/SMTP account with plain credentials. OAuth accounts go through
    /// [`start_oauth_login`](Self::start_oauth_login) instead — the AddAccount RPC
    /// carries no OAuth provider.
    pub async fn add_imap_account(&self, params: ImapAccountParams) -> Result<Account> {
        let incoming = pb::incoming_server::Kind::Imap(pb::ImapServer {
            host: params.imap_host,
            port: u32::from(params.imap_port),
            use_tls: params.use_tls,
        });
        let smtp = params
            .smtp_host
            .zip(params.smtp_port)
            .map(|(host, port)| pb::SmtpServer {
                host,
                port: u32::from(port),
            });
        self.add_password_account(
            pb::ProviderType::Imap,
            (params.name, params.email),
            incoming,
            smtp,
            (params.username, params.password),
        )
        .await
    }

    /// Add a JMAP account with plain credentials; `jmap_url` is the session
    /// URL or just the server's domain.
    pub async fn add_jmap_account(&self, params: JmapAccountParams) -> Result<Account> {
        let incoming = pb::incoming_server::Kind::Jmap(pb::JmapServer {
            session_url: params.jmap_url,
        });
        self.add_password_account(
            pb::ProviderType::Jmap,
            (params.name, params.email),
            incoming,
            None,
            (params.username, params.password),
        )
        .await
    }

    async fn add_password_account(
        &self,
        provider_type: pb::ProviderType,
        (name, email): (String, String),
        incoming: pb::incoming_server::Kind,
        smtp: Option<pb::SmtpServer>,
        (username, password): (String, String),
    ) -> Result<Account> {
        let mut client = AccountServiceClient::new(self.channel.clone());
        let resp = client
            .add_account(pb::AddAccountRequest {
                name,
                email,
                provider_type: provider_type as i32,
                connection_config: Some(pb::ConnectionConfig {
                    incoming: Some(pb::IncomingServer {
                        kind: Some(incoming),
                    }),
                    smtp,
                    auth: Some(pb::AuthConfig {
                        auth_type: Some(pb::auth_config::AuthType::Plain(pb::PlainAuth {
                            username,
                            password,
                        })),
                    }),
                }),
            })
            .await?
            .into_inner();

        let detail = resp
            .account
            .ok_or_else(|| ClientError::InvalidResponse("AddAccount returned no account".into()))?;

        Ok(account_detail_from_proto(detail))
    }

    /// Remove an account and everything projected from it.
    pub async fn remove_account(&self, account_id: &str) -> Result<()> {
        let mut client = AccountServiceClient::new(self.channel.clone());
        client
            .remove_account(pb::RemoveAccountRequest {
                account_id: account_id.to_owned(),
            })
            .await?;
        Ok(())
    }

    /// Replace a password account's IMAP/SMTP password; active after a server restart.
    pub async fn set_account_password(&self, account_id: &str, password: &str) -> Result<()> {
        let mut client = AccountServiceClient::new(self.channel.clone());
        client
            .set_account_password(pb::SetAccountPasswordRequest {
                account_id: account_id.to_owned(),
                password: password.to_owned(),
            })
            .await?;
        Ok(())
    }

    /// How much of an account is downloaded locally.
    pub async fn account_stats(&self, account_id: &str) -> Result<AccountStats> {
        let mut client = AccountServiceClient::new(self.channel.clone());
        let resp = client
            .get_account_stats(pb::GetAccountStatsRequest {
                account_id: account_id.to_owned(),
            })
            .await?
            .into_inner();

        Ok(AccountStats {
            total_messages: resp.total_messages,
            synced_messages: resp.synced_messages,
            pending_bodies: resp.pending_bodies,
        })
    }

    /// Rename an account (updates only its display name; connection config untouched).
    pub async fn rename_account(&self, account_id: &str, name: &str) -> Result<()> {
        let mut client = AccountServiceClient::new(self.channel.clone());
        client
            .update_account(pb::UpdateAccountRequest {
                account_id: account_id.to_string(),
                name: Some(name.to_string()),
                connection_config: None,
            })
            .await?;
        Ok(())
    }

    // ── Mailboxes ─────────────────────────────────────────────────────────────

    pub async fn list_mailboxes(&self, account_id: &str) -> Result<Vec<Mailbox>> {
        let mut client = MailboxServiceClient::new(self.channel.clone());
        let resp = client
            .list_mailboxes(pb::ListMailboxesRequest {
                account_id: account_id.to_owned(),
            })
            .await?
            .into_inner();

        Ok(resp.mailboxes.into_iter().map(mailbox_from_proto).collect())
    }

    /// Subscribe or unsubscribe a mailbox. `Some` is a warning: the server could
    /// not be told, so the choice holds here only.
    pub async fn set_mailbox_subscribed(
        &self,
        mailbox_id: &str,
        subscribed: bool,
    ) -> Result<Option<String>> {
        let resp = MailboxServiceClient::new(self.channel.clone())
            .set_mailbox_subscribed(pb::SetMailboxSubscribedRequest {
                mailbox_id: mailbox_id.to_owned(),
                subscribed,
            })
            .await?
            .into_inner();
        Ok((!resp.warning.is_empty()).then_some(resp.warning))
    }

    // ── Messages ──────────────────────────────────────────────────────────────

    pub async fn list_messages(
        &self,
        account_id: &str,
        mailbox_id: &str,
    ) -> Result<Vec<MessageSummary>> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let _ = account_id; // mailbox_id is globally unique, account_id reserved for future filtering
        let resp = client
            .list_messages(pb::ListMessagesRequest {
                mailbox_id: mailbox_id.to_owned(),
                pagination: Some(pb::Pagination {
                    page: 1,
                    page_size: 200,
                }),
            })
            .await?
            .into_inner();

        Ok(resp
            .messages
            .into_iter()
            .map(message_summary_from_proto)
            .collect())
    }

    /// Every message in a mailbox, fetched a page at a time.
    pub async fn list_all_messages(&self, mailbox_id: &str) -> Result<Vec<MessageSummary>> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let mut all = Vec::new();
        for page in 1.. {
            let resp = client
                .list_messages(pb::ListMessagesRequest {
                    mailbox_id: mailbox_id.to_owned(),
                    pagination: Some(pb::Pagination {
                        page,
                        page_size: 1000,
                    }),
                })
                .await?
                .into_inner();
            all.extend(resp.messages.into_iter().map(message_summary_from_proto));
            if !resp.page_info.is_some_and(|p| p.has_next_page) {
                break;
            }
        }
        Ok(all)
    }

    /// List conversation summaries for an account (newest first).
    pub async fn list_conversations(
        &self,
        account_id: &str,
        mailbox_id: Option<&str>,
        page: u32,
        page_size: u32,
    ) -> Result<Vec<crate::types::ConversationSummary>> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .list_conversations(pb::ListConversationsRequest {
                account_id: account_id.to_owned(),
                pagination: Some(pb::Pagination { page, page_size }),
                mailbox_id: mailbox_id.map(|s| s.to_owned()),
            })
            .await?
            .into_inner();

        Ok(resp
            .conversations
            .into_iter()
            .map(crate::convert::conversation_summary_from_proto)
            .collect())
    }

    /// Get all messages in a thread by any message id within it.
    pub async fn get_thread(&self, message_id: &str) -> Result<Vec<crate::types::ThreadMessage>> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .get_thread(pb::GetThreadRequest {
                message_id: message_id.to_owned(),
            })
            .await?
            .into_inner();

        Ok(resp
            .messages
            .into_iter()
            .map(crate::convert::thread_message_from_proto)
            .collect())
    }

    /// Wipe and rebuild the conversation/thread index by replaying all stored events.
    /// Returns the number of events replayed.
    pub async fn rebuild_conversation_index(&self) -> Result<u64> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .rebuild_conversation_index(pb::RebuildConversationIndexRequest {})
            .await?
            .into_inner();
        Ok(resp.events_replayed)
    }

    /// A message as seen from `mailbox_id`, one of the mailboxes it is in.
    pub async fn get_message(&self, message_id: &str, mailbox_id: &str) -> Result<MessageBody> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .get_message(pb::GetMessageRequest {
                message_id: message_id.to_owned(),
                mailbox_id: mailbox_id.to_owned(),
            })
            .await?
            .into_inner();

        let detail = resp.message.ok_or_else(|| {
            ClientError::InvalidResponse("missing message in GetMessageResponse".into())
        })?;

        Ok(message_body_from_proto(detail))
    }

    // ── Mutations ────────────────────────────────────────────────────────────

    /// Set flags on a message (add and/or remove).
    pub async fn set_flags(
        &self,
        message_id: &str,
        flags_to_add: &[MessageFlag],
        flags_to_remove: &[MessageFlag],
    ) -> Result<MessageSummary> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .set_flags(pb::SetFlagsRequest {
                message_id: message_id.to_owned(),
                flags_to_add: flags_to_add.iter().map(message_flag_to_proto).collect(),
                flags_to_remove: flags_to_remove.iter().map(message_flag_to_proto).collect(),
            })
            .await?
            .into_inner();

        let summary = resp.message.ok_or_else(|| {
            ClientError::InvalidResponse("missing message in SetFlagsResponse".into())
        })?;

        Ok(message_summary_from_proto(summary))
    }

    /// Mark every unread message in a mailbox as read (single provider round-trip).
    pub async fn mark_mailbox_read(&self, mailbox_id: &str) -> Result<u32> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .mark_mailbox_read(pb::MarkMailboxReadRequest {
                mailbox_id: mailbox_id.to_owned(),
            })
            .await?
            .into_inner();

        Ok(resp.marked_count)
    }

    /// Move a message out of `from_mailbox_id` into another mailbox.
    pub async fn move_message(
        &self,
        message_id: &str,
        from_mailbox_id: &str,
        to_mailbox_id: &str,
    ) -> Result<MessageSummary> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .move_message(pb::MoveMessageRequest {
                message_id: message_id.to_owned(),
                from_mailbox_id: from_mailbox_id.to_owned(),
                to_mailbox_id: to_mailbox_id.to_owned(),
            })
            .await?
            .into_inner();

        let summary = resp.message.ok_or_else(|| {
            ClientError::InvalidResponse("missing message in MoveMessageResponse".into())
        })?;

        Ok(message_summary_from_proto(summary))
    }

    /// Add and remove mailboxes (labels) on messages without moving them.
    pub async fn change_labels(
        &self,
        message_ids: Vec<String>,
        add_mailbox_ids: Vec<String>,
        remove_mailbox_ids: Vec<String>,
    ) -> Result<(u32, u32)> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .change_labels(pb::ChangeLabelsRequest {
                message_ids,
                add_mailbox_ids,
                remove_mailbox_ids,
            })
            .await?
            .into_inner();
        Ok((resp.succeeded, resp.failed))
    }

    /// Take messages out of the Inbox (see the `ArchiveMessages` RPC).
    pub async fn archive_messages(&self, message_ids: Vec<String>) -> Result<(u32, u32)> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .archive_messages(pb::ArchiveMessagesRequest { message_ids })
            .await?
            .into_inner();
        Ok((resp.succeeded, resp.failed))
    }

    /// Add and remove tags (keywords) on a message.
    pub async fn modify_tags(
        &self,
        message_id: &str,
        add: Vec<String>,
        remove: Vec<String>,
    ) -> Result<MessageSummary> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .modify_tags(pb::ModifyTagsRequest {
                message_id: message_id.to_owned(),
                add,
                remove,
            })
            .await?
            .into_inner();
        let summary = resp.message.ok_or_else(|| {
            ClientError::InvalidResponse("missing message in SetFlagsResponse".into())
        })?;
        Ok(message_summary_from_proto(summary))
    }

    /// Delete a message from `mailbox_id`. If `permanently` is true, expunge immediately.
    pub async fn delete_message(
        &self,
        message_id: &str,
        mailbox_id: &str,
        permanently: bool,
    ) -> Result<()> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        client
            .delete_message(pb::DeleteMessageRequest {
                message_id: message_id.to_owned(),
                mailbox_id: mailbox_id.to_owned(),
                permanently,
            })
            .await?;

        Ok(())
    }

    /// Delete many messages, each `(message id, mailbox id)` from the mailbox
    /// it is named with, in one round-trip. Returns `(succeeded, failed)` —
    /// partial success is expected when the search index is behind the mailbox.
    pub async fn delete_messages(
        &self,
        messages: Vec<(String, String)>,
        permanently: bool,
    ) -> Result<(u32, u32)> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .delete_messages(pb::DeleteMessagesRequest {
                messages: message_refs(messages),
                permanently,
            })
            .await?
            .into_inner();

        Ok((resp.succeeded, resp.failed))
    }

    /// Move many messages, each `(message id, mailbox id)` out of the mailbox
    /// it is named with, to one mailbox in a single round-trip.
    pub async fn move_messages(
        &self,
        messages: Vec<(String, String)>,
        to_mailbox_id: &str,
    ) -> Result<(u32, u32)> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .move_messages(pb::MoveMessagesRequest {
                messages: message_refs(messages),
                to_mailbox_id: to_mailbox_id.to_owned(),
            })
            .await?
            .into_inner();

        Ok((resp.succeeded, resp.failed))
    }

    fn compose_client(&self) -> MessageServiceClient<Channel> {
        MessageServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
    }

    /// Starting state of the compose window. `mode` is one of
    /// "new" | "reply" | "replyAll" | "forward" | "draft".
    pub async fn prepare_compose(
        &self,
        mode: &str,
        message_id: Option<String>,
        account_id: Option<String>,
    ) -> Result<ComposeView> {
        let view = self
            .compose_client()
            .prepare_compose(pb::PrepareComposeRequest {
                mode: mode.to_owned(),
                message_id,
                account_id,
            })
            .await?
            .into_inner();
        Ok(compose_view_from_proto(view))
    }

    /// Send a composed message; returns the local id of the sent message.
    pub async fn send_compose(&self, input: ComposeInput) -> Result<String> {
        let resp = self
            .compose_client()
            .send_compose(compose_input_to_proto(input))
            .await?
            .into_inner();
        Ok(resp.message_id)
    }

    /// Save a draft (replacing `input.draft_id`); returns the new draft id.
    pub async fn save_draft(&self, input: ComposeInput) -> Result<String> {
        let resp = self
            .compose_client()
            .save_draft(compose_input_to_proto(input))
            .await?
            .into_inner();
        Ok(resp.draft_id)
    }

    pub async fn discard_draft(&self, draft_id: &str) -> Result<()> {
        self.compose_client()
            .discard_draft(pb::DiscardDraftRequest {
                draft_id: draft_id.to_owned(),
            })
            .await?;
        Ok(())
    }

    // ── Contacts ─────────────────────────────────────────────────────────────

    /// Prefix-match contact search for autocomplete in the compose window.
    pub async fn search_contacts(
        &self,
        query: &str,
        limit: u32,
        requesting_account_id: Option<String>,
    ) -> Result<Vec<ContactHit>> {
        let mut client = ContactServiceClient::new(self.channel.clone());
        let resp = client
            .search_contacts(pb::SearchContactsRequest {
                query: query.to_owned(),
                limit,
                requesting_account_id,
            })
            .await?
            .into_inner();

        Ok(resp.hits.into_iter().map(contact_hit_from_proto).collect())
    }

    /// Backfill the address book by scanning all stored messages and calling
    /// EnsureContact for every from/to/cc address.  Idempotent.
    /// Returns `(new_contacts, already_existed)`.
    pub async fn backfill_contacts_from_messages(
        &self,
        account_id: Option<String>,
    ) -> Result<(u32, u32)> {
        let mut client = ContactServiceClient::new(self.channel.clone());
        let resp = client
            .backfill_contacts_from_messages(pb::BackfillContactsRequest { account_id })
            .await?
            .into_inner();
        Ok((resp.new_contacts, resp.already_existed))
    }

    /// Rebuild the server-side contact search index from stored contact data.
    /// Returns the number of contacts that were indexed.
    pub async fn rebuild_contact_index(&self, account_id: Option<String>) -> Result<u32> {
        let mut client = ContactServiceClient::new(self.channel.clone());
        let resp = client
            .rebuild_contact_index(pb::RebuildContactIndexRequest { account_id })
            .await?
            .into_inner();
        Ok(resp.indexed_count)
    }

    // ── Search ──────────────────────────────────────────────────────────────

    /// Full-text search across messages.
    pub async fn search(
        &self,
        query: &str,
        account_id: Option<&str>,
        mailbox_id: Option<&str>,
    ) -> Result<Vec<SearchHit>> {
        let mut client = SearchServiceClient::new(self.channel.clone());
        let resp = client
            .search(pb::SearchRequest {
                query: query.to_owned(),
                account_id: account_id.map(|s| s.to_owned()),
                mailbox_id: mailbox_id.map(|s| s.to_owned()),
                pagination: Some(pb::Pagination {
                    page: 1,
                    page_size: 50,
                }),
            })
            .await?
            .into_inner();

        Ok(resp.hits.into_iter().map(search_hit_from_proto).collect())
    }

    /// Search-as-you-type suggestions.
    pub async fn suggest(
        &self,
        prefix: &str,
        scope: Option<&str>,
        max_per_field: u32,
    ) -> Result<Vec<Suggestion>> {
        let mut client = SearchServiceClient::new(self.channel.clone());

        let proto_scope = scope.and_then(|s| match s {
            "from" => Some(pb::SuggestScope::From as i32),
            "to" => Some(pb::SuggestScope::To as i32),
            "subject" => Some(pb::SuggestScope::Subject as i32),
            "body" => Some(pb::SuggestScope::Body as i32),
            _ => None,
        });

        let resp = client
            .suggest(pb::SuggestRequest {
                prefix: prefix.to_owned(),
                scope: proto_scope,
                max_per_field: if max_per_field == 0 { 5 } else { max_per_field },
            })
            .await?
            .into_inner();

        Ok(resp
            .suggestions
            .into_iter()
            .map(suggestion_from_proto)
            .collect())
    }

    // ── Watch ─────────────────────────────────────────────────────────────────

    /// Subscribe to real-time mailbox events. Returns a stream of notifications.
    pub async fn watch_mailbox(
        &self,
        mailbox_id: &str,
    ) -> Result<impl Stream<Item = Result<crate::types::MailboxNotification>>> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let stream = client
            .watch_mailbox(pb::WatchMailboxRequest {
                mailbox_id: mailbox_id.to_owned(),
            })
            .await?
            .into_inner();

        Ok(tokio_stream::StreamExt::map(stream, |item| {
            item.map(crate::convert::mailbox_notification_from_proto)
                .map_err(ClientError::Grpc)
        }))
    }

    // ── Sync ──────────────────────────────────────────────────────────────────

    pub async fn sync_account(
        &self,
        account_id: &str,
        full_sync: bool,
        mailbox_path: Option<&str>,
    ) -> Result<impl Stream<Item = Result<SyncProgress>>> {
        let mut client = AccountServiceClient::new(self.channel.clone());
        let stream = client
            .sync_account(pb::SyncAccountRequest {
                account_id: account_id.to_owned(),
                full_sync,
                mailbox_path: mailbox_path.map(str::to_owned),
            })
            .await?
            .into_inner();

        Ok(tokio_stream::StreamExt::map(stream, |item| {
            item.map(sync_progress_from_proto)
                .map_err(ClientError::Grpc)
        }))
    }

    /// Observe sync progress without starting a sync. `account_id` of `None`
    /// watches every account. Most syncs are server-initiated (boot, the
    /// 5-minute tick, the body backfill worker), so a client that just opened
    /// watches rather than triggering one.
    pub async fn watch_sync_progress(
        &self,
        account_id: Option<&str>,
    ) -> Result<impl Stream<Item = Result<SyncProgress>>> {
        let mut client = AccountServiceClient::new(self.channel.clone());
        let stream = client
            .watch_sync_progress(pb::WatchSyncProgressRequest {
                account_id: account_id.unwrap_or_default().to_owned(),
            })
            .await?
            .into_inner();

        Ok(tokio_stream::StreamExt::map(stream, |item| {
            item.map(sync_progress_from_proto)
                .map_err(ClientError::Grpc)
        }))
    }

    /// Every account's live auth state: one status each on connect, then one
    /// per change.
    pub async fn watch_auth_status(&self) -> Result<impl Stream<Item = Result<AuthStatus>>> {
        let mut client = AccountServiceClient::new(self.channel.clone());
        let stream = client
            .watch_auth_status(pb::WatchAuthStatusRequest {})
            .await?
            .into_inner();

        Ok(tokio_stream::StreamExt::map(stream, |item| {
            item.map(auth_status_from_proto).map_err(ClientError::Grpc)
        }))
    }

    /// Start an OAuth2 login (Google/Microsoft) for a new account. Streams an
    /// `AwaitingBrowser` event with the URL to open, then exactly one of
    /// `Succeeded`/`Failed` once the browser redirect completes.
    pub async fn start_oauth_login(
        &self,
        name: &str,
        email: &str,
        provider: crate::types::OAuthProvider,
    ) -> Result<impl Stream<Item = Result<crate::types::OAuthLoginEvent>>> {
        let mut client = AccountServiceClient::new(self.channel.clone());
        let stream = client
            .start_o_auth_login(pb::StartOAuthLoginRequest {
                name: name.to_owned(),
                email: email.to_owned(),
                provider: crate::convert::oauth_provider_to_proto(provider),
            })
            .await?
            .into_inner();

        Ok(tokio_stream::StreamExt::map(stream, |item| {
            item.map(crate::convert::oauth_login_event_from_proto)
                .map_err(ClientError::Grpc)
        }))
    }

    /// Re-run the OAuth2 login for an existing account whose refresh token died.
    /// Same event stream as [`start_oauth_login`](Self::start_oauth_login); the
    /// server resolves name/email/provider from the account id.
    pub async fn reauth_account(
        &self,
        account_id: &str,
    ) -> Result<impl Stream<Item = Result<crate::types::OAuthLoginEvent>>> {
        let mut client = AccountServiceClient::new(self.channel.clone());
        let stream = client
            .reauth_account(pb::ReauthAccountRequest {
                account_id: account_id.to_owned(),
            })
            .await?
            .into_inner();

        Ok(tokio_stream::StreamExt::map(stream, |item| {
            item.map(crate::convert::oauth_login_event_from_proto)
                .map_err(ClientError::Grpc)
        }))
    }
}

fn message_refs(messages: Vec<(String, String)>) -> Vec<pb::MessageRef> {
    messages
        .into_iter()
        .map(|(message_id, mailbox_id)| pb::MessageRef {
            message_id,
            mailbox_id,
        })
        .collect()
}
