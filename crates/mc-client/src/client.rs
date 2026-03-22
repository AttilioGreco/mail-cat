use tokio_stream::Stream;
use tonic::transport::Channel;

use crate::{
    convert::*,
    error::{ClientError, Result},
    proto::mail_cat::v1::{
        self as pb,
        account_service_client::AccountServiceClient,
        mailbox_service_client::MailboxServiceClient,
        message_service_client::MessageServiceClient,
        search_service_client::SearchServiceClient,
    },
    types::*,
};

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
                pagination: Some(pb::Pagination { page: 1, page_size: 100 }),
            })
            .await?
            .into_inner();

        Ok(resp.accounts.into_iter().map(account_from_proto).collect())
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
                pagination: Some(pb::Pagination { page: 1, page_size: 200 }),
            })
            .await?
            .into_inner();

        Ok(resp.messages.into_iter().map(message_summary_from_proto).collect())
    }

    pub async fn get_message(&self, message_id: &str) -> Result<MessageBody> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .get_message(pb::GetMessageRequest {
                message_id: message_id.to_owned(),
            })
            .await?
            .into_inner();

        let detail = resp
            .message
            .ok_or_else(|| ClientError::InvalidResponse("missing message in GetMessageResponse".into()))?;

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

        let summary = resp
            .message
            .ok_or_else(|| ClientError::InvalidResponse("missing message in SetFlagsResponse".into()))?;

        Ok(message_summary_from_proto(summary))
    }

    /// Move a message to another mailbox.
    pub async fn move_message(
        &self,
        message_id: &str,
        to_mailbox_id: &str,
    ) -> Result<MessageSummary> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .move_message(pb::MoveMessageRequest {
                message_id: message_id.to_owned(),
                to_mailbox_id: to_mailbox_id.to_owned(),
            })
            .await?
            .into_inner();

        let summary = resp
            .message
            .ok_or_else(|| ClientError::InvalidResponse("missing message in MoveMessageResponse".into()))?;

        Ok(message_summary_from_proto(summary))
    }

    /// Delete a message. If `permanently` is true, expunge immediately.
    pub async fn delete_message(
        &self,
        message_id: &str,
        permanently: bool,
    ) -> Result<()> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        client
            .delete_message(pb::DeleteMessageRequest {
                message_id: message_id.to_owned(),
                permanently,
            })
            .await?;

        Ok(())
    }

    /// Send a new email.
    pub async fn send_message(&self, params: SendMessageParams) -> Result<String> {
        let mut client = MessageServiceClient::new(self.channel.clone());
        let resp = client
            .send_message(pb::SendMessageRequest {
                account_id: params.account_id,
                from: Some(pb::EmailAddress {
                    email: params.from_email,
                    name: params.from_name,
                }),
                to: params
                    .to
                    .into_iter()
                    .map(|e| pb::EmailAddress { email: e, name: None })
                    .collect(),
                cc: params
                    .cc
                    .into_iter()
                    .map(|e| pb::EmailAddress { email: e, name: None })
                    .collect(),
                bcc: params
                    .bcc
                    .into_iter()
                    .map(|e| pb::EmailAddress { email: e, name: None })
                    .collect(),
                subject: params.subject,
                body_text: params.body_text,
                body_html: params.body_html,
                in_reply_to: params.in_reply_to,
                references: params.references,
            })
            .await?
            .into_inner();

        Ok(resp.message_id)
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
                pagination: Some(pb::Pagination { page: 1, page_size: 50 }),
            })
            .await?
            .into_inner();

        Ok(resp.hits.into_iter().map(search_hit_from_proto).collect())
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
    ) -> Result<impl Stream<Item = Result<SyncProgress>>> {
        let mut client = AccountServiceClient::new(self.channel.clone());
        let stream = client
            .sync_account(pb::SyncAccountRequest {
                account_id: account_id.to_owned(),
                full_sync,
            })
            .await?
            .into_inner();

        Ok(tokio_stream::StreamExt::map(stream, |item| {
            item.map(sync_progress_from_proto).map_err(ClientError::Grpc)
        }))
    }
}
