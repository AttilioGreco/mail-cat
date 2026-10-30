use std::collections::HashMap;
use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use mc_domain::error::DomainError;
use mc_domain::{
    EmailProvider, Flag, Mailbox, MailboxChanges, MailboxPath, Message, MessageBodyResult,
    MessageEnvelopeSummary, OutgoingMessage, ProviderNotification, RemoteId, SendOutcome,
    SyncCursor,
};

use super::{ImapEmailProvider, JmapEmailProvider};

/// The account's backend. An enum rather than `dyn`: two variants, and the
/// registry stays monomorphic.
pub enum AnyProvider {
    Imap(ImapEmailProvider),
    Jmap(JmapEmailProvider),
}

macro_rules! delegate {
    ($self:ident, $p:ident => $call:expr) => {
        match $self {
            Self::Imap($p) => $call,
            Self::Jmap($p) => $call,
        }
    };
}

impl AnyProvider {
    /// A login that proves the server accepts the credential, without
    /// touching any mail.
    pub async fn verify_login(&self) -> Result<(), DomainError> {
        delegate!(self, p => p.verify_login().await)
    }
}

#[async_trait]
impl EmailProvider for AnyProvider {
    async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
        delegate!(self, p => p.list_mailboxes().await)
    }

    async fn mailbox_changes(
        &self,
        mailbox: &MailboxPath,
        since: Option<&SyncCursor>,
    ) -> Result<MailboxChanges, DomainError> {
        delegate!(self, p => p.mailbox_changes(mailbox, since).await)
    }

    async fn fetch_envelopes(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
    ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
        delegate!(self, p => p.fetch_envelopes(mailbox, ids).await)
    }

    async fn fetch_message_detail(
        &self,
        mailbox: &MailboxPath,
        id: &RemoteId,
    ) -> Result<Message, DomainError> {
        delegate!(self, p => p.fetch_message_detail(mailbox, id).await)
    }

    async fn fetch_bodies_bulk(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
    ) -> Result<Vec<MessageBodyResult>, DomainError> {
        delegate!(self, p => p.fetch_bodies_bulk(mailbox, ids).await)
    }

    async fn find_by_message_id(
        &self,
        mailbox: &MailboxPath,
        rfc_id: &str,
    ) -> Result<Vec<RemoteId>, DomainError> {
        delegate!(self, p => p.find_by_message_id(mailbox, rfc_id).await)
    }

    async fn fetch_raw_message(
        &self,
        mailbox: &MailboxPath,
        id: &RemoteId,
    ) -> Result<Vec<u8>, DomainError> {
        delegate!(self, p => p.fetch_raw_message(mailbox, id).await)
    }

    async fn set_flags_bulk(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
        flags_to_add: &[Flag],
        flags_to_remove: &[Flag],
    ) -> Result<(), DomainError> {
        delegate!(self, p => p.set_flags_bulk(mailbox, ids, flags_to_add, flags_to_remove).await)
    }

    async fn move_messages(
        &self,
        ids: &[RemoteId],
        from: &MailboxPath,
        to: &MailboxPath,
    ) -> Result<HashMap<RemoteId, Option<RemoteId>>, DomainError> {
        delegate!(self, p => p.move_messages(ids, from, to).await)
    }

    async fn change_labels(
        &self,
        ids: &[RemoteId],
        in_mailbox: &MailboxPath,
        add: &[MailboxPath],
        remove: &[MailboxPath],
    ) -> Result<(), DomainError> {
        delegate!(self, p => p.change_labels(ids, in_mailbox, add, remove).await)
    }

    async fn set_subscribed(
        &self,
        mailbox: &MailboxPath,
        subscribed: bool,
    ) -> Result<(), DomainError> {
        delegate!(self, p => p.set_subscribed(mailbox, subscribed).await)
    }

    fn backed_by(&self, mailbox: &MailboxPath) -> Option<MailboxPath> {
        delegate!(self, p => p.backed_by(mailbox))
    }

    fn preferred_mailbox_model(&self) -> Option<mc_domain::MailboxModel> {
        delegate!(self, p => p.preferred_mailbox_model())
    }

    fn supports_tags(&self) -> Option<bool> {
        delegate!(self, p => p.supports_tags())
    }

    async fn delete_messages(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
        permanently: bool,
    ) -> Result<(), DomainError> {
        delegate!(self, p => p.delete_messages(mailbox, ids, permanently).await)
    }

    async fn send_message(
        &self,
        message: &OutgoingMessage,
        sent: Option<&MailboxPath>,
    ) -> Result<SendOutcome, DomainError> {
        delegate!(self, p => p.send_message(message, sent).await)
    }

    async fn save_draft(
        &self,
        drafts: &MailboxPath,
        message: &OutgoingMessage,
        replacing: Option<&RemoteId>,
    ) -> Result<RemoteId, DomainError> {
        delegate!(self, p => p.save_draft(drafts, message, replacing).await)
    }

    fn watch_mailbox(
        &self,
        mailbox: &MailboxPath,
    ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>> {
        delegate!(self, p => p.watch_mailbox(mailbox))
    }
}
