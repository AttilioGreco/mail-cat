use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::model::account::AccountId;
use crate::model::attachment::Attachment;
use crate::model::mailbox::MailboxId;
use crate::value_objects::email_address::EmailAddress;
use crate::value_objects::flag::Flag;
use crate::value_objects::message_id::MessageId;
use crate::value_objects::uid::ProviderUid;

/// An address with optional display name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address {
    pub name: Option<String>,
    pub email: EmailAddress,
}

/// Message envelope (header summary).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub from: Vec<Address>,
    pub to: Vec<Address>,
    pub cc: Vec<Address>,
    pub bcc: Vec<Address>,
    pub reply_to: Vec<Address>,
    pub subject: String,
    pub date: DateTime<Utc>,
}

/// Message aggregate root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: MessageId,
    pub account_id: AccountId,
    pub mailbox_id: MailboxId,
    pub provider_uid: Option<ProviderUid>,
    pub message_rfc_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub envelope: Envelope,
    pub flags: Vec<Flag>,
    pub size_bytes: u64,
    pub has_attachments: bool,
    pub attachments: Vec<Attachment>,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    pub raw_headers: Option<String>,
    pub received_at: DateTime<Utc>,
    pub internal_date: DateTime<Utc>,
}

/// Outgoing message for sending.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutgoingMessage {
    pub from: Address,
    pub to: Vec<Address>,
    pub cc: Vec<Address>,
    pub bcc: Vec<Address>,
    pub subject: String,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub attachments: Vec<OutgoingAttachment>,
}

/// Attachment for outgoing messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutgoingAttachment {
    pub filename: String,
    pub mime_type: String,
    pub data: Vec<u8>,
}
