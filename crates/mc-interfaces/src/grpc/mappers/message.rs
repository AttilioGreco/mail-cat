use super::{
    datetime_to_timestamp, domain_address_to_proto, domain_flag_to_proto, proto_email_to_domain,
    proto_flag_to_domain, timestamp_to_datetime,
};
use crate::grpc::proto::{self, AttachmentInfo, MessageDetail, MessageEnvelope, MessageSummary};
use mc_application::compose::{ComposeInput, ComposeMode, ComposeView};
use mc_domain::{AccountId, MessageId, OutgoingAttachment};
use mc_domain::model::attachment::Attachment as DomainAttachment;
use mc_domain::model::message::{Envelope as DomainEnvelope, Message};
use mc_domain::value_objects::flag::Flag as DomainFlag;

/// Convert domain Envelope to proto MessageEnvelope.
pub fn domain_envelope_to_proto(envelope: &DomainEnvelope) -> MessageEnvelope {
    MessageEnvelope {
        from: envelope.from.iter().map(domain_address_to_proto).collect(),
        to: envelope.to.iter().map(domain_address_to_proto).collect(),
        cc: envelope.cc.iter().map(domain_address_to_proto).collect(),
        bcc: envelope.bcc.iter().map(domain_address_to_proto).collect(),
        reply_to: envelope
            .reply_to
            .iter()
            .map(domain_address_to_proto)
            .collect(),
        subject: envelope.subject.clone(),
        date: Some(datetime_to_timestamp(&envelope.date)),
    }
}

/// Convert proto MessageEnvelope to domain Envelope.
pub fn proto_envelope_to_domain(envelope: &MessageEnvelope) -> Result<DomainEnvelope, String> {
    let from: Result<Vec<_>, _> = envelope.from.iter().map(proto_email_to_domain).collect();
    let to: Result<Vec<_>, _> = envelope.to.iter().map(proto_email_to_domain).collect();
    let cc: Result<Vec<_>, _> = envelope.cc.iter().map(proto_email_to_domain).collect();
    let bcc: Result<Vec<_>, _> = envelope.bcc.iter().map(proto_email_to_domain).collect();
    let reply_to: Result<Vec<_>, _> = envelope
        .reply_to
        .iter()
        .map(proto_email_to_domain)
        .collect();

    Ok(DomainEnvelope {
        from: from?,
        to: to?,
        cc: cc?,
        bcc: bcc?,
        reply_to: reply_to?,
        subject: envelope.subject.clone(),
        date: envelope
            .date
            .as_ref()
            .map(timestamp_to_datetime)
            .unwrap_or_else(chrono::Utc::now),
    })
}

/// Convert domain Attachment to proto AttachmentInfo.
pub fn domain_attachment_to_proto(attachment: &DomainAttachment) -> AttachmentInfo {
    AttachmentInfo {
        id: attachment.id.to_string(),
        filename: attachment.filename.clone().unwrap_or_default(),
        mime_type: attachment.mime_type.clone(),
        size_bytes: attachment.size_bytes,
        is_inline: attachment.is_inline,
        content_id: attachment.content_id.clone(),
    }
}

/// Convert domain Message to proto MessageSummary.
pub fn domain_message_to_summary(message: &Message) -> MessageSummary {
    MessageSummary {
        id: message.id.to_string(),
        account_id: message.account_id.to_string(),
        mailbox_id: message.mailbox_id.to_string(),
        envelope: Some(domain_envelope_to_proto(&message.envelope)),
        flags: message.flags.iter().map(domain_flag_to_proto).collect(),
        size_bytes: message.size_bytes,
        has_attachments: message.has_attachments,
        received_at: Some(datetime_to_timestamp(&message.received_at)),
    }
}

/// Convert domain Message to proto MessageDetail.
pub fn domain_message_to_detail(message: &Message) -> MessageDetail {
    MessageDetail {
        id: message.id.to_string(),
        account_id: message.account_id.to_string(),
        mailbox_id: message.mailbox_id.to_string(),
        provider_uid: message.provider_uid.map(|u| u.value()),
        message_rfc_id: message.message_rfc_id.clone(),
        in_reply_to: message.in_reply_to.clone(),
        references: message.references.clone(),
        envelope: Some(domain_envelope_to_proto(&message.envelope)),
        flags: message.flags.iter().map(domain_flag_to_proto).collect(),
        size_bytes: message.size_bytes,
        attachments: message
            .attachments
            .iter()
            .map(domain_attachment_to_proto)
            .collect(),
        body_text: message.body_text.clone(),
        body_html: message.body_html.clone(),
        raw_headers: None,
        received_at: Some(datetime_to_timestamp(&message.received_at)),
        internal_date: Some(datetime_to_timestamp(&message.internal_date)),
    }
}

/// Parse proto flags into domain flags (filtering out None).
pub fn proto_flags_to_domain(flags: &[i32]) -> Vec<DomainFlag> {
    flags
        .iter()
        .filter_map(|f| proto_flag_to_domain(*f))
        .collect()
}

fn parse_uuid(field: &str, value: &str) -> Result<uuid::Uuid, String> {
    uuid::Uuid::parse_str(value).map_err(|e| format!("invalid {field}: {e}"))
}

pub fn proto_compose_mode(mode: &str) -> Result<ComposeMode, String> {
    ComposeMode::parse(mode).ok_or_else(|| format!("invalid compose mode: {mode:?}"))
}

/// Convert a proto ComposeInput (send / save draft) to the application one.
pub fn proto_compose_input_to_app(input: proto::ComposeInput) -> Result<ComposeInput, String> {
    let message_id = |field, id: Option<String>| -> Result<Option<MessageId>, String> {
        id.map(|id| parse_uuid(field, &id).map(MessageId::from_uuid)).transpose()
    };
    Ok(ComposeInput {
        draft_id: message_id("draft_id", input.draft_id)?,
        account_id: AccountId::from_uuid(parse_uuid("account_id", &input.account_id)?),
        to: input.to,
        cc: input.cc,
        bcc: input.bcc,
        subject: input.subject,
        body_md: input.body_md,
        mode: proto_compose_mode(&input.mode)?,
        source_message_id: message_id("source_message_id", input.source_message_id)?,
        attachments: input
            .attachments
            .into_iter()
            .map(|a| OutgoingAttachment {
                filename: a.filename,
                mime_type: a.mime_type,
                content_id: a.content_id,
                data: a.data,
            })
            .collect(),
        forwarded_attachment_ids: input.forwarded_attachment_ids,
    })
}

pub fn compose_view_to_proto(view: ComposeView) -> proto::ComposeView {
    proto::ComposeView {
        draft_id: view.draft_id.map(|id| id.to_string()),
        account_id: view.account_id.to_string(),
        identities: view
            .identities
            .into_iter()
            .map(|i| proto::ComposeIdentity {
                account_id: i.account_id.to_string(),
                display_name: i.display_name,
                email: i.email,
                signature_html: i.signature_html,
            })
            .collect(),
        to: view.to,
        cc: view.cc,
        bcc: view.bcc,
        subject: view.subject,
        body_md: view.body_md,
        quoted_html: view.quoted_html,
        signature_html: view.signature_html,
        forwarded_attachments: view
            .forwarded_attachments
            .into_iter()
            .map(|a| proto::ForwardedAttachment {
                id: a.id,
                filename: a.filename,
                size: a.size,
                mime_type: a.mime_type,
            })
            .collect(),
        inline_images: view
            .inline_images
            .into_iter()
            .filter_map(|a| {
                Some(proto::InlineImage { content_id: a.content_id?, mime_type: a.mime_type, data: a.data })
            })
            .collect(),
        mode: view.mode.as_str().to_owned(),
        source_message_id: view.source_message_id.map(|id| id.to_string()),
    }
}
