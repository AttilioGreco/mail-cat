/// Proto → domain type conversions, kept in one place.
use chrono::{DateTime, TimeZone, Utc};

use crate::proto::mail_cat::v1 as pb;
use crate::types::*;

pub fn account_from_proto(a: pb::AccountSummary) -> Account {
    use crate::types::AccountStatus;

    let status = match pb::AccountStatus::try_from(a.status) {
        Ok(pb::AccountStatus::Active) => AccountStatus::Active,
        Ok(pb::AccountStatus::Disabled) => AccountStatus::Disabled,
        Ok(pb::AccountStatus::Error) => AccountStatus::Error,
        Ok(pb::AccountStatus::NeedsReauth) => AccountStatus::NeedsReauth,
        Ok(pb::AccountStatus::CredentialsRejected) => AccountStatus::CredentialsRejected,
        _ => AccountStatus::Unknown,
    };

    Account {
        id: a.id,
        name: a.name,
        email: a.email.map(|e| e.email).unwrap_or_default(),
        status,
    }
}

/// Same shape as [`account_from_proto`], for the detail message returned by
/// AddAccount/GetAccount.
pub fn account_detail_from_proto(a: pb::AccountDetail) -> Account {
    account_from_proto(pb::AccountSummary {
        id: a.id,
        name: a.name,
        email: a.email,
        provider_type: a.provider_type,
        status: a.status,
        created_at: a.created_at,
        updated_at: a.updated_at,
    })
}

pub fn mailbox_from_proto(m: pb::MailboxSummary) -> Mailbox {
    use crate::types::MailboxKind;

    let mailbox_type = match pb::MailboxType::try_from(m.mailbox_type) {
        Ok(pb::MailboxType::Inbox) => MailboxKind::Inbox,
        Ok(pb::MailboxType::Sent) => MailboxKind::Sent,
        Ok(pb::MailboxType::Drafts) => MailboxKind::Drafts,
        Ok(pb::MailboxType::Trash) => MailboxKind::Trash,
        Ok(pb::MailboxType::Spam) => MailboxKind::Spam,
        Ok(pb::MailboxType::Archive) => MailboxKind::Archive,
        Ok(pb::MailboxType::All) => MailboxKind::All,
        _ => MailboxKind::Custom,
    };

    Mailbox {
        mailbox_type,
        id: m.id,
        account_id: m.account_id,
        path: m.path,
        display_name: m.display_name,
        total_messages: m.total_messages,
        unread_messages: m.unread_messages,
    }
}

pub fn message_summary_from_proto(m: pb::MessageSummary) -> MessageSummary {
    let envelope = m.envelope.unwrap_or_default();
    let from = envelope.from.into_iter().next();
    let date = proto_ts_to_datetime(envelope.date);

    MessageSummary {
        id: m.id,
        mailbox_id: m.mailbox_id,
        subject: envelope.subject,
        from_name: from.as_ref().and_then(|f| f.name.clone()),
        from_email: from.map(|f| f.email).unwrap_or_default(),
        to: envelope.to.into_iter().map(|a| a.email).collect(),
        cc: envelope.cc.into_iter().map(|a| a.email).collect(),
        date,
        is_read: m.flags.contains(&(pb::Flag::Seen as i32)),
        is_flagged: m.flags.contains(&(pb::Flag::Flagged as i32)),
        has_attachments: m.has_attachments,
    }
}

pub fn message_body_from_proto(m: pb::MessageDetail) -> MessageBody {
    MessageBody {
        id: m.id,
        html_body: m.body_html,
        text_body: m.body_text,
        rfc_message_id: m.message_rfc_id,
        in_reply_to: m.in_reply_to,
        references: m.references,
    }
}

pub fn sync_progress_from_proto(e: pb::SyncProgressEvent) -> SyncProgress {
    use crate::types::SyncPhase;

    let message = e.message.clone();
    let phase = match pb::SyncPhase::try_from(e.phase).unwrap_or(pb::SyncPhase::Unspecified) {
        pb::SyncPhase::DiscoveringMailboxes => SyncPhase::DiscoveringMailboxes,
        pb::SyncPhase::FetchingEnvelopes => SyncPhase::FetchingEnvelopes,
        pb::SyncPhase::FetchingBodies => SyncPhase::FetchingBodies,
        pb::SyncPhase::Indexing => SyncPhase::Indexing,
        pb::SyncPhase::Complete => SyncPhase::Complete,
        pb::SyncPhase::Error => SyncPhase::Error(e.message.unwrap_or_default()),
        pb::SyncPhase::Unspecified => SyncPhase::Error("unspecified".into()),
    };

    SyncProgress {
        account_id: e.account_id,
        phase,
        mailbox_name: e.mailbox_name,
        processed: e.processed,
        total: e.total,
        message,
    }
}

pub fn mailbox_notification_from_proto(e: pb::MailboxEvent) -> MailboxNotification {
    use pb::mailbox_event::EventType;

    let kind = match e.event_type {
        Some(EventType::CountsChanged(c)) => {
            MailboxNotificationKind::CountsChanged { total: c.total, unseen: c.unseen }
        }
        Some(EventType::NewMessage(m)) => {
            MailboxNotificationKind::NewMessage { provider_uid: m.provider_uid }
        }
        Some(EventType::MessageExpunged(m)) => {
            let uid = m.message_id.parse::<u32>().unwrap_or(0);
            MailboxNotificationKind::MessageExpunged { provider_uid: uid }
        }
        Some(EventType::FlagsChanged(_)) | None => MailboxNotificationKind::FlagsChanged,
    };

    MailboxNotification { mailbox_id: e.mailbox_id, event: kind }
}

pub fn message_flag_to_proto(flag: &MessageFlag) -> i32 {
    match flag {
        MessageFlag::Seen => pb::Flag::Seen as i32,
        MessageFlag::Answered => pb::Flag::Answered as i32,
        MessageFlag::Flagged => pb::Flag::Flagged as i32,
        MessageFlag::Deleted => pb::Flag::Deleted as i32,
        MessageFlag::Draft => pb::Flag::Draft as i32,
    }
}

pub fn search_hit_from_proto(h: pb::SearchHit) -> SearchHit {
    SearchHit {
        message_id: h.message_id,
        score: h.score,
        snippet: h.snippet,
        subject: h.subject,
        from_name: h.from_name,
        from_email: h.from_email,
        date: h.date,
    }
}

pub fn thread_message_from_proto(m: pb::MessageDetail) -> ThreadMessage {
    let envelope = m.envelope.unwrap_or_default();
    let from = envelope.from.into_iter().next();
    let date = proto_ts_to_datetime(envelope.date);
    let is_read = m.flags.contains(&(pb::Flag::Seen as i32));
    let to: Vec<String> = envelope.to.into_iter().map(|a| a.email).collect();
    let cc: Vec<String> = envelope.cc.into_iter().map(|a| a.email).collect();

    ThreadMessage {
        id: m.id,
        rfc_message_id: m.message_rfc_id,
        from_name: from.as_ref().and_then(|f| f.name.clone()),
        from_email: from.map(|f| f.email).unwrap_or_default(),
        to,
        cc,
        subject: envelope.subject,
        date,
        is_read,
        html_body: m.body_html,
        text_body: m.body_text,
        in_reply_to: m.in_reply_to,
        references: m.references,
    }
}

pub fn conversation_summary_from_proto(c: pb::ConversationSummary) -> ConversationSummary {
    let last_message_at = c
        .last_message_at
        .map(|ts| {
            Utc.timestamp_opt(ts.seconds, ts.nanos as u32)
                .single()
                .unwrap_or_default()
        })
        .unwrap_or_default();

    ConversationSummary {
        thread_id: c.thread_id,
        account_id: c.account_id,
        subject: c.subject,
        message_count: c.message_count,
        unread_count: c.unread_count,
        last_message_at,
        latest_sender: c.latest_sender,
        sample_message_id: c.sample_message_id,
    }
}

pub fn suggestion_from_proto(s: pb::Suggestion) -> Suggestion {
    Suggestion {
        field: s.field,
        value: s.value,
        doc_count: s.doc_count,
    }
}

pub fn contact_hit_from_proto(h: pb::ContactSearchHit) -> ContactHit {
    ContactHit {
        id: h.id,
        display_name: h.display_name,
        primary_email: h.primary_email,
        all_emails: h.all_emails,
        organization: h.organization,
        account_scope: if h.account_scope.is_empty() { None } else { Some(h.account_scope) },
        is_cross_account: h.is_cross_account,
    }
}

fn proto_ts_to_datetime(ts: Option<pb::Timestamp>) -> DateTime<Utc> {
    ts.and_then(|t| Utc.timestamp_opt(t.seconds, t.nanos as u32).single())
        .unwrap_or_else(Utc::now)
}

pub fn oauth_login_event_from_proto(event: pb::OAuthLoginEvent) -> crate::types::OAuthLoginEvent {
    use pb::o_auth_login_event::Event;
    match event.event {
        Some(Event::AwaitingBrowser(e)) => {
            crate::types::OAuthLoginEvent::AwaitingBrowser { authorize_url: e.authorize_url }
        }
        Some(Event::Succeeded(e)) => crate::types::OAuthLoginEvent::Succeeded { email: e.email },
        Some(Event::Failed(e)) => crate::types::OAuthLoginEvent::Failed { message: e.message },
        None => crate::types::OAuthLoginEvent::Failed { message: "empty OAuth login event".into() },
    }
}

pub(crate) fn oauth_provider_to_proto(provider: crate::types::OAuthProvider) -> i32 {
    match provider {
        crate::types::OAuthProvider::Google => pb::OAuthProvider::OauthProviderGoogle as i32,
        crate::types::OAuthProvider::Microsoft => pb::OAuthProvider::OauthProviderMicrosoft as i32,
    }
}

pub fn compose_view_from_proto(v: pb::ComposeView) -> ComposeView {
    ComposeView {
        draft_id: v.draft_id,
        account_id: v.account_id,
        identities: v
            .identities
            .into_iter()
            .map(|i| ComposeIdentity {
                account_id: i.account_id,
                display_name: i.display_name,
                email: i.email,
                signature_html: i.signature_html,
            })
            .collect(),
        to: v.to,
        cc: v.cc,
        bcc: v.bcc,
        subject: v.subject,
        body_md: v.body_md,
        quoted_html: v.quoted_html,
        signature_html: v.signature_html,
        forwarded_attachments: v
            .forwarded_attachments
            .into_iter()
            .map(|a| ForwardedAttachment {
                id: a.id,
                filename: a.filename,
                size: a.size,
                mime_type: a.mime_type,
            })
            .collect(),
        inline_images: v
            .inline_images
            .into_iter()
            .map(|i| InlineImage { content_id: i.content_id, mime_type: i.mime_type, data: i.data })
            .collect(),
        mode: v.mode,
        source_message_id: v.source_message_id,
    }
}

pub fn compose_input_to_proto(i: ComposeInput) -> pb::ComposeInput {
    pb::ComposeInput {
        draft_id: i.draft_id,
        account_id: i.account_id,
        to: i.to,
        cc: i.cc,
        bcc: i.bcc,
        subject: i.subject,
        body_md: i.body_md,
        mode: i.mode,
        source_message_id: i.source_message_id,
        attachments: i
            .attachments
            .into_iter()
            .map(|a| pb::ComposeAttachment {
                filename: a.filename,
                mime_type: a.mime_type,
                content_id: a.content_id,
                data: a.data,
            })
            .collect(),
        forwarded_attachment_ids: i.forwarded_attachment_ids,
    }
}

#[cfg(test)]
mod oauth_tests {
    use super::*;

    #[test]
    fn awaiting_browser_event_converts() {
        let proto_event = pb::OAuthLoginEvent {
            event: Some(pb::o_auth_login_event::Event::AwaitingBrowser(pb::AwaitingBrowser {
                authorize_url: "https://accounts.google.com/x".into(),
            })),
        };
        match oauth_login_event_from_proto(proto_event) {
            crate::types::OAuthLoginEvent::AwaitingBrowser { authorize_url } => {
                assert_eq!(authorize_url, "https://accounts.google.com/x");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
