/// Proto → domain type conversions, kept in one place.
use chrono::{DateTime, TimeZone, Utc};

use crate::proto::mail_cat::v1 as pb;
use crate::types::*;

pub fn account_from_proto(a: pb::AccountSummary) -> Account {
    Account {
        id: a.id,
        name: a.name,
        email: a.email.map(|e| e.email).unwrap_or_default(),
        is_active: a.status == pb::AccountStatus::Active as i32,
    }
}

pub fn mailbox_from_proto(m: pb::MailboxSummary) -> Mailbox {
    Mailbox {
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
        date,
        is_read: m.flags.contains(&(pb::Flag::Seen as i32)),
        has_attachments: m.has_attachments,
    }
}

pub fn message_body_from_proto(m: pb::MessageDetail) -> MessageBody {
    MessageBody {
        id: m.id,
        html_body: m.body_html,
        text_body: m.body_text,
    }
}

pub fn sync_progress_from_proto(e: pb::SyncProgressEvent) -> SyncProgress {
    use crate::types::SyncPhase;

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
    }
}

fn proto_ts_to_datetime(ts: Option<pb::Timestamp>) -> DateTime<Utc> {
    ts.and_then(|t| Utc.timestamp_opt(t.seconds, t.nanos as u32).single())
        .unwrap_or_else(Utc::now)
}
