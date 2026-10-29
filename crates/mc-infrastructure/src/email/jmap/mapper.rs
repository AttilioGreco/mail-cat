//! JMAP JSON to domain types.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::Value;

use mc_domain::error::DomainError;
use mc_domain::{
    Address, EmailAddress, Envelope, Flag, MailboxType, MessageBodyResult, MessageEnvelopeSummary,
    MessageKey, RemoteId,
};

/// Email properties a list view needs; no body values.
pub(super) const ENVELOPE_PROPERTIES: [&str; 15] = [
    "id",
    "messageId",
    "inReplyTo",
    "references",
    "from",
    "to",
    "cc",
    "bcc",
    "replyTo",
    "subject",
    "sentAt",
    "receivedAt",
    "size",
    "keywords",
    "hasAttachment",
];

pub(super) fn mailbox_type(role: Option<&str>) -> MailboxType {
    match role {
        Some("inbox") => MailboxType::Inbox,
        Some("sent") => MailboxType::Sent,
        Some("drafts") => MailboxType::Drafts,
        Some("trash") => MailboxType::Trash,
        Some("junk") => MailboxType::Spam,
        Some("archive") => MailboxType::Archive,
        Some("all") => MailboxType::All,
        Some("important") => MailboxType::Important,
        _ => MailboxType::Custom,
    }
}

pub(super) fn flag_from_keyword(keyword: &str) -> Flag {
    match keyword {
        "$seen" => Flag::Seen,
        "$flagged" => Flag::Flagged,
        "$answered" => Flag::Answered,
        "$draft" => Flag::Draft,
        other => Flag::Custom(other.to_owned()),
    }
}

pub(super) fn keyword_from_flag(flag: &Flag) -> Option<String> {
    match flag {
        Flag::Seen => Some("$seen".into()),
        Flag::Flagged => Some("$flagged".into()),
        Flag::Answered => Some("$answered".into()),
        Flag::Draft => Some("$draft".into()),
        Flag::Custom(name) => Some(name.clone()),
        // IMAP-only markers: JMAP deletes by destroying or moving to Trash.
        Flag::Deleted | Flag::Recent => None,
    }
}

pub(super) fn flags(email: &Value) -> Vec<Flag> {
    email["keywords"]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(_, set)| set.as_bool() == Some(true))
        .map(|(keyword, _)| flag_from_keyword(keyword))
        .collect()
}

/// `Mailbox.name` chained through `parentId`, joined with `/`.
pub(super) fn display_paths(mailboxes: &[Value]) -> HashMap<String, String> {
    let by_id: HashMap<&str, &Value> = mailboxes
        .iter()
        .filter_map(|m| Some((m["id"].as_str()?, m)))
        .collect();
    let path_of = |id: &str| {
        let mut names = Vec::new();
        let mut next = by_id.get(id);
        // A cycle is a server bug; the bound keeps it from hanging the sync.
        while let Some(mailbox) = next.filter(|_| names.len() < 32) {
            names.push(mailbox["name"].as_str().unwrap_or_default());
            next = mailbox["parentId"].as_str().and_then(|p| by_id.get(p));
        }
        names.reverse();
        names.join("/")
    };
    by_id
        .keys()
        .map(|id| ((*id).to_owned(), path_of(id)))
        .collect()
}

fn addresses(list: &Value) -> Vec<Address> {
    list.as_array()
        .into_iter()
        .flatten()
        .filter_map(|a| {
            Some(Address {
                name: a["name"]
                    .as_str()
                    .filter(|n| !n.is_empty())
                    .map(str::to_owned),
                email: EmailAddress::new(a["email"].as_str()?).ok()?,
            })
        })
        .collect()
}

/// JMAP hands Message-IDs without angle brackets; the domain keeps them.
fn rfc_ids(list: &Value) -> Vec<String> {
    list.as_array()
        .into_iter()
        .flatten()
        .filter_map(|id| Some(format!("<{}>", id.as_str()?)))
        .collect()
}

pub(super) fn message_ids(email: &Value) -> Vec<String> {
    rfc_ids(&email["messageId"])
}

pub(super) fn date(value: &Value) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value.as_str()?)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

pub(super) fn envelope(email: &Value) -> Result<MessageEnvelopeSummary, DomainError> {
    let id = email["id"]
        .as_str()
        .ok_or_else(|| DomainError::Provider("JMAP email without id".into()))?;
    let received = date(&email["receivedAt"]).unwrap_or_else(Utc::now);
    let references = rfc_ids(&email["references"]);
    Ok(MessageEnvelopeSummary {
        remote_id: RemoteId::new(id),
        message_rfc_id: rfc_ids(&email["messageId"]).into_iter().next(),
        in_reply_to: rfc_ids(&email["inReplyTo"]).into_iter().next(),
        references,
        envelope: Envelope {
            from: addresses(&email["from"]),
            to: addresses(&email["to"]),
            cc: addresses(&email["cc"]),
            bcc: addresses(&email["bcc"]),
            reply_to: addresses(&email["replyTo"]),
            subject: email["subject"].as_str().unwrap_or_default().to_owned(),
            date: date(&email["sentAt"]).unwrap_or(received),
        },
        flags: flags(email),
        size_bytes: email["size"].as_u64().unwrap_or(0),
        has_attachments: email["hasAttachment"].as_bool().unwrap_or(false),
        internal_date: received,
        key: Some(MessageKey::new(id)),
    })
}

/// Concatenated values of the body parts of `mime` kind in `parts`. The
/// `textBody` list falls back to HTML parts for HTML-only mail, so the type is
/// checked. The flag says a value was cut at the size cap.
fn part_text(email: &Value, parts: &str, mime: &str) -> (Option<String>, bool) {
    let mut text = String::new();
    let mut truncated = false;
    for part in email[parts].as_array().into_iter().flatten() {
        if part["type"].as_str() != Some(mime) {
            continue;
        }
        let Some(value) = part["partId"]
            .as_str()
            .map(|id| &email["bodyValues"][id])
            .filter(|v| v["value"].is_string())
        else {
            continue;
        };
        text.push_str(value["value"].as_str().unwrap_or_default());
        truncated |= value["isTruncated"].as_bool().unwrap_or(false);
    }
    ((!text.is_empty()).then_some(text), truncated)
}

pub(super) fn bodies(email: &Value) -> Option<MessageBodyResult> {
    let (body_text, text_cut) = part_text(email, "textBody", "text/plain");
    let (body_html, html_cut) = part_text(email, "htmlBody", "text/html");
    Some(MessageBodyResult {
        remote_id: RemoteId::new(email["id"].as_str()?),
        body_text,
        body_html,
        truncated: text_cut || html_cut,
    })
}
