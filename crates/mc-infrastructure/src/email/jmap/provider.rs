//! `JmapEmailProvider`: the read path of RFC 8621 behind [`EmailProvider`].

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use serde_json::{Value, json};

use mc_domain::error::DomainError;
use mc_domain::traits::login_gate::LoginGate;
use mc_domain::{
    AccountId, EmailProvider, Existing, Flag, Mailbox, MailboxChanges, MailboxCounts, MailboxId,
    MailboxPath, Message, MessageBodyResult, MessageEnvelopeSummary, MessageKey, OutgoingMessage,
    ProviderNotification, RemoteId, Reset, SendOutcome, SyncCursor,
};

use super::client::{JmapClient, MethodError};
use super::mapper;
use crate::email::body_only_message;

/// What the background backfill downloads of each text part, as for IMAP.
const TEXT_PART_CAP: u64 = 256 * 1024;

pub struct JmapEmailProvider {
    client: JmapClient,
}

fn not_implemented<T>(what: &str) -> Result<T, DomainError> {
    Err(DomainError::NotSupported(format!(
        "JMAP {what} is not implemented yet"
    )))
}

/// A JMAP mailbox path is the server id, never split into segments.
fn mailbox_id(path: &MailboxPath) -> &str {
    &path.segments()[0]
}

fn state_of(response: &Value) -> Result<SyncCursor, DomainError> {
    response["state"]
        .as_str()
        .map(SyncCursor::new)
        .ok_or_else(|| DomainError::Provider("JMAP reply has no state".into()))
}

fn ids_of(response: &Value, key: &str) -> Vec<String> {
    response[key]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|id| id.as_str().map(str::to_owned))
        .collect()
}

fn list_of(response: &Value) -> &[Value] {
    response["list"].as_array().map_or(&[], Vec::as_slice)
}

fn counts_of(response: &Value, mailbox: &str) -> Result<MailboxCounts, DomainError> {
    let entry = list_of(response)
        .first()
        .ok_or_else(|| DomainError::MailboxNotFound(mailbox.to_owned()))?;
    let count = |key: &str| entry[key].as_u64().unwrap_or(0) as u32;
    Ok(MailboxCounts {
        total: count("totalEmails"),
        unread: count("unreadEmails"),
    })
}

fn in_mailbox(email: &Value, mailbox: &str) -> bool {
    email["mailboxIds"][mailbox].as_bool() == Some(true)
}

impl JmapEmailProvider {
    pub fn new(
        session_url: &str,
        username: String,
        gate: Arc<dyn LoginGate>,
    ) -> Result<Self, DomainError> {
        Ok(Self {
            client: JmapClient::new(session_url, username, gate)?,
        })
    }

    /// Fetches the session: succeeds only when the server speaks JMAP and
    /// takes the credential.
    pub async fn verify_login(&self) -> Result<(), DomainError> {
        self.client.session().await.map(|_| ())
    }

    async fn max_objects(&self) -> Result<usize, DomainError> {
        Ok(self.client.session().await?.max_objects)
    }

    /// `Email/get` of `ids` in server-sized chunks; ids the server lacks are
    /// absent from the result.
    async fn get_emails(
        &self,
        ids: &[RemoteId],
        properties: &[&str],
        extra: Value,
    ) -> Result<Vec<Value>, DomainError> {
        let mut emails = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(self.max_objects().await?) {
            let mut args = extra.clone();
            args["ids"] = json!(chunk.iter().map(RemoteId::as_str).collect::<Vec<_>>());
            args["properties"] = json!(properties);
            let mut reply = self.client.call(vec![("Email/get", args)]).await?;
            emails.extend(list_of(&reply.remove(0)?).iter().cloned());
        }
        Ok(emails)
    }

    async fn bodies(
        &self,
        ids: &[RemoteId],
        cap: Option<u64>,
    ) -> Result<Vec<MessageBodyResult>, DomainError> {
        let mut extra = json!({"fetchTextBodyValues": true, "fetchHTMLBodyValues": true});
        if let Some(cap) = cap {
            extra["maxBodyValueBytes"] = json!(cap);
        }
        let emails = self
            .get_emails(ids, &["id", "textBody", "htmlBody", "bodyValues"], extra)
            .await?;
        Ok(emails.iter().filter_map(mapper::bodies).collect())
    }

    /// Mailbox state before the query: changes made while it pages are then
    /// found again by the next `Email/changes` instead of falling in a gap.
    async fn snapshot(
        &self,
        mailbox: &str,
        reset: Option<Reset>,
    ) -> Result<MailboxChanges, DomainError> {
        let mut head = self
            .client
            .call(vec![
                ("Email/get", json!({"ids": [], "properties": ["id"]})),
                (
                    "Mailbox/get",
                    json!({"ids": [mailbox], "properties": ["totalEmails", "unreadEmails"]}),
                ),
            ])
            .await?
            .into_iter();
        let cursor = state_of(&head.next().unwrap()?)?;
        let counts = counts_of(&head.next().unwrap()?, mailbox)?;

        let limit = self.max_objects().await?;
        let mut added = Vec::new();
        let mut snapshot = Vec::new();
        loop {
            let mut page = self
                .client
                .call(vec![
                    (
                        "Email/query",
                        json!({
                            "filter": {"inMailbox": mailbox},
                            "sort": [{"property": "receivedAt", "isAscending": false}],
                            "position": added.len(),
                            "limit": limit,
                        }),
                    ),
                    (
                        "Email/get",
                        json!({
                            "#ids": {"resultOf": "c0", "name": "Email/query", "path": "/ids"},
                            "properties": ["id", "keywords"],
                        }),
                    ),
                ])
                .await?
                .into_iter();
            let ids = ids_of(&page.next().unwrap()?, "ids");
            let emails = page.next().unwrap()?;
            let flags: HashMap<&str, Vec<Flag>> = list_of(&emails)
                .iter()
                .filter_map(|e| Some((e["id"].as_str()?, mapper::flags(e))))
                .collect();
            let full_page = ids.len() == limit;
            for id in ids {
                let id_flags = flags.get(id.as_str()).cloned().unwrap_or_default();
                snapshot.push((RemoteId::new(&id), id_flags));
                added.push(id);
            }
            if !full_page {
                break;
            }
        }
        Ok(changes(
            cursor,
            reset,
            added,
            Existing::Snapshot(snapshot),
            counts,
        ))
    }

    async fn delta(
        &self,
        mailbox: &str,
        since: &SyncCursor,
    ) -> Result<Option<MailboxChanges>, DomainError> {
        let max_changes = self.max_objects().await?;
        let mut state = since.as_str().to_owned();
        let (mut created, mut updated, mut destroyed) = (Vec::new(), Vec::new(), Vec::new());
        let mut counts = None;
        loop {
            let mut calls = vec![(
                "Email/changes",
                json!({"sinceState": state, "maxChanges": max_changes}),
            )];
            if counts.is_none() {
                calls.push((
                    "Mailbox/get",
                    json!({"ids": [mailbox], "properties": ["totalEmails", "unreadEmails"]}),
                ));
            }
            let mut reply = self.client.call(calls).await?.into_iter();
            let page = match reply.next().unwrap() {
                Ok(page) => page,
                // Stalwart answers a malformed or unknown state with invalidArguments.
                Err(MethodError { kind, .. })
                    if kind == "cannotCalculateChanges" || kind == "invalidArguments" =>
                {
                    return Ok(None);
                }
                Err(e) => return Err(e.into()),
            };
            if let Some(mailboxes) = reply.next() {
                counts = Some(counts_of(&mailboxes?, mailbox)?);
            }
            created.extend(ids_of(&page, "created"));
            updated.extend(ids_of(&page, "updated"));
            destroyed.extend(ids_of(&page, "destroyed"));
            state = page["newState"].as_str().unwrap_or(&state).to_owned();
            if page["hasMoreChanges"].as_bool() != Some(true) {
                break;
            }
        }

        let created: HashSet<String> = created.into_iter().collect();
        let touched: Vec<RemoteId> = created
            .iter()
            .map(|id| RemoteId::new(id.as_str()))
            .chain(updated.iter().map(|id| RemoteId::new(id.as_str())))
            .collect();
        let emails = self
            .get_emails(
                &touched,
                &["id", "keywords", "mailboxIds", "receivedAt"],
                json!({}),
            )
            .await?;

        let mut arrived = Vec::new();
        let mut changed = Vec::new();
        let mut removed: Vec<RemoteId> = destroyed
            .iter()
            .map(|id| RemoteId::new(id.as_str()))
            .collect();
        for email in &emails {
            let Some(id) = email["id"].as_str() else {
                continue;
            };
            if !in_mailbox(email, mailbox) {
                // Moved out; harmless when this mailbox never held it.
                removed.push(RemoteId::new(id));
            } else if created.contains(id) {
                arrived.push((mapper::date(&email["receivedAt"]), id.to_owned()));
            } else {
                changed.push((RemoteId::new(id), mapper::flags(email)));
            }
        }
        arrived.sort_by(|a, b| b.cmp(a));
        let added = arrived.into_iter().map(|(_, id)| id).collect();
        let counts = counts.expect("the first request asks for the counts");
        Ok(Some(changes(
            SyncCursor::new(state),
            None,
            added,
            Existing::Delta { changed, removed },
            counts,
        )))
    }
}

fn changes(
    cursor: SyncCursor,
    reset: Option<Reset>,
    added: Vec<String>,
    existing: Existing,
    counts: MailboxCounts,
) -> MailboxChanges {
    let keys = added
        .iter()
        .map(|id| (RemoteId::new(id.as_str()), MessageKey::new(id.as_str())))
        .collect();
    MailboxChanges {
        cursor,
        reset,
        added: added.into_iter().map(RemoteId::from).collect(),
        keys,
        existing,
        counts,
    }
}

#[async_trait]
impl EmailProvider for JmapEmailProvider {
    async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
        let mut reply = self
            .client
            .call(vec![(
                "Mailbox/get",
                json!({"ids": null, "properties":
                    ["id", "name", "parentId", "role", "totalEmails", "unreadEmails"]}),
            )])
            .await?;
        let reply = reply.remove(0)?;
        let mailboxes = list_of(&reply);
        let paths = mapper::display_paths(mailboxes);
        mailboxes
            .iter()
            .filter_map(|m| m["id"].as_str().map(|id| (id, m)))
            .map(|(id, m)| {
                let display_path = paths[id].clone();
                let count = |key: &str| m[key].as_u64().unwrap_or(0) as u32;
                Ok(Mailbox {
                    id: MailboxId::new(),
                    account_id: AccountId::new(),
                    path: MailboxPath::new(vec![id.to_owned()], '/')?,
                    display_name: m["name"].as_str().unwrap_or(id).to_owned(),
                    display_path,
                    mailbox_type: mapper::mailbox_type(m["role"].as_str()),
                    flags: vec![],
                    total_messages: count("totalEmails"),
                    unread_messages: count("unreadEmails"),
                })
            })
            .collect()
    }

    async fn mailbox_changes(
        &self,
        mailbox: &MailboxPath,
        since: Option<&SyncCursor>,
    ) -> Result<MailboxChanges, DomainError> {
        let id = mailbox_id(mailbox);
        let Some(since) = since else {
            return self.snapshot(id, None).await;
        };
        match self.delta(id, since).await? {
            Some(changes) => Ok(changes),
            None => self.snapshot(id, Some(Reset::Rescan)).await,
        }
    }

    async fn fetch_envelopes(
        &self,
        _mailbox: &MailboxPath,
        ids: &[RemoteId],
    ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
        let emails = self
            .get_emails(ids, &mapper::ENVELOPE_PROPERTIES, json!({}))
            .await?;
        emails.iter().map(mapper::envelope).collect()
    }

    async fn fetch_message_detail(
        &self,
        _mailbox: &MailboxPath,
        id: &RemoteId,
    ) -> Result<Message, DomainError> {
        let body = self
            .bodies(std::slice::from_ref(id), None)
            .await?
            .pop()
            .ok_or_else(|| DomainError::MessageNotFound(id.to_string()))?;
        Ok(body_only_message(id, body.body_text, body.body_html))
    }

    async fn fetch_bodies_bulk(
        &self,
        _mailbox: &MailboxPath,
        ids: &[RemoteId],
    ) -> Result<Vec<MessageBodyResult>, DomainError> {
        self.bodies(ids, Some(TEXT_PART_CAP)).await
    }

    async fn find_by_message_id(
        &self,
        mailbox: &MailboxPath,
        rfc_id: &str,
    ) -> Result<Vec<RemoteId>, DomainError> {
        let wanted = rfc_id.trim_matches(|c: char| c == '<' || c == '>' || c.is_whitespace());
        if wanted.is_empty() {
            return Ok(vec![]);
        }
        // The server-side `header` filter matches nothing on Stalwart, so the
        // mailbox is scanned for the id instead.
        // ponytail: O(mailbox) per lookup; use the filter again if servers get it right.
        let limit = self.max_objects().await?;
        let mut found = Vec::new();
        for position in (0..).step_by(limit) {
            let mut page = self
                .client
                .call(vec![
                    (
                        "Email/query",
                        json!({
                            "filter": {"inMailbox": mailbox_id(mailbox)},
                            "sort": [{"property": "receivedAt", "isAscending": true}],
                            "position": position,
                            "limit": limit,
                        }),
                    ),
                    (
                        "Email/get",
                        json!({
                            "#ids": {"resultOf": "c0", "name": "Email/query", "path": "/ids"},
                            "properties": ["id", "messageId"],
                        }),
                    ),
                ])
                .await?
                .into_iter();
            let queried = ids_of(&page.next().unwrap()?, "ids").len();
            let emails = page.next().unwrap()?;
            found.extend(
                list_of(&emails)
                    .iter()
                    .filter(|e| {
                        mapper::message_ids(e)
                            .iter()
                            .any(|id| id.trim_matches(['<', '>']) == wanted)
                    })
                    .filter_map(|e| e["id"].as_str().map(RemoteId::new)),
            );
            if queried < limit {
                break;
            }
        }
        Ok(found)
    }

    async fn fetch_raw_message(
        &self,
        _mailbox: &MailboxPath,
        id: &RemoteId,
    ) -> Result<Vec<u8>, DomainError> {
        let emails = self
            .get_emails(std::slice::from_ref(id), &["blobId"], json!({}))
            .await?;
        let blob = emails
            .first()
            .and_then(|e| e["blobId"].as_str())
            .ok_or_else(|| DomainError::MessageNotFound(id.to_string()))?;
        self.client.download(blob).await
    }

    async fn set_flags_bulk(
        &self,
        _mailbox: &MailboxPath,
        _ids: &[RemoteId],
        _flags_to_add: &[Flag],
        _flags_to_remove: &[Flag],
    ) -> Result<(), DomainError> {
        not_implemented("flag changes")
    }

    async fn move_messages(
        &self,
        _ids: &[RemoteId],
        _from: &MailboxPath,
        _to: &MailboxPath,
    ) -> Result<HashMap<RemoteId, Option<RemoteId>>, DomainError> {
        not_implemented("move")
    }

    async fn delete_messages(
        &self,
        _mailbox: &MailboxPath,
        _ids: &[RemoteId],
        _permanently: bool,
    ) -> Result<(), DomainError> {
        not_implemented("delete")
    }

    async fn send_message(
        &self,
        _message: &OutgoingMessage,
        _sent: Option<&MailboxPath>,
    ) -> Result<SendOutcome, DomainError> {
        not_implemented("send")
    }

    async fn save_draft(
        &self,
        _drafts: &MailboxPath,
        _message: &OutgoingMessage,
        _replacing: Option<&RemoteId>,
    ) -> Result<RemoteId, DomainError> {
        not_implemented("drafts")
    }

    fn watch_mailbox(
        &self,
        _mailbox: &MailboxPath,
    ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>> {
        Box::pin(futures::stream::empty())
    }
}
