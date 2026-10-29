//! Write path: `Email/set` patches, `Email/import` for drafts and submission.

use std::collections::HashMap;

use serde_json::{Map, Value, json};
use tracing::warn;

use mc_domain::error::DomainError;
use mc_domain::{Flag, MailboxPath, OutgoingMessage, RemoteId, SendOutcome, SentCopy};

use super::mapper;
use super::provider::{JmapEmailProvider, mailbox_id};
use crate::email::smtp::{format_for_storage, format_for_wire};

/// `"id: type (description)"` for each entry of a `not*` map in a `/set` reply.
fn refusals(reply: &Value, key: &str) -> Vec<String> {
    reply[key]
        .as_object()
        .into_iter()
        .flatten()
        .map(|(id, error)| {
            let kind = error["type"].as_str().unwrap_or("unknown");
            match error["description"].as_str() {
                Some(why) => format!("{id}: {kind} ({why})"),
                None => format!("{id}: {kind}"),
            }
        })
        .collect()
}

fn created_id(reply: &Value, creation_id: &str) -> Result<String, DomainError> {
    reply["created"][creation_id]["id"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| {
            DomainError::Provider(format!(
                "JMAP refused to create the message: {}",
                refusals(reply, "notCreated").join(", ")
            ))
        })
}

impl JmapEmailProvider {
    /// Applies `patch` to every id, or destroys them when there is none, in
    /// server-sized requests. Every chunk is tried; the ids refused are named.
    async fn set_emails(&self, ids: &[RemoteId], patch: Option<&Value>) -> Result<(), DomainError> {
        let mut refused = Vec::new();
        for chunk in ids.chunks(self.client.session().await?.max_set) {
            let args = match patch {
                Some(patch) => {
                    let update: Map<String, Value> = chunk
                        .iter()
                        .map(|id| (id.as_str().to_owned(), patch.clone()))
                        .collect();
                    json!({"update": update})
                }
                None => json!({"destroy": chunk.iter().map(RemoteId::as_str).collect::<Vec<_>>()}),
            };
            let reply = self
                .client
                .call(vec![("Email/set", args)])
                .await?
                .remove(0)?;
            refused.extend(refusals(&reply, "notUpdated"));
            refused.extend(refusals(&reply, "notDestroyed"));
        }
        if refused.is_empty() {
            return Ok(());
        }
        Err(DomainError::Provider(format!(
            "JMAP refused: {}",
            refused.join(", ")
        )))
    }

    pub(super) async fn set_flags(
        &self,
        ids: &[RemoteId],
        add: &[Flag],
        remove: &[Flag],
    ) -> Result<(), DomainError> {
        let mut patch = Map::new();
        for (flags, value) in [(remove, Value::Null), (add, json!(true))] {
            for keyword in flags.iter().filter_map(mapper::keyword_from_flag) {
                patch.insert(format!("keywords/{keyword}"), value.clone());
            }
        }
        if patch.is_empty() || ids.is_empty() {
            return Ok(());
        }
        self.set_emails(ids, Some(&Value::Object(patch))).await
    }

    pub(super) async fn move_emails(
        &self,
        ids: &[RemoteId],
        from: &MailboxPath,
        to: &MailboxPath,
    ) -> Result<HashMap<RemoteId, Option<RemoteId>>, DomainError> {
        let (from, to) = (mailbox_id(from), mailbox_id(to));
        if from != to && !ids.is_empty() {
            let patch =
                json!({format!("mailboxIds/{from}"): null, format!("mailboxIds/{to}"): true});
            self.set_emails(ids, Some(&patch)).await?;
        }
        Ok(ids
            .iter()
            .map(|id| (id.clone(), Some(id.clone())))
            .collect())
    }

    pub(super) async fn delete_emails(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
        permanently: bool,
    ) -> Result<(), DomainError> {
        if permanently {
            return self.set_emails(ids, None).await;
        }
        let trash = self.role_mailbox("trash").await?.ok_or_else(|| {
            DomainError::NotSupported("this JMAP account has no Trash mailbox".into())
        })?;
        if trash == mailbox_id(mailbox) {
            return self.set_emails(ids, None).await;
        }
        let trash = MailboxPath::new(vec![trash], '/')?;
        self.move_emails(ids, mailbox, &trash).await.map(drop)
    }

    pub(super) async fn store_draft(
        &self,
        drafts: &MailboxPath,
        message: &OutgoingMessage,
        replacing: Option<&RemoteId>,
    ) -> Result<RemoteId, DomainError> {
        let blob = self.client.upload(format_for_storage(message)?).await?;
        let mut calls = vec![(
            "Email/import",
            json!({"emails": {"d": draft_object(&blob, mailbox_id(drafts))}}),
        )];
        if let Some(old) = replacing {
            calls.push(("Email/set", json!({"destroy": [old.as_str()]})));
        }
        let mut reply = self.client.call(calls).await?.into_iter();
        let id = created_id(&reply.next().unwrap()?, "d")?;
        match reply.next() {
            Some(Ok(destroyed)) => {
                let refused = refusals(&destroyed, "notDestroyed");
                if !refused.is_empty() {
                    warn!(refused = %refused.join(", "), "new draft saved, but deleting the previous version failed");
                }
            }
            Some(Err(e)) => {
                warn!(e = %DomainError::from(e), "new draft saved, but deleting the previous version failed")
            }
            None => {}
        }
        Ok(RemoteId::new(id))
    }

    pub(super) async fn submit(
        &self,
        message: &OutgoingMessage,
        sent: Option<&MailboxPath>,
    ) -> Result<SendOutcome, DomainError> {
        let from = message.from.email.as_str();
        let identity = self.identity_for(from).await?;
        let drafts = self.role_mailbox("drafts").await?.ok_or_else(|| {
            DomainError::NotSupported("this JMAP account has no Drafts mailbox".into())
        })?;
        let blob = self.client.upload(format_for_wire(message)?).await?;
        let recipients: Vec<Value> = message
            .to
            .iter()
            .chain(&message.cc)
            .chain(&message.bcc)
            .map(|a| json!({"email": a.email.as_str()}))
            .collect();

        let mut submission = json!({"create": {"s": {
            "identityId": identity,
            "emailId": "#d",
            "envelope": {"mailFrom": {"email": from}, "rcptTo": recipients},
        }}});
        match sent {
            Some(sent) => {
                submission["onSuccessUpdateEmail"] = json!({"#s": {
                    "keywords/$draft": null,
                    format!("mailboxIds/{drafts}"): null,
                    format!("mailboxIds/{}", mailbox_id(sent)): true,
                }});
            }
            None => submission["onSuccessDestroyEmail"] = json!(["#s"]),
        }
        let mut reply = self
            .client
            .call(vec![
                (
                    "Email/import",
                    json!({"emails": {"d": draft_object(&blob, &drafts)}}),
                ),
                ("EmailSubmission/set", submission),
            ])
            .await?
            .into_iter();
        created_id(&reply.next().unwrap()?, "d")?;
        let submitted = reply.next().unwrap()?;
        if submitted["created"]["s"].is_null() {
            return Err(DomainError::Provider(format!(
                "JMAP refused to send: {}",
                refusals(&submitted, "notCreated").join(", ")
            )));
        }
        // Not `Stored`: the submission moves the copy, whose id is the one
        // the Sent sync will find.
        Ok(SendOutcome {
            sent_copy: if sent.is_some() {
                SentCopy::ServerFiled
            } else {
                SentCopy::NotStored
            },
        })
    }

    /// The identity the server lets `address` send as.
    async fn identity_for(&self, address: &str) -> Result<String, DomainError> {
        let reply = self
            .client
            .call(vec![(
                "Identity/get",
                json!({"ids": null, "properties": ["id", "email"]}),
            )])
            .await?
            .remove(0)?;
        reply["list"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|i| {
                i["email"]
                    .as_str()
                    .is_some_and(|e| e.eq_ignore_ascii_case(address))
            })
            .and_then(|i| i["id"].as_str().map(str::to_owned))
            .ok_or_else(|| DomainError::Provider(format!("no JMAP identity sends as {address}")))
    }

    /// The id of the account's mailbox with `role`.
    pub(super) async fn role_mailbox(&self, role: &str) -> Result<Option<String>, DomainError> {
        let reply = self
            .client
            .call(vec![(
                "Mailbox/get",
                json!({"ids": null, "properties": ["id", "role"]}),
            )])
            .await?
            .remove(0)?;
        Ok(reply["list"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|m| m["role"] == role)
            .and_then(|m| m["id"].as_str().map(str::to_owned)))
    }
}

fn draft_object(blob: &str, mailbox: &str) -> Value {
    json!({
        "blobId": blob,
        "mailboxIds": {mailbox: true},
        "keywords": {"$draft": true, "$seen": true},
    })
}
