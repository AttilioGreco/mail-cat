//! Write path: `Email/set` patches, `Email/import` for drafts and submission.

use std::collections::HashMap;

use serde_json::{Map, Value, json};
use tracing::warn;

use mc_domain::error::DomainError;
use mc_domain::{Flag, MailboxPath, OutgoingMessage, RemoteId, SendOutcome, SentCopy};

use super::mapper;
use super::provider::{JmapEmailProvider, mailbox_id};
use crate::email::smtp::{format_for_storage, format_for_wire};

/// Ids a `not*` map reports as gone: like IMAP, acting on them is no error.
fn missing(reply: &Value, key: &str) -> Vec<String> {
    reply[key]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(_, error)| error["type"] == "notFound")
        .map(|(id, _)| id.clone())
        .collect()
}

/// `"id: type (description)"` for each refusal in a `not*` map of a `/set`
/// reply, vanished ids excepted.
fn refusals(reply: &Value, key: &str) -> Vec<String> {
    reply[key]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(_, error)| error["type"] != "notFound")
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
    /// Returns the ids the server no longer has.
    async fn set_emails(
        &self,
        ids: &[RemoteId],
        patch: Option<&Value>,
    ) -> Result<Vec<String>, DomainError> {
        let (mut refused, mut gone) = (Vec::new(), Vec::new());
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
            for key in ["notUpdated", "notDestroyed"] {
                refused.extend(refusals(&reply, key));
                gone.extend(missing(&reply, key));
            }
        }
        if refused.is_empty() {
            return Ok(gone);
        }
        Err(DomainError::Provider(format!(
            "JMAP refused: {}",
            refused.join(", ")
        )))
    }

    /// Removes the emails from `from` only: destroyed when that was their
    /// last mailbox, like IMAP's per-mailbox expunge.
    async fn expunge(&self, from: &str, ids: &[RemoteId]) -> Result<(), DomainError> {
        let session = self.client.session().await?;
        let mut refused = Vec::new();
        for chunk in ids.chunks(session.max_set.min(session.max_objects)) {
            let known = self
                .client
                .call(vec![(
                    "Email/get",
                    json!({"ids": chunk.iter().map(RemoteId::as_str).collect::<Vec<_>>(), "properties": ["mailboxIds"]}),
                )])
                .await?
                .remove(0)?;
            let (mut update, mut destroy) = (Map::new(), Vec::new());
            for email in known["list"].as_array().into_iter().flatten() {
                let Some(id) = email["id"].as_str() else {
                    continue;
                };
                let elsewhere = email["mailboxIds"]
                    .as_object()
                    .is_some_and(|boxes| boxes.keys().any(|b| b != from));
                if elsewhere {
                    update.insert(id.to_owned(), json!({format!("mailboxIds/{from}"): null}));
                } else {
                    destroy.push(id);
                }
            }
            if update.is_empty() && destroy.is_empty() {
                continue;
            }
            let reply = self
                .client
                .call(vec![(
                    "Email/set",
                    json!({"update": update, "destroy": destroy}),
                )])
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
        self.set_emails(ids, Some(&Value::Object(patch)))
            .await
            .map(drop)
    }

    pub(super) async fn move_emails(
        &self,
        ids: &[RemoteId],
        from: &MailboxPath,
        to: &MailboxPath,
    ) -> Result<HashMap<RemoteId, Option<RemoteId>>, DomainError> {
        let (from, to) = (mailbox_id(from), mailbox_id(to));
        let mut gone = Vec::new();
        if from != to && !ids.is_empty() {
            let patch =
                json!({format!("mailboxIds/{from}"): null, format!("mailboxIds/{to}"): true});
            gone = self.set_emails(ids, Some(&patch)).await?;
        }
        Ok(ids
            .iter()
            .map(|id| {
                let kept = !gone.iter().any(|g| g == id.as_str());
                (id.clone(), kept.then(|| id.clone()))
            })
            .collect())
    }

    pub(super) async fn delete_emails(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
        permanently: bool,
    ) -> Result<(), DomainError> {
        if permanently {
            return self.expunge(mailbox_id(mailbox), ids).await;
        }
        let trash = self.role_mailbox("trash").await?.ok_or_else(|| {
            DomainError::NotSupported("this JMAP account has no Trash mailbox".into())
        })?;
        if trash == mailbox_id(mailbox) {
            return self.expunge(&trash, ids).await;
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
            .call_all(vec![
                (
                    "Email/import",
                    json!({"emails": {"d": draft_object(&blob, &drafts)}}),
                ),
                ("EmailSubmission/set", submission),
            ])
            .await?
            .into_iter();
        let draft = created_id(&reply.next().unwrap().remove(0)?, "d")?;
        let mut answers = reply.next().unwrap().into_iter();
        let submitted = match answers.next().unwrap() {
            Ok(v) if !v["created"]["s"].is_null() => v,
            refused => {
                self.discard_draft(&draft).await;
                return Err(match refused {
                    Ok(v) => DomainError::Provider(format!(
                        "JMAP refused to send: {}",
                        refusals(&v, "notCreated").join(", ")
                    )),
                    Err(e) => e.into(),
                });
            }
        };
        // The follow-up `Email/set` can fail on its own: the mail went out
        // but the draft was neither filed nor destroyed.
        let followup_failed = match answers.next() {
            Some(Ok(v)) => {
                !(refusals(&v, "notUpdated").is_empty() && refusals(&v, "notDestroyed").is_empty())
            }
            Some(Err(_)) => true,
            None => false,
        };
        if followup_failed {
            warn!(submission = %submitted["created"]["s"]["id"], "mail sent, but filing or removing the draft failed");
            self.discard_draft(&draft).await;
        }
        // Not `Stored`: the submission moves the copy, whose id is the one
        // the Sent sync will find.
        Ok(SendOutcome {
            sent_copy: if sent.is_some() && !followup_failed {
                SentCopy::ServerFiled
            } else {
                SentCopy::NotStored
            },
        })
    }

    async fn discard_draft(&self, id: &str) {
        let reply = self
            .client
            .call(vec![("Email/set", json!({"destroy": [id]}))])
            .await
            .and_then(|mut r| Ok(r.remove(0)?));
        if let Err(e) = reply {
            warn!(%e, id, "could not remove the leftover draft");
        }
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
