//! Naming a message's attachments from its BODYSTRUCTURE, with no body download.

use std::collections::HashMap;

use async_imap::imap_proto::types::{BodyContentCommon, BodyContentSinglePart, BodyStructure};
use async_imap::imap_proto::types::{BodyParams, ContentEncoding};
use tracing::{debug, warn};

use mc_domain::error::DomainError;
use mc_domain::{Attachment, AttachmentId};

use super::commands::{exchange, fetch_checked};
use super::fetch::decode_rfc2047_header;
use super::{BODY_FETCH_TIMEOUT, ImapEmailProvider};

/// The parts of a message a reader sees as attachments: flagged so by their
/// disposition, or carrying a file name (inline images). Encapsulated
/// messages count as one and are not descended into.
pub(super) fn attachments(structure: &BodyStructure) -> Vec<Attachment> {
    fn walk(node: &BodyStructure, found: &mut Vec<Attachment>) {
        match node {
            BodyStructure::Multipart { bodies, .. } => {
                bodies.iter().for_each(|body| walk(body, found));
            }
            BodyStructure::Basic { common, other, .. }
            | BodyStructure::Text { common, other, .. }
            | BodyStructure::Message { common, other, .. } => found.extend(leaf(common, other)),
        }
    }
    let mut found = Vec::new();
    walk(structure, &mut found);
    found
}

fn leaf(common: &BodyContentCommon, other: &BodyContentSinglePart) -> Option<Attachment> {
    let disposition = common
        .disposition
        .as_ref()
        .map(|d| d.ty.to_ascii_lowercase());
    let filename = common
        .disposition
        .as_ref()
        .and_then(|d| param(&d.ty, &d.params, "filename"))
        .or_else(|| param("attachment", &common.ty.params, "name"));
    // A text body has neither.
    if disposition.as_deref() != Some("attachment") && filename.is_none() {
        return None;
    }
    // BODYSTRUCTURE counts the encoded octets; base64 inflates by a third.
    let size = match other.transfer_encoding {
        ContentEncoding::Base64 => u64::from(other.octets) / 4 * 3,
        _ => u64::from(other.octets),
    };
    Some(Attachment {
        id: AttachmentId::new(),
        filename,
        mime_type: format!("{}/{}", common.ty.ty, common.ty.subtype).to_ascii_lowercase(),
        size_bytes: size,
        content_id: other.id.as_ref().map(ToString::to_string),
        is_inline: disposition.as_deref() == Some("inline"),
    })
}

/// A parameter's value with RFC 2231 continuations and charsets undone by
/// rebuilding the header for mailparse, then RFC 2047 words, which senders
/// put in file names although they shouldn't.
fn param(disposition: &str, params: &BodyParams, name: &str) -> Option<String> {
    let rebuilt: String = params
        .iter()
        .flatten()
        .map(|(k, v)| format!("; {k}=\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect();
    let header = format!("{disposition}{rebuilt}");
    let value = mailparse::parse_content_disposition(&header)
        .params
        .remove(name)
        .filter(|v| !v.is_empty())?;
    Some(if value.contains("=?") {
        decode_rfc2047_header(value.as_bytes())
    } else {
        value
    })
}

/// The attachments of `uids`, from one BODYSTRUCTURE round trip. UIDs the
/// server answers nothing for are absent.
pub(super) async fn structure_attachments<T>(
    session: &mut async_imap::Session<T>,
    uids: &[u32],
) -> Result<HashMap<u32, Vec<Attachment>>, DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    let uid_set = ImapEmailProvider::compress_uid_ranges(uids);
    let structures = exchange(
        BODY_FETCH_TIMEOUT,
        "Attachment structure fetch",
        fetch_checked(session, &uid_set, "(UID BODYSTRUCTURE)"),
    )
    .await?;
    debug!(
        uids = uids.len(),
        answered = structures.len(),
        "attachment structures"
    );
    // An unsolicited FLAGS update for the same UID carries no structure.
    Ok(structures
        .into_iter()
        .filter_map(|item| Some((item.uid?, item.attachments?)))
        .collect())
}

impl ImapEmailProvider {
    /// [`structure_attachments`] over `uids` in bounded batches. A batch the
    /// parser rejects is halved until the message failing it is alone, and
    /// that one is skipped: it is told apart from the rest, not retried.
    pub(super) async fn attachments_in(
        &self,
        mailbox_name: &str,
        uids: &[u32],
    ) -> Result<HashMap<u32, Vec<Attachment>>, DomainError> {
        let mut found = HashMap::new();
        let mut todo: Vec<&[u32]> = uids.chunks(Self::FETCH_BATCH_SIZE).collect();
        while let Some(set) = todo.pop() {
            let mut session = self.selected(mailbox_name).await?;
            match structure_attachments(&mut session, set).await {
                Ok(named) => {
                    self.release(session);
                    self.record_downloads().await;
                    found.extend(named);
                }
                Err(DomainError::Serialization(e)) => match set {
                    [uid] => warn!(uid, error = %e, "unreadable structure, attachments not named"),
                    _ => {
                        let (head, tail) = set.split_at(set.len() / 2);
                        todo.extend([tail, head]);
                    }
                },
                Err(e) => return Err(e),
            }
        }
        Ok(found)
    }
}
