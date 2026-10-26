//! Envelope and body fetching, and parsing of the fetched headers and MIME structure.

use std::collections::HashMap;

use async_imap::imap_proto::types::{BodyStructure, ContentEncoding};
use tracing::{debug, info, warn};

use mc_domain::error::DomainError;
use mc_domain::traits::email_provider::{FetchRange, MessageBodyResult, MessageEnvelopeSummary};
use mc_domain::value_objects::flag::Flag;
use mc_domain::value_objects::mailbox_path::MailboxPath;
use mc_domain::value_objects::uid::ProviderUid;

use super::super::mapper;
use super::commands::{exchange, fetch_checked};
use super::{BODY_FETCH_TIMEOUT, COMMAND_TIMEOUT, ImapEmailProvider};

impl ImapEmailProvider {
    pub(super) async fn envelopes(
        &self,
        mailbox: &MailboxPath,
        range: FetchRange,
    ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
        debug!("Fetching envelopes from mailbox: {:?}", mailbox);
        if matches!(&range, FetchRange::Uids(uids) if uids.is_empty()) {
            return Ok(Vec::new());
        }
        let mailbox_name = mapper::mailbox_path_to_string(mailbox);
        let mut session = self.selected(&mailbox_name).await?;

        // Use SEARCH to find actual existing UIDs (handles UID gaps from deleted messages)
        let search_criteria = match &range {
            FetchRange::All => "ALL".to_string(),
            FetchRange::UidRange { from, to } => format!("UID {}:{}", from, to),
            FetchRange::Last(_) => "ALL".to_string(),
            FetchRange::Since(date) => format!("SINCE {}", date.format("%d-%b-%Y")),
            FetchRange::Uids(uids) => {
                let uids: Vec<u32> = uids.iter().map(|u| u.value()).collect();
                format!("UID {}", Self::compress_uid_ranges(&uids))
            }
        };

        debug!("Executing UID SEARCH with criteria: {}", search_criteria);
        let search_results = exchange(
            COMMAND_TIMEOUT,
            "Search",
            session.uid_search(&search_criteria),
        )
        .await?;

        if search_results.is_empty() {
            debug!("No messages found matching search criteria");
            self.release(session);
            return Ok(Vec::new());
        }

        // Convert HashSet to sorted Vec
        let mut all_uids: Vec<u32> = search_results.into_iter().collect();
        all_uids.sort_unstable();

        // For Last(n), take only the last n UIDs
        let uids_to_fetch = match range {
            FetchRange::Last(n) => {
                let start = all_uids.len().saturating_sub(n as usize);
                &all_uids[start..]
            }
            _ => &all_uids[..],
        };

        info!(
            "Fetching {} envelopes in batches of {}",
            uids_to_fetch.len(),
            Self::FETCH_BATCH_SIZE
        );

        // Fetch in batches to avoid IMAP command line limits
        let mut all_envelopes = Vec::new();
        for (batch_idx, chunk) in uids_to_fetch.chunks(Self::FETCH_BATCH_SIZE).enumerate() {
            let query = Self::compress_uid_ranges(chunk);
            debug!(
                "Batch {}: fetching {} UIDs (query: {})",
                batch_idx,
                chunk.len(),
                &query[..query.len().min(100)]
            );

            use futures::TryStreamExt;
            let batch_results: Vec<async_imap::types::Fetch> =
                exchange(BODY_FETCH_TIMEOUT, "Fetch envelopes", async {
                    session
                        .uid_fetch(&query, "(ENVELOPE FLAGS INTERNALDATE BODY.PEEK[HEADER])")
                        .await?
                        .try_collect()
                        .await
                })
                .await?;

            debug!(
                "Batch {}: fetched {} envelopes",
                batch_idx,
                batch_results.len()
            );
            // ponytail: the ENVELOPE structure repeats the header, so it costs
            // about as much again; counted as the header bytes twice.
            let header_bytes: usize = batch_results
                .iter()
                .filter_map(|f| f.header())
                .map(<[u8]>::len)
                .sum();
            self.metered(2 * header_bytes as u64).await;

            for fetch_data in &batch_results {
                if let Some(envelope_data) = fetch_data.envelope() {
                    let provider_uid = ProviderUid::new(fetch_data.uid.ok_or_else(|| {
                        DomainError::Provider("Missing UID in fetch response".to_string())
                    })?);

                    let from = Self::parse_imap_addresses(envelope_data.from.as_ref());
                    let to = Self::parse_imap_addresses(envelope_data.to.as_ref());
                    let cc = Self::parse_imap_addresses(envelope_data.cc.as_ref());

                    let subject = envelope_data
                        .subject
                        .as_ref()
                        .map(|s| decode_rfc2047_header(s))
                        .unwrap_or_default();

                    let date = envelope_data
                        .date
                        .as_ref()
                        .and_then(|date_bytes| {
                            let date_str = String::from_utf8_lossy(date_bytes);
                            chrono::DateTime::parse_from_rfc2822(&date_str)
                                .ok()
                                .map(|dt| dt.with_timezone(&chrono::Utc))
                        })
                        .unwrap_or_else(chrono::Utc::now);

                    let envelope = mc_domain::Envelope {
                        from,
                        to,
                        cc,
                        bcc: Vec::new(),
                        reply_to: Vec::new(),
                        subject,
                        date,
                    };

                    // Parse flags from IMAP response
                    let flags: Vec<Flag> = fetch_data
                        .flags()
                        .filter_map(|f| mapper::imap_flag_to_domain(&f))
                        .collect();

                    let internal_date = fetch_data
                        .internal_date()
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .unwrap_or(date);

                    // Parse threading headers from raw RFC header block.
                    let (message_rfc_id, in_reply_to, references) = fetch_data
                        .header()
                        .map(parse_threading_headers)
                        .unwrap_or((None, None, Vec::new()));

                    let summary = MessageEnvelopeSummary {
                        provider_uid,
                        message_rfc_id,
                        in_reply_to,
                        references,
                        envelope,
                        flags,
                        size_bytes: 0,
                        has_attachments: false,
                        internal_date,
                    };

                    all_envelopes.push(summary);
                } else {
                    debug!("No envelope data for UID {:?}", fetch_data.uid);
                }
            }
        }
        self.release(session);

        info!("Fetched {} envelopes total", all_envelopes.len());
        Ok(all_envelopes)
    }
}

/// The text `uids` need — their first non-attachment text/plain and
/// text/html parts, each cut at `cap` when given — never attachments: one
/// BODYSTRUCTURE round trip, then one fetch per distinct part layout. Every
/// UID that came back gets a result (bodiless if it has no text), so it
/// dequeues. Also returns the bytes downloaded.
pub(super) async fn text_bodies<T>(
    session: &mut async_imap::Session<T>,
    uids: &[u32],
    cap: Option<u32>,
) -> Result<(Vec<MessageBodyResult>, u64), DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    let uid_set = ImapEmailProvider::compress_uid_ranges(uids);
    let structures = exchange(
        BODY_FETCH_TIMEOUT,
        "Body structure fetch",
        fetch_checked(session, &uid_set, "(UID BODYSTRUCTURE)"),
    )
    .await?;
    let structure_count = structures.len();

    let parts_by_uid: HashMap<u32, Vec<TextPart>> = structures
        .into_iter()
        // An unsolicited FLAGS update for the same UID carries no structure:
        // skipping it keeps the real one. A UID with none comes back missing.
        .filter_map(|item| Some((item.uid?, item.text_parts?)))
        .collect();
    // Messages laid out alike share one FETCH of the same sections.
    let mut layouts: HashMap<Vec<Vec<u32>>, Vec<u32>> = HashMap::new();
    for (uid, parts) in &parts_by_uid {
        layouts
            .entry(parts.iter().map(|p| p.section.clone()).collect())
            .or_default()
            .push(*uid);
    }
    // ponytail: a BODYSTRUCTURE reply's size isn't exposed; 1 KiB is a typical one.
    let mut bytes = structure_count as u64 * 1024;
    let range = cap.map_or_else(String::new, |n| format!("<0.{n}>"));

    let mut results = Vec::with_capacity(structure_count);
    for (sections, message_uids) in layouts {
        if sections.is_empty() {
            results.extend(message_uids.into_iter().map(|uid| MessageBodyResult {
                provider_uid: ProviderUid::new(uid),
                body_text: None,
                body_html: None,
                truncated: false,
            }));
            continue;
        }
        let wanted: Vec<String> = sections
            .iter()
            .map(|s| {
                format!(
                    "BODY.PEEK[{}]{range}",
                    s.iter().map(u32::to_string).collect::<Vec<_>>().join(".")
                )
            })
            .collect();
        let fetched = exchange(
            BODY_FETCH_TIMEOUT,
            "Body fetch",
            fetch_checked(
                session,
                &ImapEmailProvider::compress_uid_ranges(&message_uids),
                &format!("(UID {})", wanted.join(" ")),
            ),
        )
        .await?;
        for item in &fetched {
            let Some((uid, parts)) = item
                .uid
                .and_then(|uid| Some((uid, parts_by_uid.get(&uid)?)))
            else {
                continue;
            };
            let mut result = MessageBodyResult {
                provider_uid: ProviderUid::new(uid),
                body_text: None,
                body_html: None,
                truncated: false,
            };
            for part in parts {
                let Some(data) = item.section(Some(&part.section)) else {
                    continue;
                };
                bytes += data.len() as u64;
                let cut = cap.is_some_and(|n| part.octets > n);
                result.truncated |= cut;
                let text = part.decode(if cut { whole_lines(data) } else { data });
                if part.html {
                    result.body_html = text;
                } else {
                    result.body_text = text;
                }
            }
            results.push(result);
        }
    }
    Ok((results, bytes))
}

/// A cut part up to its last line break: a base64 line or quoted-printable
/// escape split in two would fail the whole part's decoding.
fn whole_lines(data: &[u8]) -> &[u8] {
    data.iter()
        .rposition(|&b| b == b'\n')
        .map_or(data, |end| &data[..=end])
}

/// The first `cap` bytes of a message, headers included, read like a whole
/// one: the text of a message whose BODYSTRUCTURE can't be read, without
/// ever pulling a multi-MB attachment. Also returns the bytes downloaded.
pub(super) async fn message_start<T>(
    session: &mut async_imap::Session<T>,
    uid: u32,
    cap: u32,
) -> Result<(Option<MessageBodyResult>, u64), DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    let fetched = exchange(
        BODY_FETCH_TIMEOUT,
        "Partial raw fetch",
        fetch_checked(
            session,
            &uid.to_string(),
            &format!("(UID BODY.PEEK[]<0.{cap}>)"),
        ),
    )
    .await?;
    let Some(raw) = fetched.iter().find_map(|f| f.section(None)) else {
        return Ok((None, 0));
    };
    let truncated = raw.len() >= cap as usize;
    let (body_text, body_html) = whole_message_text(if truncated { whole_lines(raw) } else { raw });
    let result = MessageBodyResult {
        provider_uid: ProviderUid::new(uid),
        body_text,
        body_html,
        truncated,
    };
    Ok((Some(result), raw.len() as u64))
}

/// `fetch` over `uids`; a set the parser rejected is split in halves until the
/// message failing it is alone, which gets `fallback`. One unreadable message
/// costs a few retries, not one per message of its set. Any other failure (a
/// refusal, a dropped connection) would hit every message alike: it fails the
/// lot, so the caller backs off instead of hammering the server. Both return
/// an outer error for a connection that couldn't be had and an inner one for
/// the fetch itself.
pub(super) async fn isolating<'a, F, FF, G, GF>(
    uids: &'a [u32],
    mut fetch: F,
    mut fallback: G,
) -> Result<Vec<MessageBodyResult>, DomainError>
where
    F: FnMut(&'a [u32]) -> FF,
    FF: std::future::Future<
            Output = Result<Result<Vec<MessageBodyResult>, DomainError>, DomainError>,
        >,
    G: FnMut(u32) -> GF,
    GF: std::future::Future<
            Output = Result<Result<Option<MessageBodyResult>, DomainError>, DomainError>,
        >,
{
    let mut results = Vec::with_capacity(uids.len());
    let mut todo = vec![uids];
    while let Some(set) = todo.pop() {
        let error = match fetch(set).await? {
            Ok(found) => {
                results.extend(found);
                continue;
            }
            Err(e @ DomainError::Serialization(_)) => e,
            Err(e) => return Err(e),
        };
        let [uid] = set else {
            debug!(uids = set.len(), error = %error, "body fetch failed, splitting the set");
            let (head, tail) = set.split_at(set.len() / 2);
            todo.extend([tail, head]);
            continue;
        };
        warn!(uid, error = %error, "body fetch failed for one message, fetching the start of it");
        match fallback(*uid).await? {
            Ok(found) => results.extend(found),
            Err(e) => return Err(e),
        }
    }
    Ok(results)
}

/// Parse RFC threading headers (Message-ID, In-Reply-To, References) from raw
/// header bytes returned by `BODY.PEEK[HEADER]`.
///
/// Returns `(message_rfc_id, in_reply_to, references)`.
fn parse_threading_headers(bytes: &[u8]) -> (Option<String>, Option<String>, Vec<String>) {
    let text = String::from_utf8_lossy(bytes);
    let mut message_id: Option<String> = None;
    let mut in_reply_to: Option<String> = None;
    let mut references: Vec<String> = Vec::new();

    // Accumulate folded header lines then process each complete header.
    let mut current_name = String::new();
    let mut current_value = String::new();

    let flush = |name: &str,
                 value: &str,
                 mid: &mut Option<String>,
                 irt: &mut Option<String>,
                 refs: &mut Vec<String>| {
        match name.to_lowercase().as_str() {
            "message-id" => *mid = extract_first_msgid(value),
            "in-reply-to" => *irt = extract_first_msgid(value),
            "references" => *refs = extract_all_msgids(value),
            _ => {}
        }
    };

    for line in text.lines() {
        if line.starts_with(' ') || line.starts_with('\t') {
            // Folded continuation of previous header.
            current_value.push(' ');
            current_value.push_str(line.trim());
        } else if let Some(colon) = line.find(':') {
            // New header — flush the previous one first.
            if !current_name.is_empty() {
                flush(
                    &current_name,
                    &current_value,
                    &mut message_id,
                    &mut in_reply_to,
                    &mut references,
                );
            }
            current_name = line[..colon].trim().to_string();
            current_value = line[colon + 1..].trim().to_string();
        }
    }
    // Flush the last header.
    if !current_name.is_empty() {
        flush(
            &current_name,
            &current_value,
            &mut message_id,
            &mut in_reply_to,
            &mut references,
        );
    }

    (message_id, in_reply_to, references)
}

/// Extract the first `<…>` message-ID token from a header value.
fn extract_first_msgid(value: &str) -> Option<String> {
    let start = value.find('<')?;
    let end = value[start..].find('>')? + start;
    Some(value[start..=end].to_string())
}

/// Extract all `<…>` message-ID tokens from a References header value.
fn extract_all_msgids(value: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut rest = value;
    while let Some(start) = rest.find('<') {
        rest = &rest[start..];
        if let Some(end) = rest.find('>') {
            ids.push(rest[..=end].to_string());
            rest = &rest[end + 1..];
        } else {
            break;
        }
    }
    ids
}

/// A text part of a message, downloaded on its own instead of the whole
/// message, and what it takes to decode it.
#[derive(Debug, PartialEq)]
pub(super) struct TextPart {
    /// IMAP part number, `[1, 2]` = `BODY[1.2]`.
    pub(super) section: Vec<u32>,
    pub(super) html: bool,
    /// Its size on the server, encoded.
    octets: u32,
    /// MIME headers rebuilt from BODYSTRUCTURE, so mailparse can undo the
    /// transfer encoding and charset.
    headers: String,
}

impl TextPart {
    pub(super) fn decode(&self, data: &[u8]) -> Option<String> {
        let part = [self.headers.as_bytes(), data].concat();
        mailparse::parse_mail(&part).ok()?.get_body().ok()
    }
}

/// The first text/plain and text/html parts that aren't attachments, in
/// MIME order — what a reader shows of a whole message.
/// Encapsulated messages (forwarded as attachment) are not descended into.
pub(super) fn text_parts(structure: &BodyStructure) -> Vec<TextPart> {
    fn walk(node: &BodyStructure, path: &mut Vec<u32>, found: &mut Vec<TextPart>) {
        match node {
            BodyStructure::Multipart { bodies, .. } => {
                for (n, body) in (1..).zip(bodies) {
                    path.push(n);
                    walk(body, path, found);
                    path.pop();
                }
            }
            BodyStructure::Text { common, other, .. } => {
                let attachment = common
                    .disposition
                    .as_ref()
                    .is_some_and(|d| d.ty.eq_ignore_ascii_case("attachment"));
                let html = match common.ty.subtype.to_ascii_lowercase().as_str() {
                    "plain" => false,
                    "html" => true,
                    _ => return,
                };
                if attachment || found.iter().any(|p| p.html == html) {
                    return;
                }
                let charset = common
                    .ty
                    .params
                    .iter()
                    .flatten()
                    .find(|(k, _)| k.eq_ignore_ascii_case("charset"))
                    .map_or("us-ascii", |(_, v)| v);
                let encoding = match &other.transfer_encoding {
                    ContentEncoding::SevenBit => "7bit",
                    ContentEncoding::EightBit => "8bit",
                    ContentEncoding::Binary => "binary",
                    ContentEncoding::Base64 => "base64",
                    ContentEncoding::QuotedPrintable => "quoted-printable",
                    ContentEncoding::Other(other) => other,
                };
                found.push(TextPart {
                    // A single-part message's body is part 1 (RFC 3501 §6.4.5).
                    section: if path.is_empty() { vec![1] } else { path.clone() },
                    html,
                    octets: other.octets,
                    headers: format!(
                        "Content-Type: text/{}; charset=\"{charset}\"\r\nContent-Transfer-Encoding: {encoding}\r\n\r\n",
                        common.ty.subtype
                    ),
                });
            }
            _ => {}
        }
    }
    let mut found = Vec::new();
    walk(structure, &mut Vec::new(), &mut found);
    found
}

/// First non-attachment text/plain and text/html of a whole message — the
/// fallback when its BODYSTRUCTURE can't be parsed.
pub(super) fn whole_message_text(raw: &[u8]) -> (Option<String>, Option<String>) {
    let Ok(mail) = mailparse::parse_mail(raw) else {
        return (None, None);
    };
    let (mut text, mut html) = (None, None);
    for part in mail.parts().filter(|p| {
        p.subparts.is_empty()
            && p.get_content_disposition().disposition != mailparse::DispositionType::Attachment
    }) {
        let slot = match part.ctype.mimetype.as_str() {
            "text/plain" => &mut text,
            "text/html" => &mut html,
            _ => continue,
        };
        if slot.is_none() {
            *slot = part.get_body().ok();
        }
    }
    (text, html)
}

/// Decode an RFC 2047 encoded header value (e.g. `=?UTF-8?Q?...?=`).
/// Falls back to lossy UTF-8 if parsing fails.
pub(super) fn decode_rfc2047_header(raw: &[u8]) -> String {
    // mailparse::parse_header expects a full "Name: value\r\n" line
    let header_line = [b"Subject: ", raw, b"\r\n"].concat();
    match mailparse::parse_header(&header_line) {
        Ok((hdr, _)) => hdr.get_value(),
        Err(_) => String::from_utf8_lossy(raw).to_string(),
    }
}
