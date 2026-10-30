use std::borrow::Cow;

use async_imap::types::NameAttribute;
use mailparse::{MailParseError, parse_mail};

use mc_domain::error::DomainError;
use mc_domain::{Attachment, AttachmentId, Flag, MailboxPath, MailboxType};

/// Convert MailParseError to DomainError.
#[allow(dead_code)] // Day 9: Will be used in IMAP integration
pub fn mailparse_err(e: MailParseError) -> DomainError {
    DomainError::Provider(format!("Failed to parse email: {}", e))
}

/// Convert IMAP mailbox name to MailboxPath.
#[allow(dead_code)] // Day 9: Will be used in IMAP integration
pub fn parse_mailbox_path(name: &str, delimiter: char) -> Result<MailboxPath, DomainError> {
    let segments: Vec<String> = name.split(delimiter).map(|s| s.to_string()).collect();
    MailboxPath::new(segments, delimiter)
}

/// A mailbox name as people read it: IMAP sends non-ASCII names in modified
/// UTF-7 (RFC 3501 5.1.3), `&` opening a base64 run of UTF-16 that `-` closes.
/// Anything malformed stays as the server sent it.
pub fn decode_utf7(name: &str) -> Cow<'_, str> {
    if !name.contains('&') {
        return Cow::Borrowed(name);
    }
    let mut out = String::with_capacity(name.len());
    let mut rest = name;
    while let Some(start) = rest.find('&') {
        out.push_str(&rest[..start]);
        let tail = &rest[start + 1..];
        let Some(end) = tail.find('-') else {
            // Unterminated: the rest is not ours to decode.
            rest = &rest[start..];
            break;
        };
        match &tail[..end] {
            "" => out.push('&'),
            run => match decode_utf7_run(run) {
                Some(text) => out.push_str(&text),
                None => out.push_str(&rest[start..start + end + 2]),
            },
        }
        rest = &tail[end + 1..];
    }
    out.push_str(rest);
    Cow::Owned(out)
}

/// The base64 (alphabet with `,` for `/`) of UTF-16BE inside one `&…-` run.
fn decode_utf7_run(run: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(run.len() * 3 / 4);
    let (mut bits, mut width) = (0u32, 0u32);
    for c in run.bytes() {
        let value = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b',' => 63,
            _ => return None,
        };
        bits = bits << 6 | u32::from(value);
        width += 6;
        if width >= 8 {
            width -= 8;
            bytes.push((bits >> width) as u8);
            bits &= (1 << width) - 1;
        }
    }
    if bytes.len() % 2 != 0 || bits != 0 {
        return None;
    }
    let text = char::decode_utf16(bytes.chunks(2).map(|p| u16::from_be_bytes([p[0], p[1]])))
        .collect::<Result<String, _>>()
        .ok()?;
    // An encoder never emits an empty run, controls or printable ASCII (those
    // go literal), so such a run ("Q&A-x", "&AAAAAA-") is a name, not encoding.
    let canonical = text
        .chars()
        .all(|c| !c.is_control() && !c.is_ascii_graphic() && c != ' ');
    (!text.is_empty() && canonical).then_some(text)
}

/// Convert MailboxPath to IMAP mailbox name string.
#[allow(dead_code)] // Day 9: Will be used in IMAP integration
pub fn mailbox_path_to_string(path: &MailboxPath) -> String {
    path.segments().join(&path.delimiter().to_string())
}

/// Classify a mailbox from its `LIST` response.
///
/// Prefers the RFC 6154 SPECIAL-USE attributes the server advertises, because the
/// name heuristic below is English-only and silently misclassifies every localized
/// account: "Archivio", "Cestino" and "Posta inviata" match none of these stems, and
/// Gmail has no folder named "Archive" at all (its archive is `\All`). Getting this
/// wrong breaks archive, delete-to-Trash and the Sent folder alike, so the attributes
/// win whenever the server bothers to send them.
pub fn classify_mailbox(name: &str, attributes: &[NameAttribute<'_>]) -> MailboxType {
    special_use(name, attributes).unwrap_or_else(|| infer_mailbox_type(name))
}

/// Gmail labels are user-named, so a name says nothing about the role: only
/// the attributes and the reserved INBOX do ("Presentations" is not Sent).
pub fn classify_gmail_mailbox(name: &str, attributes: &[NameAttribute<'_>]) -> MailboxType {
    special_use(name, attributes).unwrap_or(MailboxType::Custom)
}

fn special_use(name: &str, attributes: &[NameAttribute<'_>]) -> Option<MailboxType> {
    for attr in attributes {
        let special = match attr {
            NameAttribute::Sent => MailboxType::Sent,
            NameAttribute::Drafts => MailboxType::Drafts,
            NameAttribute::Trash => MailboxType::Trash,
            NameAttribute::Junk => MailboxType::Spam,
            NameAttribute::Archive => MailboxType::Archive,
            NameAttribute::All => MailboxType::All,
            // Gmail's own attribute, not RFC 6154.
            NameAttribute::Extension(name) if name.eq_ignore_ascii_case("\\Important") => {
                MailboxType::Important
            }
            _ => continue,
        };
        return Some(special);
    }
    // INBOX is not a special-use attribute — it is a reserved name (RFC 3501 §5.1).
    name.eq_ignore_ascii_case("inbox")
        .then_some(MailboxType::Inbox)
}

/// Infer mailbox type from name. Fallback for servers that don't advertise SPECIAL-USE.
///
/// Stems are matched against the lowercased name, so they must be the shortest form
/// shared by the languages we care about ("archiv" covers Archive/Archivio/Archiv).
pub fn infer_mailbox_type(name: &str) -> MailboxType {
    let name_lower = name.to_lowercase();
    let has = |stems: &[&str]| stems.iter().any(|s| name_lower.contains(s));

    if has(&["inbox", "posta in arrivo"]) {
        MailboxType::Inbox
    } else if has(&["sent", "inviat", "gesendet", "envoyé", "enviado"]) {
        MailboxType::Sent
    } else if has(&["draft", "bozze", "entwür", "brouillon", "borrador"]) {
        MailboxType::Drafts
    } else if has(&[
        "trash",
        "deleted",
        "cestino",
        "papierkorb",
        "corbeille",
        "papelera",
    ]) {
        MailboxType::Trash
    } else if has(&["spam", "junk", "indesiderat", "pourriel"]) {
        MailboxType::Spam
    } else if has(&["archiv", "archief"]) {
        MailboxType::Archive
    } else {
        MailboxType::Custom
    }
}

/// Parse message body to extract text, HTML, and attachments.
///
/// Day 8: Core parsing logic, will be called by IMAP fetch handler in Day 9.
#[allow(dead_code)] // Day 9: Will be used in IMAP integration
#[allow(clippy::type_complexity)] // Tuple return is clearer than struct for this case
pub fn parse_message_body(
    body_bytes: &[u8],
) -> Result<(Option<String>, Option<String>, Vec<Attachment>), DomainError> {
    let parsed = parse_mail(body_bytes).map_err(mailparse_err)?;

    let mut text_body = None;
    let mut html_body = None;
    let mut attachments = Vec::new();

    // Extract text and HTML parts
    for part in parsed.subparts.iter().chain(std::iter::once(&parsed)) {
        let content_type = part.ctype.mimetype.to_lowercase();

        if content_type.starts_with("text/plain") && text_body.is_none() {
            if let Ok(body) = part.get_body() {
                text_body = Some(body);
            }
        } else if content_type.starts_with("text/html") && html_body.is_none() {
            if let Ok(body) = part.get_body() {
                html_body = Some(body);
            }
        } else if content_type.starts_with("multipart/") {
            // Recursively handled by mailparse
            continue;
        } else if part.ctype.params.contains_key("name")
            || matches!(
                part.get_content_disposition().disposition,
                mailparse::DispositionType::Attachment
            )
        {
            // This is an attachment
            let content_disp = part.get_content_disposition();
            let filename = part
                .ctype
                .params
                .get("name")
                .or_else(|| content_disp.params.get("filename"))
                .map(|s| s.to_string());

            let mime_type = part.ctype.mimetype.clone();
            let size = part.get_body_raw().map(|b| b.len() as u64).unwrap_or(0);

            // Check if inline
            let is_inline = matches!(
                part.get_content_disposition().disposition,
                mailparse::DispositionType::Inline
            );

            // Get content-id if present
            let content_id = part
                .headers
                .iter()
                .find(|h| h.get_key().to_lowercase() == "content-id")
                .map(|h| h.get_value());

            attachments.push(Attachment {
                id: AttachmentId::new(),
                filename,
                mime_type,
                size_bytes: size,
                content_id,
                is_inline,
            });
        }
    }

    Ok((text_body, html_body, attachments))
}

/// Convert domain Flag to IMAP flag string.
///
/// Day 8: Helper for Day 9 IMAP set_flags implementation.
#[allow(dead_code)] // Day 9: Will be used in IMAP integration
pub fn flag_to_imap(flag: &Flag) -> String {
    match flag {
        Flag::Seen => "\\Seen".to_string(),
        Flag::Answered => "\\Answered".to_string(),
        Flag::Flagged => "\\Flagged".to_string(),
        Flag::Deleted => "\\Deleted".to_string(),
        Flag::Draft => "\\Draft".to_string(),
        Flag::Recent => "\\Recent".to_string(),
        // A keyword, sent as it is: a backslash would make it a system flag.
        Flag::Custom(s) => s.clone(),
    }
}

/// Convert async-imap Flag enum to domain Flag.
pub fn imap_flag_to_domain(flag: &async_imap::types::Flag<'_>) -> Option<Flag> {
    match flag {
        async_imap::types::Flag::Seen => Some(Flag::Seen),
        async_imap::types::Flag::Answered => Some(Flag::Answered),
        async_imap::types::Flag::Flagged => Some(Flag::Flagged),
        async_imap::types::Flag::Deleted => Some(Flag::Deleted),
        async_imap::types::Flag::Draft => Some(Flag::Draft),
        async_imap::types::Flag::Recent => Some(Flag::Recent),
        async_imap::types::Flag::Custom(s) => Some(Flag::Custom(s.to_string())),
        _ => None,
    }
}

/// The flags the application may see, or `None` for a message that is gone.
///
/// `\Deleted` marks a message for expunge (a move without UIDPLUS leaves one
/// behind): sync treats it as absent. `\Recent` is volatile (present on first
/// fetch, gone once the mailbox is selected), so diffing it would churn flags
/// on every sync.
pub fn visible_flags(mut flags: Vec<Flag>) -> Option<Vec<Flag>> {
    if flags.contains(&Flag::Deleted) {
        return None;
    }
    flags.retain(|f| *f != Flag::Recent);
    Some(flags)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf7_names_decode_and_malformed_ones_stay() {
        assert_eq!(decode_utf7("INBOX"), "INBOX");
        assert_eq!(decode_utf7("Caf&AOk-"), "Café");
        assert_eq!(decode_utf7("A &- B"), "A & B");
        assert_eq!(decode_utf7("Lavoro/&AOA-&AOg-"), "Lavoro/àè");
        assert_eq!(decode_utf7("&ZeVnLIqe-"), "日本語");
        assert_eq!(decode_utf7("Mail &2D3eAA-"), "Mail \u{1F600}");
        assert_eq!(decode_utf7("bad &!!-"), "bad &!!-");
        assert_eq!(decode_utf7("open &AOk"), "open &AOk");
        assert_eq!(decode_utf7("Q&A-x"), "Q&A-x");
        assert_eq!(decode_utf7("&AAAAAA-"), "&AAAAAA-");
        assert_eq!(decode_utf7("&AGEAYg-"), "&AGEAYg-");
    }

    #[test]
    fn deleted_messages_are_gone_and_recent_is_dropped() {
        assert_eq!(visible_flags(vec![Flag::Seen, Flag::Deleted]), None);
        assert_eq!(
            visible_flags(vec![Flag::Recent, Flag::Flagged, Flag::Seen]),
            Some(vec![Flag::Flagged, Flag::Seen])
        );
    }

    #[test]
    fn test_parse_mailbox_path() {
        let path = parse_mailbox_path("INBOX/Sent", '/').unwrap();
        assert_eq!(path.segments(), &["INBOX", "Sent"]);
        assert_eq!(path.delimiter(), '/');
    }

    #[test]
    fn test_mailbox_path_to_string() {
        let path = MailboxPath::new(vec!["INBOX".to_string(), "Sent".to_string()], '/').unwrap();
        assert_eq!(mailbox_path_to_string(&path), "INBOX/Sent");
    }

    #[test]
    fn test_infer_mailbox_type() {
        assert_eq!(infer_mailbox_type("INBOX"), MailboxType::Inbox);
        assert_eq!(infer_mailbox_type("Sent"), MailboxType::Sent);
        assert_eq!(infer_mailbox_type("Drafts"), MailboxType::Drafts);
        assert_eq!(infer_mailbox_type("Trash"), MailboxType::Trash);
        assert_eq!(infer_mailbox_type("Spam"), MailboxType::Spam);
        assert_eq!(infer_mailbox_type("Archive"), MailboxType::Archive);
        assert_eq!(infer_mailbox_type("Custom"), MailboxType::Custom);

        // Localized names — these all fell through to Custom before, which is why
        // "Archivia" from a notification silently did nothing.
        assert_eq!(infer_mailbox_type("Archivio"), MailboxType::Archive);
        assert_eq!(infer_mailbox_type("Archiv"), MailboxType::Archive);
        assert_eq!(infer_mailbox_type("Cestino"), MailboxType::Trash);
        assert_eq!(infer_mailbox_type("Posta inviata"), MailboxType::Sent);
        assert_eq!(infer_mailbox_type("Bozze"), MailboxType::Drafts);
    }

    #[test]
    fn special_use_attributes_beat_the_name_heuristic() {
        // Gmail's Italian All Mail: the name matches no stem, only the \All attribute
        // identifies it — and it is the archive target for Gmail accounts.
        assert_eq!(
            classify_mailbox("[Gmail]/Tutti i messaggi", &[NameAttribute::All]),
            MailboxType::All
        );
        assert_eq!(
            classify_mailbox("[Gmail]/Posta inviata", &[NameAttribute::Sent]),
            MailboxType::Sent
        );
        assert_eq!(
            classify_mailbox(
                "[Gmail]/Importanti",
                &[NameAttribute::Extension("\\Important".into())]
            ),
            MailboxType::Important
        );
        // No attributes → fall back to the name.
        for hijack in [
            "Presentations",
            "Absent",
            "Inbox Zero",
            "Drafts 2024",
            "Spam Reports",
            "Old Trash",
        ] {
            assert_eq!(
                classify_gmail_mailbox(hijack, &[]),
                MailboxType::Custom,
                "{hijack}"
            );
        }
        assert_eq!(classify_gmail_mailbox("INBOX", &[]), MailboxType::Inbox);
        assert_eq!(
            classify_gmail_mailbox("[Gmail]/Tutti i messaggi", &[NameAttribute::All]),
            MailboxType::All
        );
        assert_eq!(
            classify_gmail_mailbox("[Gmail]/Cestino", &[NameAttribute::Trash]),
            MailboxType::Trash
        );
        assert_eq!(classify_mailbox("Archivio", &[]), MailboxType::Archive);
        // INBOX is a reserved name, not a special-use attribute.
        assert_eq!(classify_mailbox("INBOX", &[]), MailboxType::Inbox);
        // Non-special-use attributes must not shadow the name.
        assert_eq!(
            classify_mailbox("Cestino", &[NameAttribute::NoInferiors]),
            MailboxType::Trash
        );
    }

    #[test]
    fn test_flag_to_imap() {
        assert_eq!(flag_to_imap(&Flag::Seen), "\\Seen");
        assert_eq!(flag_to_imap(&Flag::Answered), "\\Answered");
        assert_eq!(flag_to_imap(&Flag::Flagged), "\\Flagged");
        assert_eq!(flag_to_imap(&Flag::Deleted), "\\Deleted");
        assert_eq!(flag_to_imap(&Flag::Draft), "\\Draft");
        assert_eq!(flag_to_imap(&Flag::Recent), "\\Recent");
        assert_eq!(flag_to_imap(&Flag::Custom("$Work".to_string())), "$Work");
        assert_eq!(
            flag_to_imap(&Flag::Custom("\\MyFlag".to_string())),
            "\\MyFlag"
        );
    }

    #[test]
    fn test_parse_message_body_simple_text() {
        let body = b"Content-Type: text/plain\r\n\r\nHello, world!";
        let (text, html, attachments) = parse_message_body(body).unwrap();
        assert!(text.is_some());
        assert_eq!(text.unwrap(), "Hello, world!");
        assert!(html.is_none());
        assert_eq!(attachments.len(), 0);
    }
}
