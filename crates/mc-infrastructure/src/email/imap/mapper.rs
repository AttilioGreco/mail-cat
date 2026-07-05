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

/// Convert MailboxPath to IMAP mailbox name string.
#[allow(dead_code)] // Day 9: Will be used in IMAP integration
pub fn mailbox_path_to_string(path: &MailboxPath) -> String {
    path.segments().join(&path.delimiter().to_string())
}

/// Infer mailbox type from name (heuristic).
#[allow(dead_code)] // Day 9: Will be used in IMAP integration
pub fn infer_mailbox_type(name: &str) -> MailboxType {
    let name_lower = name.to_lowercase();
    if name_lower.contains("inbox") {
        MailboxType::Inbox
    } else if name_lower.contains("sent") {
        MailboxType::Sent
    } else if name_lower.contains("draft") {
        MailboxType::Drafts
    } else if name_lower.contains("trash") || name_lower.contains("deleted") {
        MailboxType::Trash
    } else if name_lower.contains("spam") || name_lower.contains("junk") {
        MailboxType::Spam
    } else if name_lower.contains("archive") {
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
        Flag::Custom(s) => {
            if s.starts_with('\\') {
                s.clone()
            } else {
                format!("\\{}", s)
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

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
    }

    #[test]
    fn test_flag_to_imap() {
        assert_eq!(flag_to_imap(&Flag::Seen), "\\Seen");
        assert_eq!(flag_to_imap(&Flag::Answered), "\\Answered");
        assert_eq!(flag_to_imap(&Flag::Flagged), "\\Flagged");
        assert_eq!(flag_to_imap(&Flag::Deleted), "\\Deleted");
        assert_eq!(flag_to_imap(&Flag::Draft), "\\Draft");
        assert_eq!(flag_to_imap(&Flag::Recent), "\\Recent");
        assert_eq!(
            flag_to_imap(&Flag::Custom("Custom".to_string())),
            "\\Custom"
        );
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
