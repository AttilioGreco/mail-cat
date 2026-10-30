use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::error::DomainError;

/// Represents a mailbox path with hierarchical segments and a delimiter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailboxPath {
    segments: Vec<String>,
    delimiter: char,
}

// Equal by the joined name, not the segmentation: the store reloads an opaque id
// holding the delimiter as one segment, while a LIST reply splits it, and the
// two must still be the same mailbox.
impl PartialEq for MailboxPath {
    fn eq(&self, other: &Self) -> bool {
        self.delimiter == other.delimiter && self.full_path() == other.full_path()
    }
}

impl Eq for MailboxPath {}

impl Hash for MailboxPath {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.full_path().hash(state);
    }
}

impl MailboxPath {
    pub fn new(segments: Vec<String>, delimiter: char) -> Result<Self, DomainError> {
        if segments.is_empty() {
            return Err(DomainError::InvalidMailboxPath(
                "mailbox path cannot be empty".into(),
            ));
        }
        if segments.iter().any(|s| s.is_empty()) {
            return Err(DomainError::InvalidMailboxPath(
                "mailbox path segments cannot be empty".into(),
            ));
        }
        Ok(Self {
            segments,
            delimiter,
        })
    }

    /// Parse a full path string with a given delimiter.
    pub fn parse(path: &str, delimiter: char) -> Result<Self, DomainError> {
        let segments: Vec<String> = path.split(delimiter).map(|s| s.to_string()).collect();
        Self::new(segments, delimiter)
    }

    /// Create the INBOX path.
    pub fn inbox() -> Self {
        Self {
            segments: vec!["INBOX".into()],
            delimiter: '/',
        }
    }

    pub fn full_path(&self) -> String {
        self.segments.join(&self.delimiter.to_string())
    }

    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    pub fn delimiter(&self) -> char {
        self.delimiter
    }

    /// The last segment (leaf name), e.g., "Projects" in "Work/Projects".
    pub fn name(&self) -> &str {
        self.segments.last().unwrap()
    }

    /// The depth of the path (1 for top-level like INBOX).
    pub fn depth(&self) -> usize {
        self.segments.len()
    }

    /// Whether this is a direct child of the given parent path.
    pub fn is_child_of(&self, parent: &MailboxPath) -> bool {
        self.segments.len() == parent.segments.len() + 1
            && self.segments.starts_with(&parent.segments)
    }
}

impl fmt::Display for MailboxPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.full_path())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn equality_ignores_segmentation() {
        let split = MailboxPath::parse("[Gmail]/Bozze", '/').unwrap();
        let opaque = MailboxPath::new(vec!["[Gmail]/Bozze".into()], '/').unwrap();
        assert_eq!(split, opaque);
    }

    use super::*;

    #[test]
    fn inbox() {
        let inbox = MailboxPath::inbox();
        assert_eq!(inbox.full_path(), "INBOX");
        assert_eq!(inbox.name(), "INBOX");
        assert_eq!(inbox.depth(), 1);
    }

    #[test]
    fn parse_hierarchical_path() {
        let path = MailboxPath::parse("Work/Projects/Rust", '/').unwrap();
        assert_eq!(path.segments(), &["Work", "Projects", "Rust"]);
        assert_eq!(path.name(), "Rust");
        assert_eq!(path.depth(), 3);
        assert_eq!(path.full_path(), "Work/Projects/Rust");
    }

    #[test]
    fn parse_with_dot_delimiter() {
        let path = MailboxPath::parse("INBOX.Subfolder.Deep", '.').unwrap();
        assert_eq!(path.delimiter(), '.');
        assert_eq!(path.full_path(), "INBOX.Subfolder.Deep");
    }

    #[test]
    fn empty_path_is_invalid() {
        assert!(MailboxPath::parse("", '/').is_err());
    }

    #[test]
    fn empty_segment_is_invalid() {
        assert!(MailboxPath::new(vec!["Work".into(), "".into()], '/').is_err());
    }

    #[test]
    fn is_child_of() {
        let parent = MailboxPath::parse("Work", '/').unwrap();
        let child = MailboxPath::parse("Work/Projects", '/').unwrap();
        let grandchild = MailboxPath::parse("Work/Projects/Rust", '/').unwrap();

        assert!(child.is_child_of(&parent));
        assert!(!grandchild.is_child_of(&parent));
        assert!(grandchild.is_child_of(&child));
    }

    #[test]
    fn display_trait() {
        let path = MailboxPath::parse("Sent/2024", '/').unwrap();
        assert_eq!(format!("{path}"), "Sent/2024");
    }
}
