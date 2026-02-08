use serde::{Deserialize, Serialize};
use std::fmt;

/// Email message flag, following IMAP conventions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Flag {
    Seen,
    Answered,
    Flagged,
    Deleted,
    Draft,
    Recent,
    Custom(String),
}

impl Flag {
    /// Parse from IMAP flag string (e.g., `\Seen`, `\Flagged`).
    pub fn from_imap(s: &str) -> Self {
        match s.trim_start_matches('\\') {
            "Seen" => Self::Seen,
            "Answered" => Self::Answered,
            "Flagged" => Self::Flagged,
            "Deleted" => Self::Deleted,
            "Draft" => Self::Draft,
            "Recent" => Self::Recent,
            other => Self::Custom(other.to_string()),
        }
    }

    /// Convert to IMAP flag string.
    pub fn to_imap(&self) -> String {
        match self {
            Self::Seen => "\\Seen".into(),
            Self::Answered => "\\Answered".into(),
            Self::Flagged => "\\Flagged".into(),
            Self::Deleted => "\\Deleted".into(),
            Self::Draft => "\\Draft".into(),
            Self::Recent => "\\Recent".into(),
            Self::Custom(s) => s.clone(),
        }
    }

    pub fn is_seen(&self) -> bool {
        matches!(self, Self::Seen)
    }

    pub fn is_flagged(&self) -> bool {
        matches!(self, Self::Flagged)
    }
}

impl fmt::Display for Flag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Seen => write!(f, "Seen"),
            Self::Answered => write!(f, "Answered"),
            Self::Flagged => write!(f, "Flagged"),
            Self::Deleted => write!(f, "Deleted"),
            Self::Draft => write!(f, "Draft"),
            Self::Recent => write!(f, "Recent"),
            Self::Custom(s) => write!(f, "{s}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_imap_standard_flags() {
        assert_eq!(Flag::from_imap("\\Seen"), Flag::Seen);
        assert_eq!(Flag::from_imap("\\Flagged"), Flag::Flagged);
        assert_eq!(Flag::from_imap("\\Answered"), Flag::Answered);
        assert_eq!(Flag::from_imap("\\Deleted"), Flag::Deleted);
        assert_eq!(Flag::from_imap("\\Draft"), Flag::Draft);
        assert_eq!(Flag::from_imap("\\Recent"), Flag::Recent);
    }

    #[test]
    fn from_imap_custom_flag() {
        assert_eq!(
            Flag::from_imap("$Important"),
            Flag::Custom("$Important".into())
        );
    }

    #[test]
    fn to_imap_roundtrip() {
        let flags = vec![
            Flag::Seen,
            Flag::Answered,
            Flag::Flagged,
            Flag::Deleted,
            Flag::Draft,
            Flag::Recent,
        ];
        for flag in flags {
            let imap_str = flag.to_imap();
            let parsed = Flag::from_imap(&imap_str);
            assert_eq!(flag, parsed);
        }
    }

    #[test]
    fn display_trait() {
        assert_eq!(format!("{}", Flag::Seen), "Seen");
        assert_eq!(format!("{}", Flag::Custom("Junk".into())), "Junk");
    }
}
