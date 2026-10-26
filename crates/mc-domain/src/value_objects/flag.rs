use serde::{Deserialize, Serialize};
use std::fmt;

/// Email message flag.
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
    fn display_trait() {
        assert_eq!(format!("{}", Flag::Seen), "Seen");
        assert_eq!(format!("{}", Flag::Custom("Junk".into())), "Junk");
    }
}
