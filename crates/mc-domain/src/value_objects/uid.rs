use serde::{Deserialize, Serialize};
use std::fmt;

/// Provider-specific UID (e.g., IMAP UID).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProviderUid(u32);

impl ProviderUid {
    pub fn new(uid: u32) -> Self {
        Self(uid)
    }

    pub fn value(&self) -> u32 {
        self.0
    }
}

impl fmt::Display for ProviderUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for ProviderUid {
    fn from(uid: u32) -> Self {
        Self(uid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creation_and_value() {
        let uid = ProviderUid::new(42);
        assert_eq!(uid.value(), 42);
    }

    #[test]
    fn ordering() {
        let a = ProviderUid::new(1);
        let b = ProviderUid::new(100);
        assert!(a < b);
    }

    #[test]
    fn from_u32() {
        let uid: ProviderUid = 99u32.into();
        assert_eq!(uid.value(), 99);
    }
}
