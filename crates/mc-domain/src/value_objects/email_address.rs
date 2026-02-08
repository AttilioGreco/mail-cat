use serde::{Deserialize, Serialize};
use std::fmt;

use crate::error::DomainError;

/// A validated email address, stored in lowercase.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EmailAddress(String);

impl EmailAddress {
    pub fn new(raw: &str) -> Result<Self, DomainError> {
        let raw = raw.trim().to_lowercase();
        if raw.is_empty() {
            return Err(DomainError::InvalidEmailAddress(
                "email address cannot be empty".into(),
            ));
        }
        let parts: Vec<&str> = raw.splitn(2, '@').collect();
        if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
            return Err(DomainError::InvalidEmailAddress(raw));
        }
        if !parts[1].contains('.') {
            return Err(DomainError::InvalidEmailAddress(raw));
        }
        Ok(Self(raw))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn local_part(&self) -> &str {
        self.0.split('@').next().unwrap()
    }

    pub fn domain(&self) -> &str {
        self.0.split('@').nth(1).unwrap()
    }
}

impl fmt::Display for EmailAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for EmailAddress {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_email() {
        let email = EmailAddress::new("User@Example.COM").unwrap();
        assert_eq!(email.as_str(), "user@example.com");
        assert_eq!(email.local_part(), "user");
        assert_eq!(email.domain(), "example.com");
    }

    #[test]
    fn valid_email_with_whitespace() {
        let email = EmailAddress::new("  hello@world.org  ").unwrap();
        assert_eq!(email.as_str(), "hello@world.org");
    }

    #[test]
    fn empty_email_is_invalid() {
        assert!(EmailAddress::new("").is_err());
        assert!(EmailAddress::new("   ").is_err());
    }

    #[test]
    fn missing_at_is_invalid() {
        assert!(EmailAddress::new("nope").is_err());
    }

    #[test]
    fn missing_domain_is_invalid() {
        assert!(EmailAddress::new("user@").is_err());
    }

    #[test]
    fn missing_local_is_invalid() {
        assert!(EmailAddress::new("@example.com").is_err());
    }

    #[test]
    fn domain_without_dot_is_invalid() {
        assert!(EmailAddress::new("user@localhost").is_err());
    }

    #[test]
    fn display_trait() {
        let email = EmailAddress::new("test@mail.com").unwrap();
        assert_eq!(format!("{email}"), "test@mail.com");
    }
}
