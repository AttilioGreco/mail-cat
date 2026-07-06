//! OS keychain adapter for the [`SecretStore`] port.

mod keyring_store;

pub use keyring_store::KeyringSecretStore;
