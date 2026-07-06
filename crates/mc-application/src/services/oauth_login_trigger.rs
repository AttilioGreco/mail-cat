//! Port for starting an interactive OAuth2 login. The concrete implementation
//! (in `mc-server`) owns the loopback listener, token exchange, keychain
//! write, and config.toml write; this crate only knows the shape of the
//! progress stream, so `mc-interfaces`'s gRPC layer can depend on it without
//! depending on `mc-server`.

use mc_domain::OAuthProvider;
use tokio::sync::mpsc;

/// One event in an OAuth login's progress. A `start_login` call yields
/// exactly one `AwaitingBrowser`, followed by exactly one of
/// `Succeeded`/`Failed`.
pub enum OAuthLoginProgress {
    AwaitingBrowser { authorize_url: String },
    Succeeded { email: String },
    Failed { message: String },
}

pub trait OAuthLoginTrigger: Send + Sync {
    /// Starts the login in the background; the caller drives it to
    /// completion by reading from the returned channel.
    fn start_login(
        &self,
        name: String,
        email: String,
        provider: OAuthProvider,
    ) -> mpsc::Receiver<OAuthLoginProgress>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeTrigger;

    impl OAuthLoginTrigger for FakeTrigger {
        fn start_login(&self, _name: String, email: String, _provider: OAuthProvider) -> mpsc::Receiver<OAuthLoginProgress> {
            let (tx, rx) = mpsc::channel(2);
            tokio::spawn(async move {
                let _ = tx.send(OAuthLoginProgress::AwaitingBrowser { authorize_url: "https://example.com".into() }).await;
                let _ = tx.send(OAuthLoginProgress::Succeeded { email }).await;
            });
            rx
        }
    }

    #[tokio::test]
    async fn trigger_yields_awaiting_then_succeeded() {
        let trigger = FakeTrigger;
        let mut rx = trigger.start_login("Work".into(), "me@gmail.com".into(), OAuthProvider::Google);

        match rx.recv().await {
            Some(OAuthLoginProgress::AwaitingBrowser { authorize_url }) => {
                assert_eq!(authorize_url, "https://example.com");
            }
            other => panic!("expected AwaitingBrowser, got {}", matches_desc(&other)),
        }
        match rx.recv().await {
            Some(OAuthLoginProgress::Succeeded { email }) => assert_eq!(email, "me@gmail.com"),
            other => panic!("expected Succeeded, got {}", matches_desc(&other)),
        }
    }

    fn matches_desc(event: &Option<OAuthLoginProgress>) -> &'static str {
        match event {
            Some(OAuthLoginProgress::AwaitingBrowser { .. }) => "AwaitingBrowser",
            Some(OAuthLoginProgress::Succeeded { .. }) => "Succeeded",
            Some(OAuthLoginProgress::Failed { .. }) => "Failed",
            None => "None",
        }
    }
}
