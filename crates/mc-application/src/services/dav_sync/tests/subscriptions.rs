use super::*;
use mc_domain::{Account, AccountWriteRepository, EmailAddress, ProviderType, Subscribed};

const BOB: &str = "/bob/work/";

impl Env {
    /// The account must exist for its config to be edited.
    async fn with_account(self) -> Self {
        let added = DomainEvent::AccountAdded {
            account_id: self.account,
            name: "Alice".into(),
            email: EmailAddress::new("alice@example.com").unwrap(),
            provider_type: ProviderType::Imap,
        };
        let account = Account::rehydrate(std::slice::from_ref(&added)).unwrap();
        self.repo.save(&account, vec![added]).await.unwrap();
        self
    }

    async fn subscribed(&self) -> Vec<String> {
        let account = self.repo.load(&self.account).await.unwrap().unwrap();
        account
            .subscribed()
            .unwrap_or_default()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// A calendar the server lists, synced into the store.
    async fn listed(&self, path: &str, ownership: Ownership) {
        let mut cal = calendar(path, vec![CalendarComponent::Event]);
        cal.ownership = ownership;
        self.fake.add_calendar(cal);
        self.sync().await;
    }
}

fn bob() -> url::Url {
    format!("https://dav.example.com{BOB}").parse().unwrap()
}

#[tokio::test]
async fn a_local_subscription_is_kept_in_the_config_until_it_is_dropped() {
    let env = env().with_account().await;
    env.fake.subscribes_to(Subscribed::Local(vec![bob()]));
    env.handler
        .handle_subscribe_calendar(env.account, " bob@example.com ".into())
        .await
        .unwrap();
    assert_eq!(env.subscribed().await, [bob().to_string()]);

    env.listed(BOB, Ownership::Shared).await;
    env.handler
        .handle_unsubscribe_calendar(env.account, CollectionId::new(BOB))
        .await
        .unwrap();
    assert!(env.subscribed().await.is_empty());
    assert_eq!(env.fake.calls_to("unsubscribe /bob/work/"), 1);
}

#[tokio::test]
async fn a_remote_subscription_leaves_the_config_alone() {
    let env = env().with_account().await;
    env.fake.subscribes_to(Subscribed::Remote);
    env.handler
        .handle_subscribe_calendar(env.account, "bob@example.com".into())
        .await
        .unwrap();
    assert!(env.subscribed().await.is_empty());
}

#[tokio::test]
async fn an_unknown_colleague_is_the_providers_not_found() {
    let env = env().with_account().await;
    let err = env
        .handler
        .handle_subscribe_calendar(env.account, "nobody@example.com".into())
        .await
        .unwrap_err();
    assert!(
        matches!(err, ApplicationError::Domain(DomainError::NotFound(_))),
        "{err}"
    );
}

#[tokio::test]
async fn the_accounts_own_calendar_cannot_be_unsubscribed() {
    let env = env().with_account().await;
    env.listed("/alice/work/", Ownership::Own).await;
    let err = env
        .handler
        .handle_unsubscribe_calendar(env.account, CollectionId::new("/alice/work/"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, ApplicationError::PreconditionFailed(_)),
        "{err}"
    );
    assert_eq!(env.fake.calls_to("unsubscribe"), 0);
}
