use super::*;
use crate::commands::{CommandResult, ContactFields};

fn named(name: &str) -> ContactFields {
    ContactFields {
        display_name: Some(name.into()),
        ..ContactFields::default()
    }
}

#[tokio::test]
async fn a_new_contact_is_pushed_at_once_without_a_pass() {
    let env = Env::with_book();
    env.sync().await;
    let fields = ContactFields {
        emails: Some(vec![mc_domain::ContactEmail::primary(
            mc_domain::EmailAddress::new("zed@example.com").unwrap(),
        )]),
        ..named("Zed")
    };
    let CommandResult::ContactAdded { contact_id } = env
        .handler
        .handle_add_contact(env.account, CollectionId::new(BOOK), fields)
        .await
        .unwrap()
    else {
        panic!("expected the contact id");
    };
    env.settled().await;

    let hrefs = env.fake.hrefs();
    assert_eq!(hrefs.len(), 1, "{hrefs:?}");
    let raw = env.fake.raw(&hrefs[0]).unwrap();
    assert!(
        raw.contains("FN:Zed") && raw.contains("zed@example.com"),
        "{raw}"
    );
    assert_eq!(env.contact_at(&hrefs[0]).await, contact_id);
    assert_eq!(
        env.contact(contact_id).await.source,
        mc_domain::ContactSource::CardDav
    );
}

#[tokio::test]
async fn an_edit_keeps_what_the_app_does_not_model() {
    let env = Env::with_book();
    let raw = vcard("a", "Ann", None).replace("END:VCARD", "X-CUSTOM:keep me\r\nEND:VCARD");
    env.fake.write("/book/a.vcf", &raw);
    env.sync().await;
    let a = env.contact_at("/book/a.vcf").await;

    env.handler
        .handle_update_contact(a, named("Annie"))
        .await
        .unwrap();
    env.settled().await;

    let sent = env.fake.raw("/book/a.vcf").unwrap();
    assert!(
        sent.contains("FN:Annie") && sent.contains("X-CUSTOM:keep me"),
        "{sent}"
    );
    assert!(
        sent.contains("REV:"),
        "the edit is dated for the other clients"
    );
}

#[tokio::test]
async fn writes_to_a_read_only_collection_are_refused_before_any_event() {
    let env = Env::with_book();
    env.fake.add_book(AddressBook {
        read_only: true,
        ..book("/shared/")
    });
    env.fake.write("/shared/s.vcf", &vcard("s", "Sam", None));
    env.sync().await;
    let s = env.contact_at("/shared/s.vcf").await;
    let before = env.history(s).await.len();

    let refused = env
        .handler
        .handle_add_contact(env.account, CollectionId::new("/shared/"), named("New"))
        .await;
    assert!(matches!(
        refused,
        Err(ApplicationError::PreconditionFailed(_))
    ));
    let refused = env.handler.handle_update_contact(s, named("Samuel")).await;
    assert!(matches!(
        refused,
        Err(ApplicationError::PreconditionFailed(_))
    ));
    let refused = env.handler.handle_remove_contact(s).await;
    assert!(matches!(
        refused,
        Err(ApplicationError::PreconditionFailed(_))
    ));

    assert_eq!(env.history(s).await.len(), before);
    assert_eq!(env.contact_count().await, 1);
    assert_eq!(env.fake.calls_to("put"), 0);
}

#[tokio::test]
async fn a_create_needs_a_listed_collection() {
    let env = Env::with_book();
    let unknown = env
        .handler
        .handle_add_contact(env.account, CollectionId::new(BOOK), named("Zed"))
        .await;
    assert!(matches!(unknown, Err(ApplicationError::NotFound(..))));
}
