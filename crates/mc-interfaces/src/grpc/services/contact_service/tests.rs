use super::*;
use crate::grpc::proto::{ContactFields, StringList};
use crate::grpc::services::dav_fakes::{FakeDav, NoQueries, RecordingBus};
use mc_domain::{AddressBook, RemoteBinding, RemoteContact};

const RAW: &str =
    "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:u\r\nFN:Ada\r\nEMAIL:ada@example.com\r\nEND:VCARD\r\n";

fn book(collection: &str) -> AddressBook {
    AddressBook {
        account: AccountId::new(),
        collection: CollectionId::new(collection),
        name: "Contacts".into(),
        description: None,
        read_only: true,
    }
}

fn contact(collection: &str) -> RemoteContact {
    RemoteContact {
        id: ContactId::new(),
        binding: RemoteBinding {
            account: AccountId::new(),
            collection: CollectionId::new(collection),
            href: mc_domain::Href::new(format!("{collection}a.vcf")),
            etag: None,
            remote_modified: None,
        },
        raw: RAW.into(),
        dirty: true,
        push_error: Some("403".into()),
    }
}

fn service(
    dav: FakeDav,
    result: CommandResult,
) -> (
    ContactServiceImpl<NoQueries, RecordingBus>,
    Arc<RecordingBus>,
) {
    let bus = Arc::new(RecordingBus::answering(result));
    let svc = ContactServiceImpl::new(Arc::new(NoQueries), bus.clone(), Arc::new(dav));
    (svc, bus)
}

#[tokio::test]
async fn lists_address_books_and_contacts_by_book() {
    let dav = FakeDav {
        books: vec![book("/one/")],
        contacts: vec![contact("/one/"), contact("/two/")],
        ..Default::default()
    };
    let (svc, _) = service(dav, CommandResult::ContactRemoved);
    let account_id = AccountId::new().to_string();

    let books = svc
        .list_address_books(Request::new(ListAddressBooksRequest {
            account_id: account_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(books.address_books[0].collection, "/one/");
    assert!(books.address_books[0].read_only);

    let listed = svc
        .list_remote_contacts(Request::new(ListRemoteContactsRequest {
            account_id,
            address_book: Some("/one/".into()),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.contacts.len(), 1);
    let c = &listed.contacts[0];
    assert_eq!(c.display_name, "Ada");
    assert_eq!(c.emails, ["ada@example.com"]);
    assert!(c.dirty);
    assert_eq!(c.push_failed.as_deref(), Some("403"));
}

#[tokio::test]
async fn unknown_contact_is_not_found_and_bad_id_is_invalid() {
    let (svc, _) = service(FakeDav::default(), CommandResult::ContactRemoved);
    let missing = svc
        .get_remote_contact(Request::new(GetRemoteContactRequest {
            contact_id: ContactId::new().to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    let bad = svc
        .get_remote_contact(Request::new(GetRemoteContactRequest {
            contact_id: "nope".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(bad.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn create_update_and_delete_dispatch_the_commands() {
    let id = ContactId::new();
    let (svc, bus) = service(
        FakeDav::default(),
        CommandResult::ContactAdded { contact_id: id },
    );
    let account_id = AccountId::new().to_string();

    let created = svc
        .create_remote_contact(Request::new(CreateRemoteContactRequest {
            account_id,
            address_book: "/one/".into(),
            fields: Some(ContactFields {
                display_name: Some("Ada".into()),
                emails: Some(StringList {
                    items: vec!["ada@example.com".into()],
                }),
                ..Default::default()
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(created.contact_id, id.to_string());

    svc.update_remote_contact(Request::new(UpdateRemoteContactRequest {
        contact_id: id.to_string(),
        fields: Some(ContactFields {
            notes: Some(String::new()),
            ..Default::default()
        }),
    }))
    .await
    .unwrap();
    svc.delete_remote_contact(Request::new(DeleteRemoteContactRequest {
        contact_id: id.to_string(),
    }))
    .await
    .unwrap();

    let seen = bus.seen.lock().unwrap();
    assert!(matches!(
        &seen[0],
        Command::AddRemoteContact { book, fields, .. }
            if book.as_str() == "/one/" && fields.display_name.as_deref() == Some("Ada")
    ));
    assert!(matches!(
        &seen[1],
        Command::UpdateRemoteContact { contact_id, fields }
            if *contact_id == id && fields.notes == Some(None)
    ));
    assert!(matches!(
        &seen[2],
        Command::RemoveRemoteContact { contact_id } if *contact_id == id
    ));
}

#[tokio::test]
async fn create_refuses_a_bad_email_before_any_command() {
    let (svc, bus) = service(FakeDav::default(), CommandResult::ContactRemoved);
    let err = svc
        .create_remote_contact(Request::new(CreateRemoteContactRequest {
            account_id: AccountId::new().to_string(),
            address_book: "/one/".into(),
            fields: Some(ContactFields {
                emails: Some(StringList {
                    items: vec!["nope".into()],
                }),
                ..Default::default()
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(bus.seen.lock().unwrap().is_empty());
}
