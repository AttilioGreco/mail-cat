use super::*;
use mc_domain::MailboxModel;

struct Setup {
    msg: Message,
    inbox: mc_domain::Mailbox,
    work: mc_domain::Mailbox,
    archive: mc_domain::Mailbox,
    es: Arc<MockEventStore>,
    provider: Arc<MockEmailProvider>,
    handler: MessageCommandHandler<
        MockEventStore,
        MockProviderRegistry,
        MockMessageReadRepo,
        MockEventBus,
    >,
    account: Account,
}

fn setup(model: MailboxModel) -> Setup {
    let msg = make_test_message();
    let inbox = inbox_of(&msg);
    let work = test_mailbox(msg.account_id, "work", MailboxType::Custom);
    let archive = test_mailbox(msg.account_id, "archive", MailboxType::Archive);
    let es = Arc::new(MockEventStore::with_message(&msg));
    let provider = Arc::new(MockEmailProvider::moving_to(900));
    let repo =
        MockMessageReadRepo::with_mailboxes(vec![inbox.clone(), work.clone(), archive.clone()]);
    let handler = MessageCommandHandler::new(
        es.clone(),
        Arc::new(MockProviderRegistry::new(provider.clone())),
        Arc::new(repo),
        Arc::new(MockEventBus::new()),
    );
    let mut account = test_account("a@example.com", "imap.example.com");
    account.id = msg.account_id;
    account.set_mailbox_model(model);
    Setup {
        msg,
        inbox,
        work,
        archive,
        es,
        provider,
        handler,
        account,
    }
}

#[tokio::test]
async fn adding_a_label_keeps_one_message_in_two_mailboxes() {
    let s = setup(MailboxModel::Labels);
    s.handler
        .handle_change_labels(vec![s.msg.id], vec![s.work.id], vec![])
        .await
        .unwrap();

    let calls = s.provider.relabelled.read().unwrap().clone();
    assert_eq!(
        calls[0],
        (vec![RemoteId::from(42)], vec![s.work.path.clone()], vec![])
    );
    let (after, _) = s.handler.load_message(&s.msg.id).await.unwrap();
    assert_eq!(after.memberships.len(), 2);
    assert_eq!(
        after.membership(s.work.id).unwrap().remote_id,
        Some(RemoteId::from(42)),
        "an email keeps its id in the labels it gains"
    );
}

#[tokio::test]
async fn the_last_label_cannot_be_removed() {
    let s = setup(MailboxModel::Labels);
    let err = s
        .handler
        .handle_change_labels(vec![s.msg.id], vec![], vec![s.inbox.id])
        .await
        .unwrap_err();

    assert!(
        matches!(err, ApplicationError::PreconditionFailed(_)),
        "{err}"
    );
    assert!(s.provider.relabelled.read().unwrap().is_empty());
    // Swapping is fine: the message ends up in one.
    s.handler
        .handle_change_labels(vec![s.msg.id], vec![s.work.id], vec![s.inbox.id])
        .await
        .unwrap();
    let (after, _) = s.handler.load_message(&s.msg.id).await.unwrap();
    assert_eq!(after.memberships.len(), 1);
    assert!(after.membership(s.work.id).is_some());
}

#[tokio::test]
async fn archive_drops_only_the_inbox_label_under_labels() {
    let s = setup(MailboxModel::Labels);
    s.handler
        .handle_change_labels(vec![s.msg.id], vec![s.work.id], vec![])
        .await
        .unwrap();
    s.handler
        .handle_archive(std::slice::from_ref(&s.account), vec![s.msg.id])
        .await
        .unwrap();

    let calls = s.provider.relabelled.read().unwrap().clone();
    assert_eq!(calls[1].1, vec![], "nothing added: it still has a label");
    assert_eq!(calls[1].2, vec![s.inbox.path.clone()]);
    let (after, _) = s.handler.load_message(&s.msg.id).await.unwrap();
    assert!(after.membership(s.inbox.id).is_none() && after.membership(s.work.id).is_some());
    assert!(s.provider.moved.read().unwrap().is_empty());
}

#[tokio::test]
async fn archive_of_a_lone_inbox_message_files_it_in_archive() {
    let s = setup(MailboxModel::Labels);
    let result = s
        .handler
        .handle_archive(std::slice::from_ref(&s.account), vec![s.msg.id])
        .await
        .unwrap();

    assert!(matches!(
        result,
        CommandResult::MessagesBulkChanged {
            succeeded: 1,
            failed: 0
        }
    ));
    let calls = s.provider.relabelled.read().unwrap().clone();
    assert_eq!(calls[0].1, vec![s.archive.path.clone()]);
    assert_eq!(calls[0].2, vec![s.inbox.path.clone()]);
    let (after, _) = s.handler.load_message(&s.msg.id).await.unwrap();
    assert!(after.membership(s.archive.id).is_some() && after.memberships.len() == 1);
}

#[tokio::test]
async fn archive_without_an_archive_mailbox_leaves_a_lone_message_alone() {
    let mut s = setup(MailboxModel::Labels);
    let repo = MockMessageReadRepo::with_mailboxes(vec![s.inbox.clone()]);
    s.handler = MessageCommandHandler::new(
        s.es.clone(),
        Arc::new(MockProviderRegistry::new(s.provider.clone())),
        Arc::new(repo),
        Arc::new(MockEventBus::new()),
    );
    let result = s
        .handler
        .handle_archive(std::slice::from_ref(&s.account), vec![s.msg.id])
        .await
        .unwrap();

    assert!(matches!(
        result,
        CommandResult::MessagesBulkChanged {
            succeeded: 0,
            failed: 1
        }
    ));
    assert!(s.provider.relabelled.read().unwrap().is_empty());
}

#[tokio::test]
async fn archive_under_folders_is_a_move() {
    let s = setup(MailboxModel::Folders);
    s.handler
        .handle_archive(std::slice::from_ref(&s.account), vec![s.msg.id])
        .await
        .unwrap();

    assert!(s.provider.relabelled.read().unwrap().is_empty());
    assert_eq!(*s.provider.moved.read().unwrap(), [RemoteId::from(42)]);
    let (after, _) = s.handler.load_message(&s.msg.id).await.unwrap();
    assert!(after.membership(s.archive.id).is_some() && after.memberships.len() == 1);
}
