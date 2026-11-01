use super::*;

struct Deleting {
    handler: MessageCommandHandler<
        MockEventStore,
        MockProviderRegistry,
        MockMessageReadRepo,
        MockEventBus,
    >,
    provider: Arc<MockEmailProvider>,
    repo: Arc<MockMessageReadRepo>,
    bus: Arc<MockEventBus>,
    junk: mc_domain::Mailbox,
    sent: mc_domain::Mailbox,
}

/// A role-less `Sent` holding `count` messages, next to the real `Sent Items`.
fn deleting(count: usize, provider: MockEmailProvider) -> Deleting {
    deleting_with(count, provider, AccountId::new(), vec![])
}

fn deleting_with(
    count: usize,
    provider: MockEmailProvider,
    account: AccountId,
    more: Vec<mc_domain::Mailbox>,
) -> Deleting {
    let junk = test_mailbox(account, "Sent", MailboxType::Custom);
    let sent = test_mailbox(account, "Sent Items", MailboxType::Sent);
    let es = Arc::new(MockEventStore::new());
    let mut listing = Vec::new();
    for uid in 0..count {
        let (message, _) = Message::receive(
            account,
            junk.id,
            RemoteId::from(uid as u32 + 1),
            None,
            None,
            vec![],
            test_envelope(),
            vec![],
            10,
            false,
            chrono::Utc::now(),
        );
        es.add(&message);
        listing.push(mc_domain::MessageSummary {
            id: message.id,
            account_id: account,
            memberships: message.memberships.clone(),
            from_name: None,
            from_email: "a@example.com".into(),
            to: vec![],
            cc: vec![],
            subject: String::new(),
            flags: vec![],
            size_bytes: 10,
            has_attachments: false,
            received_at: message.received_at,
        });
    }
    let repo = Arc::new(MockMessageReadRepo {
        mailboxes: vec![
            junk.clone(),
            sent.clone(),
            test_mailbox(account, "INBOX", MailboxType::Inbox),
        ]
        .into_iter()
        .chain(more)
        .collect(),
        listing: std::sync::Mutex::new(listing),
        ..MockMessageReadRepo::new()
    });
    let provider = Arc::new(provider);
    let bus = Arc::new(MockEventBus::new());
    let handler = MessageCommandHandler::new(
        es,
        Arc::new(MockProviderRegistry::new(provider.clone())),
        repo.clone(),
        bus.clone(),
    );
    Deleting {
        handler,
        provider,
        repo,
        bus,
        junk,
        sent,
    }
}

fn refused(result: Result<CommandResult, ApplicationError>) -> String {
    match result {
        Err(ApplicationError::PreconditionFailed(why)) => why,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn a_system_mailbox_is_never_deleted() {
    let d = deleting(0, MockEmailProvider::new());

    let why = refused(d.handler.handle_delete_mailbox(d.sent.id, None).await);

    assert!(why.contains("system mailbox"), "{why}");
    let inbox = d.repo.mailboxes[2].id;
    refused(d.handler.handle_delete_mailbox(inbox, None).await);
    assert!(d.provider.deleted_mailboxes.read().unwrap().is_empty());
}

#[tokio::test]
async fn a_mailbox_with_messages_needs_somewhere_to_move_them() {
    let d = deleting(2, MockEmailProvider::moving_to(900));

    let why = refused(d.handler.handle_delete_mailbox(d.junk.id, None).await);

    assert!(why.contains("holds 2 messages"), "{why}");
    assert!(d.provider.moved.read().unwrap().is_empty());
    assert!(d.provider.deleted_mailboxes.read().unwrap().is_empty());
}

#[tokio::test]
async fn messages_are_moved_then_the_mailbox_is_deleted() {
    let d = deleting(3, MockEmailProvider::moving_to(900));

    let result = d
        .handler
        .handle_delete_mailbox(d.junk.id, Some(d.sent.id))
        .await
        .unwrap();

    assert!(matches!(result, CommandResult::MailboxDeleted { moved: 3 }));
    assert_eq!(d.provider.moved.read().unwrap().len(), 3);
    assert_eq!(
        *d.provider.deleted_mailboxes.read().unwrap(),
        std::slice::from_ref(&d.junk.path)
    );
    assert_eq!(d.bus.last_event_name().unwrap(), "MailboxDeleted");
}

#[tokio::test]
async fn an_empty_mailbox_is_deleted_without_a_destination() {
    let d = deleting(0, MockEmailProvider::new());

    d.handler
        .handle_delete_mailbox(d.junk.id, None)
        .await
        .unwrap();

    assert_eq!(d.provider.deleted_mailboxes.read().unwrap().len(), 1);
}

#[tokio::test]
async fn a_failed_move_keeps_the_mailbox() {
    let provider = MockEmailProvider::moving_to(900);
    *provider.refuse.write().unwrap() = true;
    let d = deleting(2, provider);

    let why = refused(
        d.handler
            .handle_delete_mailbox(d.junk.id, Some(d.sent.id))
            .await,
    );

    assert!(
        why.contains("mailbox kept") && why.contains("of 2"),
        "{why}"
    );
    assert!(d.provider.deleted_mailboxes.read().unwrap().is_empty());
    assert_ne!(d.bus.last_event_name().as_deref(), Some("MailboxDeleted"));
}

#[tokio::test]
async fn the_destination_must_be_another_mailbox_of_the_account() {
    let d = deleting(1, MockEmailProvider::moving_to(900));

    refused(
        d.handler
            .handle_delete_mailbox(d.junk.id, Some(d.junk.id))
            .await,
    );
    refused(
        d.handler
            .handle_delete_mailbox(d.junk.id, Some(MailboxId::new()))
            .await,
    );
    assert!(d.provider.deleted_mailboxes.read().unwrap().is_empty());
}

/// `Work` and `Work/Fin` on the repo, the server answering as after the
/// rename to `Jobs`.
fn renaming() -> (Deleting, mc_domain::Mailbox, mc_domain::Mailbox) {
    let account = AccountId::new();
    let work = test_mailbox(account, "Work", MailboxType::Custom);
    let fin = test_mailbox(account, "Work/Fin", MailboxType::Custom);
    let other = test_mailbox(account, "Other", MailboxType::Custom);
    let provider = MockEmailProvider::new();
    *provider.listed.write().unwrap() = vec![
        test_mailbox(account, "Jobs", MailboxType::Custom),
        test_mailbox(account, "Jobs/Fin", MailboxType::Custom),
    ];
    let more = vec![work.clone(), fin.clone(), other];
    (deleting_with(0, provider, account, more), work, fin)
}

#[tokio::test]
async fn renaming_a_mailbox_renames_its_children_as_the_server_lists_them() {
    let (d, work, fin) = renaming();

    let result = d.handler.handle_rename_mailbox(work.id, "Jobs").await;

    assert!(matches!(
        result,
        Ok(CommandResult::MailboxRenamed { renamed: 2 })
    ));
    assert_eq!(
        *d.provider.renamed_mailboxes.read().unwrap(),
        [(work.path.clone(), "Jobs".to_owned())]
    );
    let renamed: Vec<_> = d
        .bus
        .published
        .read()
        .unwrap()
        .iter()
        .filter_map(|e| match &e.payload {
            DomainEvent::MailboxRenamed {
                mailbox_id,
                new_path,
                display_path,
                ..
            } => Some((*mailbox_id, new_path.full_path(), display_path.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        renamed,
        [
            (work.id, "Jobs".into(), "Jobs".into()),
            (fin.id, "Jobs/Fin".into(), "Jobs/Fin".into())
        ]
    );
}

#[tokio::test]
async fn a_mailbox_is_not_renamed_to_a_taken_name_inside_itself_or_as_a_system_one() {
    let (d, work, _) = renaming();

    for (new_name, why) in [
        ("other", "is taken"),
        ("Work/Fin/Old", "is taken"),
        ("Work/New", "inside"),
        ("A//B", "empty"),
    ] {
        let said = refused(d.handler.handle_rename_mailbox(work.id, new_name).await);
        assert!(said.contains(why), "{new_name}: {said}");
    }
    let said = refused(d.handler.handle_rename_mailbox(d.sent.id, "Mine").await);
    assert!(said.contains("system mailbox"), "{said}");
    assert!(d.provider.renamed_mailboxes.read().unwrap().is_empty());
}

#[tokio::test]
async fn renaming_to_the_name_it_has_changes_nothing() {
    let (d, work, _) = renaming();

    let result = d.handler.handle_rename_mailbox(work.id, "Work").await;

    assert!(matches!(
        result,
        Ok(CommandResult::MailboxRenamed { renamed: 0 })
    ));
    assert!(d.provider.renamed_mailboxes.read().unwrap().is_empty());
}

fn renamed_ids(d: &Deleting) -> Vec<MailboxId> {
    d.bus
        .published
        .read()
        .unwrap()
        .iter()
        .filter_map(|e| match &e.payload {
            DomainEvent::MailboxRenamed { mailbox_id, .. } => Some(*mailbox_id),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_rename_is_written_down_before_the_server_hears_of_it_and_forgotten_once_recorded() {
    let (d, work, _) = renaming();

    d.handler
        .handle_rename_mailbox(work.id, "Jobs")
        .await
        .unwrap();

    assert!(d.repo.renames.lock().unwrap().is_empty());

    // The server refuses: the record stays for a sync to settle.
    let (d, work, _) = renaming();
    *d.provider.listed.write().unwrap() = vec![];
    refused(d.handler.handle_rename_mailbox(work.id, "Jobs").await);
    assert_eq!(d.repo.renames.lock().unwrap().len(), 1);
    assert!(renamed_ids(&d).is_empty());
}

#[tokio::test]
async fn a_rename_the_crash_left_on_the_server_is_recorded_by_the_next_attempt() {
    let (d, work, fin) = renaming();
    d.repo
        .put_pending_rename(&mc_domain::PendingRename {
            mailbox_id: work.id,
            account_id: work.account_id,
            old_display: "Work".into(),
            new_display: "Jobs".into(),
        })
        .await
        .unwrap();

    // The mock read model does not follow events, so the rename that comes
    // after the settling is sent again; what counts is the settling.
    d.handler
        .handle_rename_mailbox(work.id, "Jobs")
        .await
        .unwrap();

    // Recorded from the listing, before anything else.
    assert_eq!(renamed_ids(&d)[..2], [work.id, fin.id]);
    assert!(d.repo.renames.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_rename_waits_for_a_sync_holding_the_account() {
    let (mut d, work, _) = renaming();
    let locks = Arc::new(crate::services::AccountLocks::default());
    d.handler = d.handler.clone().with_sync_locks(locks.clone());
    let sync = locks.lock(&work.account_id).await;

    let rename = tokio::spawn({
        let handler = d.handler.clone();
        async move { handler.handle_rename_mailbox(work.id, "Jobs").await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(d.provider.renamed_mailboxes.read().unwrap().is_empty());

    drop(sync);
    let result = rename.await.unwrap();
    assert!(matches!(
        result,
        Ok(CommandResult::MailboxRenamed { renamed: 2 })
    ));
}
