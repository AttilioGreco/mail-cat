use super::*;

const COUNTS: MailboxCounts = MailboxCounts {
    total: 0,
    unread: 0,
};

fn at(uid_validity: u32, uid_next: u32) -> Position {
    Position {
        uid_validity,
        uid_next,
    }
}

fn ids(ids: &[u32]) -> Vec<RemoteId> {
    ids.iter().copied().map(RemoteId::from).collect()
}

fn snapshot(changes: MailboxChanges) -> Vec<(RemoteId, Vec<Flag>)> {
    match changes.existing {
        Existing::Snapshot(entries) => entries,
        Existing::Delta { .. } => panic!("IMAP lists everything"),
    }
}

#[test]
fn cursor_round_trips_and_rejects_other_formats() {
    let position = at(7, 42);
    assert_eq!(position.cursor().as_str(), "imap1:7:42");
    assert_eq!(Position::parse(&position.cursor()), Some(position));
    for foreign in ["", "jmap:abc", "imap1:7", "imap1:7:42:1", "imap1:x:42"] {
        assert_eq!(
            Position::parse(&SyncCursor::new(foreign)),
            None,
            "{foreign}"
        );
    }
}

#[test]
fn first_sync_adds_everything_newest_first() {
    let listing = vec![(1, vec![]), (5, vec![]), (3, vec![])];
    let changes = changes_from(None, 7, COUNTS, listing);
    assert_eq!(changes.reset, None);
    assert_eq!(changes.added, ids(&[5, 3, 1]));
    assert_eq!(
        changes.cursor,
        at(7, 6).cursor(),
        "one past the highest UID"
    );
}

#[test]
fn only_messages_from_the_cursors_uid_next_are_added() {
    let cursor = at(7, 4).cursor();
    let listing = vec![(2, vec![]), (4, vec![]), (6, vec![])];
    let changes = changes_from(Some(&cursor), 7, COUNTS, listing);
    assert_eq!(changes.reset, None);
    assert_eq!(changes.added, ids(&[6, 4]));
    assert_eq!(
        snapshot(changes).len(),
        3,
        "known messages stay in the listing"
    );
}

#[test]
fn changed_uid_validity_invalidates_ids_and_adds_everything() {
    let cursor = at(7, 100).cursor();
    let changes = changes_from(Some(&cursor), 8, COUNTS, vec![(1, vec![]), (2, vec![])]);
    assert_eq!(changes.reset, Some(Reset::IdsInvalidated));
    assert_eq!(changes.added, ids(&[2, 1]));
}

#[test]
fn unreadable_cursor_invalidates_ids() {
    let cursor = SyncCursor::new("something else");
    let changes = changes_from(Some(&cursor), 8, COUNTS, vec![(1, vec![])]);
    assert_eq!(changes.reset, Some(Reset::IdsInvalidated));
}

#[test]
fn deleted_and_recent_never_reach_the_application() {
    let listing = vec![
        (1, vec![Flag::Seen, Flag::Deleted]),
        (2, vec![Flag::Recent, Flag::Flagged]),
        (3, vec![Flag::Deleted]),
    ];
    let changes = changes_from(None, 1, COUNTS, listing);
    assert_eq!(changes.added, ids(&[2]));
    assert_eq!(
        snapshot(changes),
        vec![(RemoteId::from(2), vec![Flag::Flagged])]
    );
}

#[test]
fn first_sync_of_an_empty_mailbox_starts_at_one() {
    let changes = changes_from(None, 7, COUNTS, vec![]);
    assert!(changes.added.is_empty());
    assert_eq!(changes.cursor, at(7, 1).cursor());
}

#[test]
fn the_cursor_never_moves_back() {
    let cursor = at(7, 10).cursor();
    let changes = changes_from(Some(&cursor), 7, COUNTS, vec![(3, vec![])]);
    assert!(changes.added.is_empty());
    assert_eq!(changes.cursor, at(7, 10).cursor());
}

#[test]
fn reset_starts_the_cursor_over() {
    let cursor = at(7, 100).cursor();
    let changes = changes_from(Some(&cursor), 8, COUNTS, vec![(2, vec![])]);
    assert_eq!(changes.cursor, at(8, 3).cursor());
}

/// A message appended between STATUS and the listing is in the listing,
/// so in both `added` and the snapshot, and the cursor moves past it.
#[test]
fn a_message_appended_during_the_sync_is_added_and_the_cursor_passes_it() {
    let cursor = at(7, 3).cursor();
    let changes = changes_from(
        Some(&cursor),
        7,
        COUNTS,
        vec![(2, vec![]), (3, vec![]), (9, vec![])],
    );
    assert_eq!(changes.added, ids(&[9, 3]));
    assert_eq!(changes.cursor, at(7, 10).cursor());
    assert_eq!(snapshot(changes).len(), 3);
}

#[test]
fn a_missing_uid_validity_is_an_error() {
    assert!(matches!(
        require_uid_validity(None, "INBOX"),
        Err(DomainError::Provider(_))
    ));
    assert_eq!(require_uid_validity(Some(7), "INBOX").unwrap(), 7);
}
