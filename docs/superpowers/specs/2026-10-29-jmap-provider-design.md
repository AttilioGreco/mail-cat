# JMAP provider and Labels|Folders semantics

## Problem

`EmailProvider` is already provider-neutral (`RemoteId`, `SyncCursor`, `MessageKey`,
`MailboxChanges` with `Snapshot|Delta`, `Reset::Rescan`), but only IMAP implements it.
`ProviderType::Jmap` and `IncomingServer::Jmap{session_url}` exist in the domain and proto
and are rejected at `account_service.rs:92`. Wiring is monomorphic
(`InMemoryProviderRegistry<ImapEmailProvider>`, `wiring.rs:42`). Mailboxes are always shown as
folders, even for Gmail, where a message really lives under several labels.

## Goals

1. A `JmapEmailProvider` (RFC 8620 core, RFC 8621 mail) usable against Stalwart, with push.
2. The application layer (`sync_service`, handlers) stays untouched except for label operations.
3. Per-account **Labels|Folders** semantics, used by JMAP now and by Gmail X-GM-LABELS next.
4. Contacts via JMAP Contacts (RFC 9610) in a later phase; calendars stay CalDAV.

## Non-goals

- Gmail/Outlook native APIs (`GmailApi`/`OutlookApi` are already gone from code).
- JMAP Calendars (still a draft).
- Data migration: the data folder is wiped on model changes.

## 1. Mapping to the existing trait

| Trait / type | JMAP |
|---|---|
| `RemoteId` | `Email.id` (same in every mailbox) |
| `MessageKey` | `Email.id` too, so cross-mailbox dedup is native |
| `MailboxPath` | JMAP `Mailbox.id`, opaque (decision A, see §1a); the readable path lives in `Mailbox.display_path`, built from `name`/`parentId` |
| `list_mailboxes` | `Mailbox/get`; `role` maps to `MailboxType` (inbox, sent, drafts, trash, junk→Spam, archive, all, important) |
| `SyncCursor` | per collection `Email/get.state`; mailbox list has its own state under collection `"mailboxes"` |
| `mailbox_changes(None)` | `Email/query` (filter `inMailbox`, sort `receivedAt` desc) → `Snapshot` + `added` |
| `mailbox_changes(Some)` | `Email/changes` + `Mailbox/get` for counts → `Delta{changed, removed}`; `cannotCalculateChanges` → `Reset::Rescan` |
| `fetch_envelopes` | `Email/get` with explicit properties (no body values) |
| `fetch_bodies_bulk` | `Email/get` with `fetchTextBodyValues`, `maxBodyValueBytes` = size cap |
| `fetch_message_detail` / `fetch_raw_message` | `Email/get` full / blob download |
| `find_by_message_id` | `Email/query` filter `header: ["Message-ID", id]` |
| `set_flags_bulk` | `Email/set` patches `keywords/$seen` etc. (`\Seen`→`$seen`, `\Flagged`→`$flagged`, `\Answered`→`$answered`, `\Draft`→`$draft`) |
| `move_messages` | `Email/set` patch `mailboxIds`; the new id equals the old one, so the returned map is identity |
| `delete_messages` | soft: move to the Trash role; permanent: `Email/set destroy` |
| `send_message` | blob upload + `Email/set` create + `EmailSubmission/set` with `onSuccessUpdateEmail` (move to Sent, clear `$draft`) → `SentCopy::ServerFiled` |
| `save_draft` | `Email/set` create in the Drafts role with `$draft`; `replacing` is destroyed in the same request |
| `watch_mailbox` / `AccountEventSource` | `EventSource` SSE (`/eventsource`, types `Email,Mailbox`); a state change yields `MailboxCountChanged`, and the sync pass does the diff, like IMAP |

Batching: one JMAP request carries several method calls; chunk by the server's
`maxObjectsInGet/Set` and `maxCallsInRequest` from the session object.

Throttling and auth errors map to the same `DomainError` variants `stops_the_pass` already
recognises, so `AuthGate` works unchanged. JMAP credentials: Basic (password) or Bearer
(OAuth), read from the keychain like IMAP.

## 1a. Opaque `MailboxPath`

`MailboxPath` becomes an opaque provider identifier (IMAP: the mailbox name, as today; JMAP:
`Mailbox.id`). `Mailbox` gains `display_path` for everything a person reads: sidebar tree,
sorting, CLI output. Step 1 of phase 1 is an audit of every caller that treats `path` as text
(sidebar tree split on the delimiter, `Sent` fallback lookups, CLI `--mailbox`, proto mappers)
and moving it to `display_path`. CLI `--mailbox` matches id, path or display_path.

## 2. Wiring

- `ProviderRegistry::Provider` becomes `AnyProvider`, an enum `{Imap(ImapEmailProvider),
  Jmap(JmapEmailProvider)}` delegating the trait (two variants, so no `dyn`).
  `AnyEventSource` likewise.
- `wiring.rs` builds the provider from `IncomingServer`; `config.rs` `ProviderTypeConfig` gains
  `Jmap`; `account_provisioner.rs` and `account_service.rs:92` accept it.
- Account setup: user gives the session URL (or just the domain and we try
  `https://<domain>/.well-known/jmap`). Login test = fetch the session object before storing
  anything, same rule as IMAP.
- Crate: `jmap-client` (Stalwart Labs) rather than a hand-rolled client. If it lacks
  `EmailSubmission` or `EventSource` features we need, wrap the raw request API.

## 3. Labels|Folders

`message_mailboxes` is already M:N, so storage does not change. What changes is the
**meaning** and the operations offered.

```
enum MailboxModel { Folders, Labels }   // per account, stored on the account
```

| | Folders | Labels |
|---|---|---|
| Default for | classic IMAP | JMAP, Gmail |
| Message sits in | exactly one mailbox | one or more |
| UI | tree, drag to move | chips on the message, label picker |
| Move | replace the set | n/a (add/remove) |
| Archive | move to Archive | remove Inbox label |
| Delete | move to Trash | replace the set with Trash (labels are lost, as in Gmail) |
| Sidebar | folder tree | label list; a label is a filtered view |

Two label mechanisms exist and both are supported:

- **Mailbox membership** (Gmail X-GM-LABELS, JMAP `mailboxIds`): the label is a mailbox, the
  message sits in several. Operated by `change_labels` below.
- **Keywords / tags** (IMAP user flags such as `$Work`, JMAP `keywords`): the message stays in one
  mailbox and carries N tags. Standard IMAP (`PERMANENTFLAGS \*`), so it also works on classic
  IMAP accounts; operated by `set_flags_bulk` with custom `Flag`s. Servers may refuse arbitrary
  keywords, so support is detected per account and the tag UI hidden when absent.

Trait addition (one method; `move_messages` stays for Folders):

```rust
async fn change_labels(&self, ids: &[RemoteId], in_mailbox: &MailboxPath,
                       add: &[MailboxPath], remove: &[MailboxPath]) -> Result<(), DomainError>;
```

- JMAP: `Email/set` patches `mailboxIds/<id>: true | null` (atomic, no copy).
- IMAP Gmail (phase 2): `STORE X-GM-LABELS`. Plain IMAP: not supported; the account is `Folders`.
- Handler `change_labels` updates `message_mailboxes` rows through the existing
  `MessageMoved`/`left_mailbox` events; no new event type unless the projector needs one
  (decide while implementing, see Open).
- The UI reads `MailboxModel` from the account and shows chips vs tree; the choice is a user
  setting per account, defaulted by provider. Switching only changes presentation and which
  operations are offered, never stored data.

## 4. Sync notes

- Snapshot on first sync can be large (48k). `Email/query` with `position/limit` pages of
  `maxObjectsInGet`; `added` is newest-first as the trait requires.
- Email ids are stable across mailboxes, so `join_known` by `MessageKey` collapses label copies
  without any X-GM-MSGID-style trick.
- `Reset::Rescan` replaces the IMAP UIDVALIDITY path: full snapshot, no wipe of local state.
- No Trash/Spam special-casing here; which mailboxes are synced is the separate
  subscriptions work (after JMAP).

## 5. Testing

- Stalwart v0.16.25 already runs in `tests/itest` (docker). Add a JMAP account to the harness
  next to the IMAP one and run the same 8 scenarios through `JmapEmailProvider`.
- Unit: keyword↔`Flag` mapping, `Reset::Rescan` on `cannotCalculateChanges`, role→`MailboxType`.
- `mc-cli` gaps still open for testing (`sync --mailbox`, IDLE off, `--json`, show
  UID/mailbox/Message-ID/flags, `accounts add` with password) are fixed first, as step 0.

## 6. Phases

0. mc-cli testability gaps.
1. `JmapEmailProvider` read path: session, `list_mailboxes`, `mailbox_changes`, envelopes,
   bodies. `AnyProvider` wiring and account add. itest sync scenarios green.
2. Write path: flags, move, delete, drafts, send. SSE push.
3. `MailboxModel` + `change_labels` (JMAP), handler, proto field, UI chips/picker.
4. Gmail X-GM-LABELS on IMAP using the same `change_labels`.
5. JMAP Contacts.

## Open

- Per-account detection of arbitrary-keyword support on IMAP (PERMANENTFLAGS `\*`).
- Whether `change_labels` needs its own domain event or fits `MessageMoved` with several
  destinations.
- `Email/query` snapshot vs `Email/changes` from state `""` for the first sync on big
  mailboxes; measure on Stalwart first.

## Phase 4 design: Gmail over IMAP as labels

Account stays `ProviderType::Imap`. Data folder is wiped (memberships change meaning).

1. **Detection.** `CAPABILITY X-GM-EXT-1` (already `ServerCaps.gmail`) and a LIST that carries
   `\All`. Without `\All` (hidden from IMAP in Gmail settings) the account is plain IMAP.
   Default model: `EmailProvider::preferred_mailbox_model()` (`Labels` once detected). Each
   full sync appends `AccountMailboxModelChanged` only if the account's event stream holds
   none, so an explicit choice made after the first sync is never overridden (a choice
   of the already-current `Folders` before the first sync writes no event and is lost).
2. **Sync shape.** Real IMAP mailboxes synced: All Mail, Spam, Trash (All Mail excludes the
   last two). Every other LIST entry (INBOX, Sent, Drafts, Important, Starred, user labels)
   is a *view*: discovered and shown, never synced, never selected. New trait method
   `backed_by(path) -> Option<MailboxPath>` (view -> All Mail, else `None`): `run_sync` skips
   views, `sync_mailbox` redirects them to All Mail (IDLE on INBOX, resyncs after label ops).
3. **Memberships.** A view membership carries the *All Mail UID* as its `remote_id` (the
   handlers' "same RemoteId in every mailbox" rule, as for JMAP). The provider resolves a
   view to All Mail in `selected()`, so every call that names a view (flags, bodies,
   find_by_message_id, ...) addresses All Mail. Trash/Spam rows keep their own UID.
   `MailboxChanges.labels: HashMap<RemoteId, Vec<MailboxPath>>` lists the views of each
   added/changed All Mail message; `sync_one` reconciles that message's view memberships
   (`MessageMailboxesChanged`, only views backed by this mailbox) and removes them with the
   All Mail membership when the message leaves All Mail.
   Label name -> path: the LIST name equals the label as `X-GM-LABELS` spells it (modified
   UTF-7, `/` hierarchy), except system labels (`\Inbox \Sent \Draft \Important \Starred`)
   which map by role / `\Flagged`. Display names are UTF-7-decoded. Unknown labels are dropped.
4. **Delta.** All Mail cursor `gm1:{validity}:{uid_next}:{modseq}`. First sync (or
   UIDVALIDITY change, or no HIGHESTMODSEQ): `FETCH (FLAGS X-GM-LABELS X-GM-MSGID)` of
   everything. Later: `UID SEARCH UNDELETED` (what is still there; CONDSTORE reports no
   expunges, Gmail has no QRESYNC) plus `FETCH 1:* (...) (CHANGEDSINCE modseq)` (flags and
   labels of what changed, added messages included). New `Existing::Listed{present, changed}`;
   reconcile treats an absent known id as gone, a changed one as flags + labels.
5. **Operations** (Gmail mode only; plain IMAP untouched).
   - `change_labels`: `UID STORE .. +/-X-GM-LABELS (..)` on All Mail; `\Inbox` for the Inbox
     view (archive = `-X-GM-LABELS (\Inbox)`); custom names quoted. Refused for Trash/Spam
     rows (their UID is not an All Mail UID) and for All Mail/Trash/Spam as a label.
   - `move_messages` (Folders model, delete, archive): view/All Mail -> view = label add/remove,
     identity id map; -> Trash/Spam or out of them = real `UID MOVE`/`COPY`, new ids kept only
     when the destination is a real mailbox, else `None` (handler looks them up by Message-ID).
   - permanent delete of an All Mail row: MOVE to Trash, then `\Deleted` + `UID EXPUNGE` there
     (flagging in All Mail is not a documented hard delete).
   - Tags: unchanged (PERMANENTFLAGS), STORE on All Mail.
   - IDLE on a view: a pure FETCH becomes `MailboxCountChanged` (UIDs of the view are not ours).
6. **Subscriptions** (LSUB) are not consulted; future work, orthogonal.
