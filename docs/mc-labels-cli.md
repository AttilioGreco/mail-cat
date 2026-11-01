# `mc labels` for assistants

mail-cat finds groups of similar unlabelled mail (HDBSCAN clusters) and proposes
each as a new label, named from its keywords. These commands let a person or an
external assistant do a second pass: merge duplicates, rename, reject noise,
reuse existing labels, then accept. mail-cat never calls an AI itself.

Global flags: `--server <url>` (or `MAILCAT_SERVER_URL`), `--json`. Errors exit
non-zero with the message on stderr. Ids are full UUIDs. `--account` takes an
account id, name or email; it selects that account's training group (the set
of accounts whose labels are learned together). Proposals only merge within one
group.

## Commands

| Command | What it does |
|---|---|
| `mc labels discover [--account A]` | Run a search now and wait; prints the proposals |
| `mc labels proposals [--account A] [--status pending\|accepted\|rejected\|all]` | List proposals (default `pending`) |
| `mc labels proposals show <id> [--samples N]` | One proposal with N messages (default 10) |
| `mc labels proposals rename <id> <name>` | Rename a pending proposal |
| `mc labels proposals merge <id> <id>... [--name N]` | Replace pending proposals by one new pending proposal |
| `mc labels proposals assign <id> <label> [--archive]` | Put an existing label on the proposal's mail |
| `mc labels proposals accept <id> [--name N] [--archive]` | Create the label and put it on the mail |
| `mc labels proposals reject <id>` | Refuse it; the same cluster is not proposed again |
| `mc labels list [--account A]` | Existing labels with mode, threshold and example count |
| `mc labels mode <label> <off\|suggest\|apply\|apply_archive> [--threshold T] [--account A] [--existing] [--yes]` | Set a label's mode |

Notes:

- `merge`: members are the union, samples are taken in turn from the sources,
  keywords and senders are recomputed (the keywords weigh the merged mail
  against the group's other pending proposals). The new proposal has a new id;
  the sources disappear (they are not marked rejected). Needs 2+ different
  pending proposals of one group.
- `assign`: the label must already exist in the proposal's group (see
  `labels list`); it is applied to the mail that is still unlabelled, through
  the same throttled background queue as `accept`, the model learns from it, and
  the proposal becomes `accepted`.
- `accept` / `assign` return immediately; labelling continues in the
  background. `--archive` also takes the mail out of the Inbox. A label that was
  off starts in `suggest`.
- `mode`: `off` and `suggest` apply at once. `apply` and `apply_archive` only
  print what they would do until `--yes`. `--existing` (apply modes) also labels
  the mail already there: it prints the counts first, and with `--yes` runs.
  Without `--existing` an apply mode affects new mail only. A label needs 5
  examples before it can leave `off`; `--threshold` is 0.5 to 1.0.

## JSON (field names are stable; camelCase)

`proposals` -> array of

```json
{"id": "uuid", "name": "GitHub", "keywords": ["github"], "size": 120, "inbox": 100,
 "samples": [{"messageId": "uuid", "subject": "...", "from": "..."}],
 "group": "Tutti", "createdAt": 1700000000, "status": "pending",
 "scope": "g:<group uuid>", "senders": [{"domain": "github.com", "count": 118}]}
```

`size` is the message count, `inbox` how many are in an Inbox, `senders` the
dominant sender domains (most frequent first, at most 8), `createdAt` Unix
seconds.

`proposals show` -> the same object plus
`"messages": [{"messageId", "receivedAt" (RFC 3339), "from", "subject", "excerpt"}]`;
`excerpt` is the first ~200 characters of the plain text (empty while the body
is not downloaded). The proposal's own samples come first.

`proposals rename` / `merge` -> the resulting proposal object (`merge` includes
`senders`; `rename` leaves `senders` empty).

`proposals accept` / `assign` -> `{"id", "status": "accepted", "queued": N}`;
`reject` -> `{"id", "status": "rejected", "queued": null}`.

`labels list` -> array of
`{"scope", "group", "accountIds": [], "labels": [{"name", "label", "mode",
"threshold", "positives", "minPositives"}]}`. `name` is the label as the group
spells it, `label` its lowercase key, `mode` is `off|suggest|apply|apply_archive`.

`labels mode` -> `{"label", "mode", "threshold", "dryRun": bool,
"existingMail": null | {"toLabel", "toArchive", "sample": [...]}}`.

## Recommended workflow

1. `mc labels list --json` to learn the existing labels (and spellings).
2. `mc labels proposals --json` to see the pending proposals; start with the
   biggest `inbox` counts.
3. For anything unclear, `mc labels proposals show <id> --samples 15 --json`
   and read the subjects, senders and excerpts.
4. Clean up: `merge` duplicates (e.g. several clusters of one sender), `rename`
   with real names, `reject` noise, and `assign` proposals that belong to a
   label that already exists.
5. Show the user the resulting list and what each proposal will do.
6. Only after the user agrees, `accept` (add `--archive` only when the user
   agreed to take that mail out of the Inbox). Changes reach the mail servers
   in the background, a few messages per second.
7. Check `mc labels list`, then optionally move a new label from `suggest` to
   `apply` with `mc labels mode <label> apply --yes` (preview first without
   `--yes`).

A new discovery run (every few hours, or `mc labels discover`) refreshes the
pending list: a merged proposal is kept only when the new clusters still overlap
it by half; otherwise it is replaced, so answer proposals soon after curating.
