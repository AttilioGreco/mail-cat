# `mc labels` for assistants

mail-cat finds groups of similar unlabelled mail (HDBSCAN clusters) and proposes
each as a new label, named from its keywords. These commands let a person or an
external assistant do a second pass: merge duplicates, rename, reject noise,
reuse existing labels, then accept. mail-cat never calls an AI itself.

Global flags: `--server <url>` (or `MAILCAT_SERVER_URL`), `--format
table|json|toon` (`--json` is an alias of `--format json`; `toon` is described
below and exists for the `labels` commands only). Errors exit non-zero with the
message on stderr. Ids are full UUIDs. `--account` takes an
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
| `mc labels proposals plan <file> [--dry-run] [--yes]` | Check a whole curation plan and, with `--yes`, run it |
| `mc labels proposals undo <id> [--yes] [--reject]` | Take an accepted proposal's label off again |
| `mc labels list [--account A]` | Existing labels with mode, threshold and example count |
| `mc labels show <label> [--account A] [--samples N]` | One existing label: scope, mode, where each account keeps it, recent mail |
| `mc labels stats` | Background work, including the labelling jobs (`applying`) |
| `mc labels mode <label> <off\|suggest\|apply\|apply_archive> [--threshold T] [--account A] [--existing] [--yes]` | Set a label's mode |

Notes:

- `rename` / `merge` mark the proposal `curated`. A new discovery run never
  deletes or replaces a curated pending proposal, and drops any new cluster
  that overlaps one by half or more (Jaccard), so curated work is not undone
  by the next search.
- Names are label paths: `Shopping/Ebay` is the label `Ebay` inside `Shopping`
  (see Hierarchy). Segments are trimmed; an empty segment is refused.
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
 "scope": "g:<group uuid>", "senders": [{"domain": "github.com", "count": 118}],
 "curated": false}
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

`labels show` -> array (one per training scope that has the label) of
`{"scope", "group", "accountIds", "name", "label", "mode", "threshold",
"positives", "minPositives", "accounts": [{"accountId", "kind", "mailboxId",
"path", "messages", "keyword", "examples"}], "samples": [<proposals show message>]}`.
`kind` is `mailbox` (the account has a mailbox of that name; `messages` is its
size), `keyword` (no mailbox, but its mail carries the keyword: the account
labels with tags) or `missing`. `examples` is how many of the account's
messages the model learnt the label from; `samples` are the newest of them.

`labels mode` -> `{"label", "mode", "threshold", "dryRun": bool,
"existingMail": null | {"toLabel", "toArchive", "sample": [...]}}`.

`labels stats` -> besides the embedding fields, `"applying": [{"label",
"kind": "apply"|"undo", "done", "total", "archived"}]`: one entry per
labelling job from the moment it is queued (`done` is 0 while it waits; jobs
run one at a time, a few messages per second). The UI's background card shows
the same entries.

`proposals plan` -> `{"dryRun", "summary", "results"}`, see Plans.
`proposals undo` -> `{"dryRun", "toUnlabel", "toUnarchive", "accounts":
[{"accountId", "messages", "archived"}], "labelsToDelete": [], "status"}`.

## TOON (`--format toon`)

A compact text form, cheaper for an assistant to read than JSON because field
names appear once per table. Written by hand to the TOON conventions: a table
is `name[N]{field,...}:` followed by one comma-separated row per line, indented
two spaces; an object is `key: value` lines; a short list is `key[N]: a,b,c`.
A value is quoted, with JSON escapes inside the quotes, when it is empty, has a
space at either end, contains a comma, colon, quote, backslash, bracket, brace
or control character (newline), starts with `-`, or would otherwise read as a
number, `true`, `false` or `null`. A list inside a cell is its items joined by
`|` (`github|pull`). Field names are the JSON ones.

```
$ mc labels proposals --format toon
proposals[2]{id,name,size,inbox,curated,status,group,keywords,senders}:
  0198d1a2-...-0001,GitHub,120,100,true,pending,Tutti,github|pull,github.com(118)
  0198d1a2-...-0002,"Shop, \"Ebay\"",80,60,false,pending,Tutti,ebay,ebay.com(80)
```

Available for `labels proposals` (table `proposals`), `proposals show` (header
fields, `keywords[N]`, `senders[N]` and a `samples[N]{messageId,receivedAt,from,
subject,excerpt}` table), `labels list` (table `labels`, one row per label with
its scope), `labels show` (header, `accounts[N]` and `samples[N]`), `labels
stats` (fields plus `retrains` and `applying` tables) and the `plan` / `undo`
summaries.

## Hierarchy

A label name with `/` is a path: `Shopping/Ebay` is `Ebay` inside `Shopping`.
The label key the classifier learns, matches and shows is the whole lowercased
path (`shopping/ebay`); a parent such as `shopping` is a label only of the mail
that is in it directly, so a container created to hold children, and
empty, is not a training label (`labels list` does not show it, `labels show
shopping` reports 0 examples).

| Account | What creating `Shopping/Ebay` does |
|---|---|
| Gmail (IMAP) | `CREATE` + `SUBSCRIBE` of `Shopping`, then of `Shopping/Ebay`, as Gmail's delimiter is `/`; a level that exists is kept. Gmail shows the nesting. |
| Other IMAP, delimiter `/` | The same two levels. |
| JMAP | `Mailbox/get` first, then one `Mailbox/set` that creates the missing levels, each with `parentId` of the one above (a new parent is referenced as `#m0`...). Names are matched case-insensitively under the same parent, so an existing `Shopping` is reused. |
| Folder accounts (keywords) | Keywords cannot nest. The label key is used as the keyword, with only characters outside the IMAP atom / JMAP keyword set replaced by `_` (spaces too, read back as spaces). `/` is a valid keyword character in both, so `shopping/ebay` is stored as it is and reads back as the same label. A server that refuses `/` in a keyword is not handled. |

IMAP servers whose hierarchy delimiter is not `/` (for example `.`) are not
translated: the name is created with the `/` in it, which such a server may
refuse or keep as one flat mailbox.

## Plans

`proposals plan <file>` takes many curation steps as one reviewed unit. The
server checks the whole plan against the current proposals before touching
anything; if any step is invalid the command fails, nothing is applied, and the
error lists every problem (`op 3 (assign): label "X" does not exist in the
proposal's group`). Without `--yes` (or with `--dry-run`) it only reports. With
`--yes` it runs the steps in order and stops at the first one that fails: the
steps before it stay done, the report marks that one `failed` and the rest
`skipped`. Accepting and assigning start the usual throttled background
labelling (one job at a time); watch it in `mc labels stats`, or undo it.

The file is a JSON array of steps (or `{"ops": [...]}`); each step is an object
with a single key. Unknown keys are errors.

| Step | Fields |
|---|---|
| `merge` | `ids` (2+), `name`, `as` (an alias for later steps) |
| `rename` | `id`, `name` |
| `assign` | `id`, `label` (must exist in the group, or be created by an earlier step), `archive` |
| `accept` | `id`, `name` (default: the proposal's), `archive` |
| `reject` | `id` |
| `mode` | `label`, `mode` (`off`, `suggest`, `apply`, `apply_archive`), `threshold`, `existing`, `account` (only if the label is in several groups) |

`id` is a proposal id, or `@alias` for the result of an earlier `merge` that
had `"as": "alias"`. A proposal can be used by one step only (a merged,
accepted, assigned or rejected one is gone for the steps after; `rename` does
not use it up). A `merge` without `name` takes its name from its keywords at
run time, which a plan cannot know: name it, or give the `accept` a name. A
`mode` step needs a label that already has 5 examples and is not filled by the
same plan (a new label's examples arrive as the labelling runs; set its mode
afterwards).

```json
[
  {"merge": {"ids": ["<uuid>", "<uuid>"], "name": "Dev/GitHub", "as": "github"}},
  {"accept": {"id": "@github", "archive": true}},
  {"rename": {"id": "<uuid>", "name": "Shopping/Ebay"}},
  {"accept": {"id": "<uuid>"}},
  {"assign": {"id": "<uuid>", "label": "Newsletter", "archive": true}},
  {"reject": {"id": "<uuid>"}},
  {"mode": {"label": "Newsletter", "mode": "apply", "threshold": 0.9}}
]
```

The report: `summary` has the number of each step kind (`merged`, `renamed`,
`accepted`, `assigned`, `rejected`, `modes`), `toLabel` and `toArchive`
(messages the plan will label, and of those how many leave the Inbox),
`accounts` (the same per account: `accountId`, `messages`, `archived`),
`labelsToCreate` (`[{"name", "accountIds"}]`: labels no account has yet, once
per name even when several steps use it) and `labelsTouched` (labels that
already exist and receive mail or a new mode). `results` has one entry per
step: `index` (1-based), `kind`, `status` (`planned`, `done`, `failed`,
`skipped`), `detail`, `proposalId` (the proposal a merge made) and `queued`
(messages queued by an accept, assign or `existing` mode). Counts only include
mail that is still unlabelled and can take a label.

## Undoing an acceptance

`proposals undo <id>` reverses an accepted (or assigned) proposal using the
records its labelling left: the label comes off exactly the messages it put it
on (and only those that still carry it), mail it archived goes back to the
Inbox, the model withdraws those examples (they do not become negatives), and
a label the acceptance created is deleted when it is empty (a label that was
there before is never deleted; parents made for a path are left). The proposal
becomes `pending` again, or `rejected` with `--reject`. Without `--yes` it
only prints the counts. It is refused while the label is still being applied or
undone. Undoing runs in the background, throttled like accepting, and shows in
`mc labels stats` as an `undo` job; the proposal changes status when it is
done, and if it stops halfway it stays `accepted` and the command can be run
again.

## Recommended workflow

For an assistant curating many proposals (about a hundred) for a person who
approves one consolidated plan:

1. `mc labels proposals --format toon` for the pending proposals; start with
   the biggest `inbox` counts. `curated` ones were already worked on.
2. For the ambiguous ones, `mc labels proposals show <id> --samples 15
   --format toon` and read the subjects, senders and excerpts.
3. `mc labels list --format toon`, and `mc labels show <label> --format toon`
   for any label a proposal might belong to: compare names, example counts and
   recent mail before choosing between `assign` and `accept`.
4. Write a plan file that groups everything into a small set of top-level
   categories with `/` subcategories (`Shopping/Ebay`, `Dev/GitHub`), merges
   duplicates, names every new label, assigns proposals that belong to an
   existing label and rejects noise. Use `archive` only for what the person
   agreed to take out of the Inbox.
5. `mc labels proposals plan plan.json --dry-run --format toon`. Fix every
   error it lists and repeat until it validates.
6. Present the person the summary: labels to create, labels touched, messages
   to label and to archive per account, proposals rejected. Apply with `--yes`
   only after they agree.
7. Watch `mc labels stats --format toon` (`applying`) until it is empty, then
   check `mc labels list`. Optionally move a new label from `suggest` to
   `apply` with `mc labels mode <label> apply --yes` (preview first without
   `--yes`).
8. If the person changes their mind about an accepted proposal, `mc labels
   proposals undo <id>` (look first, then `--yes`).

A new discovery run (every few hours, or `mc labels discover`) refreshes the
pending list: a plain proposal is replaced by the new clusters that overlap it
by half, or dropped when none does. Renamed and merged proposals are curated
and kept (new clusters overlapping them are dropped), but answer plain ones
soon.
