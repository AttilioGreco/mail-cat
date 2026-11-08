# `mc labels` for assistants

mail-cat finds groups of similar unlabelled mail (HDBSCAN clusters) and proposes
each as a new label, named from its keywords. These commands let a person or an
external assistant do a second pass: merge duplicates, rename, reject noise,
reuse existing labels, then accept. mail-cat never calls an AI itself.

Global flags: `--socket <path>` (or `MAILCAT_SOCKET`), `--format
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
| `mc labels rename <from> <to> [--mail-only] [--account A] [--yes]` | Rename a label (and its children) on every account of its group, or merge it into one that exists; `--mail-only` moves just its own mail |
| `mc labels delete <label> [--account A] [--yes]` | Take a label off all its mail and delete it (an empty container too); the mail stays |
| `mc labels mode <label> <off\|suggest\|apply\|apply_archive> [--threshold T] [--account A] [--existing] [--yes]` | Set a label's mode |
| `mc labels assign-matching <label> [--from ADDR\|DOMAIN]... [--list-id ID]... [--account A] [--archive] [--yes]` | Put a label on all the mail from some senders, domains or lists |
| `mc labels apply-all [--label L]... [--account A] [--yes]` | Label the existing mail the classifier is sure of, for many labels at once |

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
  examples before it can leave `off`; `--threshold` is 0.5 to 1.0. `--account`
  is needed only when several training groups have the label.

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
"kind": "apply"|"undo"|"relabel"|"delete", "done", "total", "archived"}]`: one entry per
labelling job from the moment it is queued (`done` is 0 while it waits; jobs
run one at a time, a few messages per second). The UI's background card shows
the same entries.

`labels rename` / `labels delete` -> `{"dryRun", "label", "into", "merge",
"mailOnly", "messages", "accounts": [{"accountId", "kind", "messages",
"merges", "deletesMailbox", "rehomed", "mailboxes": [{"label", "into", "how",
"messages"}]}]}`; `into` is `null` for a delete, `kind` is `mailbox`,
`folder` or `keyword`, `how` is `rename`, `merge`, `create+move` or `delete`
and `mailboxes` lists, parents first, every mailbox of the label's tree the
account changes. See Renaming and deleting labels.

`labels assign-matching` -> `{"dryRun", "label", "toLabel", "toArchive",
"already", "creates", "accounts": [{"accountId", "messages", "archived",
"already", "creates"}], "sample": [{"messageId", "subject", "from"}]}`;
`apply-all` -> `{"dryRun", "toLabel", "labels": [{"scope", "name", "label",
"mode", "threshold", "positives", "toLabel", "sample": [...]}]}`. See Labelling
in bulk.

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
stats` (fields plus `retrains` and `applying` tables), the `plan` / `undo`
summaries, `rename` / `delete` (header fields plus `accounts` and `mailboxes` tables),
`assign-matching` (header fields plus `accounts` and `sample` tables) and `apply-all`
(header fields plus a `labels` table).

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

## Labelling in bulk

`assign-matching <label>` is for mail discovery never grouped (it sat in a
mixed cluster, so the model cannot learn it). It takes the mail of the label's
group whose sender is `--from` (an address, or a domain: `facile.it` also
matches `preventivo@remind.facile.it`, not `notfacile.it`) or that came through
the list `--list-id` (with or without `Name <...>`). At least one filter is
needed. Mail in the Trash, Spam or Drafts and mail that already has the label
are left out (the latter is counted as `already`). `<label>` is a path; each
account that holds matching mail and lacks the label gets it created (`creates`),
or uses a keyword on a folders account. The label goes on through the same
throttled queue as `assign` (an `apply` job in `labels stats`), the model
learns the mail as your own examples (like `assign`, so `undo` and retraining
treat them as yours), and `--archive` also takes it out of the Inbox. A label
that was off starts in `suggest`. Without `--yes` it prints the counts per
account, the skipped ones and a few samples. `--account` is needed only for a
new label when there are several groups. gRPC: `AssignMatchingMail`
(`{label, from[], listId[], accountId?, archive, dryRun}`).

`apply-all` is `mode <label> apply --existing` for many labels. The preview
lists every label of the group with at least 5 examples: `mode`, `threshold`,
`positives` and `toLabel`, the existing mail (unlabelled or not, as for
`--existing`) the classifier would label at that threshold, computed in one
pass over the mail for all labels. With `--yes` it queues one `apply` job per
label (default: labels whose mode is not `off`; `--label` names others
explicitly, repeatable), never archives, and leaves modes alone. These are
auto-applied labels: they can be undone and the model does not count them as
your examples. A label that is being applied, undone or relabelled is refused.
gRPC: `ApplyAllLabels` (`{labels[], accountId?, dryRun}`).

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

## Renaming and deleting labels

`labels rename <from> <to>` and `labels delete <label>` change a label that
already exists, on every account of one training group (`--account` picks the
group, and is needed only when several groups have the label). Nothing is
done without `--yes`: the dry run prints, per account, every mailbox of the
label's tree that changes, how (`rename`, `merge`, `create+move`, `delete`) and
how many messages it holds. With `--yes` the work is queued like an
acceptance: throttled, one job at a time, listed in `mc labels stats` as a
`relabel` or `delete` job, and refused while either label is being applied or
undone. If it stops halfway, run it again: the steps can be repeated.

`rename` moves the whole tree, level by level, on each account:

- `to` (or `to/c` for a child `from/c`) does not exist there and nothing is
  below it: the mailbox is **renamed on the server** with `RENAME` (IMAP, Gmail
  included) or `Mailbox/set` (JMAP), missing parent levels of `to` are made,
  and its children follow. No message is touched, the mailbox keeps its id, and
  the next sync finds it under its new name. A change of case alone
  (`Dev/news` to `Dev/News`) is a rename too: classifier keys do not change,
  only the spelling is updated.
- `to` exists: a **merge**. The mail of `from` gets `to` and loses `from`
  (other labels and the Inbox are kept), then each child `from/c` goes to
  `to/c` by the same rule, and `from` is deleted once it is empty and
  childless. If only `to`'s children exist, `to` is made first (`create+move`).
- `--mail-only`: just the mail of `from` itself goes to `to` (made when
  missing, merged into when it exists); `from` and its children stay, and
  `from` is deleted only if it ends empty and childless. `to` may be inside
  `from`: `rename 4sigma "4sigma/Buste paga" --mail-only` files the direct mail
  of `4sigma` under its child and keeps the other children where they are. On a
  folders account a folder with children is refused, as moving its mail would
  delete it.

| Account | Rename | Delete |
|---|---|---|
| Gmail (IMAP), JMAP | As above; a merge puts `to` on every message that has `from` and takes `from` off | Takes `from` off, deletes the mailbox. A message in no other mailbox is filed in the Archive (else All Mail, else the Inbox): JMAP refuses mail in no mailbox |
| Folders, label is a real folder | Renamed; a merge moves the mail into the folder `to` and deletes `from` | Refused: say where the mail goes with `mc mailboxes delete --move-to` |
| Folders, label is a keyword | Replaces the keyword on each message, for the label and every child | Removes the keyword |

`delete` works on an empty container too (no mail, no tagging data), found by
its mailbox name. A label with child labels is refused: rename or delete the
children first, or rename the label to move the tree.

Refused: a system mailbox or Inbox as `from` or `to`, `to` spelled exactly like
`from`, `to` inside `from` without `--mail-only`, and a label no account of
the group has. Mail in the Trash, Spam or Drafts is relabelled like the rest
(it stays where it is, and on delete it just loses the label; only mail in no
other mailbox is filed in the Archive). If the mailbox still holds mail the
job could not move off it, it is kept, the error says so and the job is
finished by running the command again.

`mc mailboxes rename <account> <mailbox> <new name>` is the plain mailbox
rename (no classifier), with the same server behaviour. Names with non-ASCII
characters are sent as they are (no modified UTF-7 encoding), like label
creation.

What the classifier keeps follows, for the label and every label below it
(`from/x` becomes `to/x`). The settings move to the new key (the settings `to`
has win in a merge), the rejections that name it follow it, and its counts are
dropped and learnt again from the mail (deleting a mailbox makes the group
retrain, keyword changes retrain message by message). A rename that keeps the
keys (case only) leaves all of this alone. Accepted proposals keep working:
their records move to the new key and their names to the new spelling, so
`proposals undo` still takes the label off exactly the mail it put it on. A
renamed mailbox keeps its id, so the label an acceptance created is still the
one an undo deletes; after a merge the undo deletes nothing for the merged
level, since that label was not the acceptance's. Deleting a label turns the
accepted proposals of that name into `rejected` (the cluster is not proposed
again) and forgets their records; the examples are withdrawn, not turned into
negatives.

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
