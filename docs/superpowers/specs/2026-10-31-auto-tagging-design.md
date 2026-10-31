# Auto-tagging: label suggestions, auto-apply and label discovery

**Date:** 2026-10-31
**Status:** Design, builder-checked, pending user approval

## Intent

Newsletters, promotions and other recurring mail are labelled by hand today.
The user wants mail-cat to:

1. **suggest** the right label on a message ("put this in Newsletter"), and,
   once trusted, **apply it automatically**;
2. **discover** labels the user hasn't created yet: group similar unlabelled
   mail and propose a name for the group ("43 similar emails: amazon · ordine ·
   spedizione — create label *Ordini*?");
3. keep learning: every label the user sets, every suggestion accepted or
   rejected, and every discovered label accepted becomes training data.

It runs continuously in the background ("un po' sempre"), not as a one-off.

## Goals

- Per-label suggestion and auto-apply, with the action configurable per label.
- Unsupervised discovery of new labels from unlabelled mail, named from
  keywords, proposed to the user.
- Feedback loop: accepted discoveries train the per-label classifiers.
- Everything computed **locally**. No mail content, features or vectors leave
  the machine.

## Non-goals

- Spam filtering (the providers already do it).
- Natural-language label names from a local LLM. Keyword-derived names first;
  an LLM namer can be added later if keyword names prove insufficient.
- Server-side rules (Sieve, Gmail filters). Labels are applied by mail-cat
  through the existing label path.
- Migrating data. Pre-1.0: schema changes mean a data-folder wipe + resync.

## Anti-goals (would make this a failure even if it works)

- Anything leaves the PC (remote embedding API, telemetry).
- Mail gets archived or labelled without the user ever having opted that label
  in. Nothing is auto-applied by default.
- The embedding model sits in RAM permanently, or the background work makes
  the machine sluggish while the user is working.
- Silent decisions: an auto-applied label must be visible and undoable from
  the message.

## Current state (what exists)

- **Labels are mailboxes.** A message's labels are its mailbox memberships
  (`mc-domain/src/model/message.rs`, `MailboxType::Custom`; per-account
  `MailboxModel` Folders|Labels in `model/account.rs`). Apply/remove goes
  through `handle_change_labels` / `apply_labels`
  (`mc-application/src/handlers/message/labels.rs`) → provider `change_labels`
  (JMAP, IMAP keywords, Gmail `X-GM-LABELS`). Auto-tagging reuses this path;
  there is no new "label" concept.
- **Ingest hook:** `ingest()` in
  `mc-application/src/services/sync_service/ingest.rs` emits `MessageReceived`
  with the envelope; bodies arrive either eagerly (`eager_body_fetch`) or later
  via backfill (`MessageBodyFetched`).
- **Text extraction:** Tantivy index already extracts body text incl.
  tag-stripped HTML (`body_of`, `strip_html_for_index` in
  `mc-infrastructure/src/search/tantivy_index.rs`). Reuse, don't duplicate.
- **Storage:** SQLite via rusqlite, schema in an in-code `MIGRATIONS` array
  (`persistence/sqlite/mod.rs`), edited in place pre-1.0.
- **No** ML, classifier, embedding or rules code; **no** parsing of
  `List-Unsubscribe` / `List-Id` / `Precedence` / `Auto-Submitted`.

## Design

Three classifiers feed one per-label score; a separate discovery job proposes
new labels. Delivered in three phases, each usable on its own.

### Signals

1. **List headers** — `List-Unsubscribe`, `List-Id`, `Precedence: bulk|list`,
   `Auto-Submitted`. Fetched with the envelope (IMAP header fetch, JMAP
   `header:` properties), stored on the message. Used as features (and as the
   strongest prior for a Newsletter-like label), not as a hard rule.
2. **Naive Bayes, one-vs-rest per label.** Tokens from sender address/domain,
   subject, body text (reusing the index extractor) and list-header presence.
   Token counts per label in SQLite, updated incrementally on every training
   example. Works from the first few examples, cheap enough to run at ingest on
   the envelope alone and again when the body arrives.
3. **Embeddings + nearest neighbours.** Small multilingual sentence-embedding
   model (mail is mostly Italian + English) run locally via ONNX. One vector
   per message (subject + start of body). Score for label L = weighted share
   of L among the k most similar labelled messages.

### Combining and deciding

- Per message and per label, a combined confidence in [0,1] from the
  available signals (Bayes alone until the body/vector exists).
- Per-label settings (new SQLite table):
  - `mode`: `off` | `suggest` | `apply` | `apply_archive`
  - `threshold`: confidence above which `apply`/`apply_archive` acts
    (below it, and in `suggest` mode, it is shown as a suggestion).
- Default for every label: `off` until the user enables it; labels born from
  discovery start in `suggest`.
- `apply_archive` = add label + remove from Inbox (existing archive semantics:
  drop INBOX membership).
- Each auto-applied label is recorded (message, label, score, when) so the UI
  can show "applied automatically" and undo it; undo counts as a negative
  example.

### Training data and feedback

- Positive examples: messages carrying label L (user-applied, or accepted
  suggestion/discovery).
- Negative examples: rejected suggestions, undone auto-applies, and messages
  without L from the same account (sampled; one-vs-rest).
- Initial training: on first run, train from all already-labelled mail.
- **Origin of label changes (user):** labels are synced with the provider in
  both directions, and changes made elsewhere (Gmail web, other clients) are
  real user decisions. So: a label added by sync is a positive; a label
  removed by sync is a negative. mail-cat records its own auto-applies, so a
  remote removal of an auto-applied label is recognised as an undo (strong
  negative), and mail-cat's own applies coming back through sync are not
  counted twice.

### Training groups (user)

Models are not per account but per **training group**:

- A group is a set of accounts that learn together. Labels are matched across
  the group's accounts **by name** (case-insensitive): "Newsletter" on Gmail
  and "Newsletter" on 4sigma share one Bayes model, one kNN pool and one
  per-label setting.
- Default: a single group ("Tutti") containing every account; **new accounts
  join it automatically**.
- Configurable: move an account to another group, create groups, or put an
  account in no group (it learns alone). E.g. two personal accounts in one
  group, two work accounts in another.
- Applying a label to a message in account A uses A's own label of that name;
  if A has no such label, it is created on A (remote too) the first time it
  is applied. *(Claude's call)*
- Discovery runs per group over the group's unlabelled mail; an accepted
  proposal creates the label on each account that has messages in the
  cluster. *(Claude's call)*
- Changing group membership retrains the affected groups from scratch (cheap
  for Bayes; vectors are per message and don't change).

### Applying to existing mail (user)

When a label is switched to `apply` / `apply_archive` (or a discovery is
accepted), the classifier also runs over existing mail of the group. Before
acting, the UI shows a preview ("312 messaggi verrebbero etichettati / 290
archiviati", with a sample) and asks for confirmation. Changes go through the
normal label path, so they reach the provider, and are recorded as
auto-applies so they can be undone. Batched and throttled, honouring the
existing provider throttling/backoff.

### Discovery (phase 3)

- Periodic background job over **unlabelled** messages that have a vector.
- Cluster the vectors (density-based, no fixed number of clusters, leaves
  outliers unassigned — e.g. HDBSCAN).
- Name each cluster from its distinctive terms (class-based TF-IDF over the
  cluster vs the rest, plus dominant sender domain) → proposed label name +
  keyword list.
- Propose clusters above a minimum size to the user: accept (optionally
  renamed) → create label, apply to the cluster's messages, train; reject →
  remember it so the same cluster isn't proposed again.

### When work runs

- **On ingest:** list headers + Bayes on the envelope (cheap).
- **On body available** (`MessageBodyFetched` or eager): Bayes re-score +
  embedding + kNN.
- **Backlog:** existing mail is embedded in background batches, alongside
  body backfill, throttled so it doesn't load the machine.
- **Model lifecycle:** embedding model loaded on demand, unloaded after a few
  minutes idle. Downloaded once on first use to the data folder.

### UI

- Message view / message row: suggestion chip "Newsletter?" with accept /
  reject; auto-applied labels marked as automatic with undo.
- Label settings (per label, in the folder bar label context menu and/or
  settings): mode + threshold.
- Discovery proposals: a list (settings page section or a panel) with name,
  keywords, size, sample messages; accept / rename / reject.

### Model abstraction (added 2026-10-31, user-approved)

Goal: swap models, or let users pick one that fits their hardware, without
touching the application layer.

**Domain ports (mc-domain)** — what the application needs, not how:

- `Embedder`
  - `model() -> ModelId` (id + revision; vectors of different ids are never
    compared — a model change means background re-embedding).
  - `dims()`.
  - `embed(texts, purpose)` with `purpose: Document | Query`. Each model owns
    its own handling (e5 `passage:`/`query:` prefixes, none for others), so
    the application never knows about prefixes. Phase-2 vectors were stored
    with the `query:` prefix; the first model id under this API differs, so
    they are re-embedded once.
  - parallelism/load/stats hooks as today.
- `LabelNamer` — given a cluster summary (distinctive keywords, dominant
  senders/domains, sample subjects) returns a proposed name. Phase 3 ships a
  keyword-based implementation; a local-LLM implementation can replace it.
- A generic text-generation port (summaries, extraction, LLM naming) is
  **not** defined now; it is added when first used (user decision).

**Model catalog (mc-infrastructure)** — the menu users choose from. Each
entry: id, revision, files + SHA-256, dims, max tokens, document/query
handling, download size, RAM per worker, quality tier, languages. Profiles:

| Profile | Example model | Size |
|---|---|---|
| Leggero | multilingual-e5-small (today) | ~120 MB |
| Bilanciato | multilingual-e5-base | ~280 MB |
| Qualità | bge-m3 or multilingual-e5-large | ~600 MB+ |

Exact models are the builder's call; each must cover Italian + English.

**Execution (mc-infrastructure, not domain)** — `ExecutionTarget`:
`Cpu` | `Gpu(Cuda | Rocm | DirectMl | CoreMl)` | `Npu(VitisAi)`. Only `Cpu` is
implemented now; the others are declared with a clear "not available on this
build/machine" error. Speed (workers × threads, low OS priority on dedicated
threads) belongs to this layer too.

**User settings** — model profile, execution target, background speed, plus
the live stats/progress card. Changing model → new `ModelId` → background
re-embedding with the progress bar; until vectors exist the kNN signal is
simply absent (Bayes alone, already the no-vector behaviour).

## Phases

1. **Headers + Bayes + per-label settings + suggestion UI.** Delivers
   Newsletter/promotion suggestions and auto-apply.
2. **Local embeddings + kNN signal + background embedding of the backlog.**
2b. **Model abstraction** (section above) + background speed/stats.
3. **Discovery: clustering, keyword naming (`LabelNamer`), proposals,
   feedback into 1–2.**

## Constraints

- Local only; no network except the one-time model download.
- Hexagonal layering: classifier/embedder behind domain ports, implementations
  in `mc-infrastructure`, orchestration in `mc-application`, wiring in
  `mc-server`.
- Pre-1.0: schema changes edited in place, data folder wiped; tell the user
  what changed.
- Files 400–600 lines; Rust tests in `foo/tests.rs`.

## Decided

- Local embeddings only (user).
- Action configurable per label: suggest / apply / apply+archive (user).
- Continuous background operation (user).
- Discovery via clustering + keyword naming; accepted discoveries train the
  Bayes classifier (user).
- Training groups of accounts, default one group with all accounts, new
  accounts join it, configurable (user).
- Label changes synced from the provider count as training signal (user).
- Enabling auto-apply also applies to existing mail, after a preview and
  confirmation (user).
- Keyword-derived names first, local LLM namer deferred (Claude's call,
  user did not object).

## Claude's calls (settled after builder check; user may override)

- **Minimum examples:** a label can leave `off` only with ≥ 5 positives;
  below that the UI says "servono altri esempi".
- **Negatives:** explicit negatives (rejected suggestion, undone auto-apply)
  weigh fully; unlabelled mail is only a weak negative (down-weighted,
  sampled, recency-biased). Exact ratio left to the builder.
- **Confidence:** a heuristic blend, not a calibrated probability. The UI
  shows it as a level (alta / media), not a number. Calibration on
  accept/reject history can come later if thresholds prove unreliable.
- **Resources:** model unloaded after 5 min idle; backlog embedding in small
  batches with pauses, lowest priority. No battery/activity detection in v1.
- **Model shipping:** downloaded at runtime on first use of phase 2 (pinned URL
  + checksum), after a one-line consent in the UI; ONNX runtime bundled with
  the app. Linux first; macOS/Windows must build but aren't tested in v1.
- **Evaluation:** an offline CLI command (`mc labels eval --account …`) that
  holds out part of the labelled mail and prints precision/recall per label
  and per threshold. Used to pick the default threshold; no hard precision
  floor.
- **Existing mail and headers:** pre-1.0 policy applies — schema change,
  data wipe, resync. No lazy header backfill.
- **UI placement:** built on top of the new double sidebar (folder bar label
  context menu, message row chips), which lands first.

## Left to the builder

- Embedding runtime and model choice (e.g. `fastembed`/`ort` with a small
  multilingual model ~100 MB); must handle Italian.
- Vector storage: SQLite BLOBs + brute-force cosine is expected to be enough
  at tens of thousands of messages per account; add an ANN index only if
  measured slow.
- Bayes details (tokenizer, smoothing, priors), score combination weights,
  default threshold, k for kNN, clustering parameters and minimum cluster size.
- Exact placement of settings and proposals in the UI.
