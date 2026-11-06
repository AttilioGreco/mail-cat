# Roadmap

Work agreed but not scheduled. Newest decisions first within each section.

## Attachments

Names, types and sizes are stored and searchable (`filename:`). Still to do:

1. **Download setting.** An option per account to download attachment content
   in the background. Attachments come last in the download queue: after
   envelopes and text bodies, and inside the same daily byte budget
   (`daily_download_mb`). Off by default; on-demand download when opened
   stays as it is.
2. **Text extraction for search.** Turn downloaded attachments into text and
   index it with the message (PDF, DOCX/ODT, XLSX/ODS, plain text, HTML). A
   separate pipeline: extraction can be slow or fail, so it runs niced in the
   background, records per-attachment state and never blocks sync. Needs a
   choice of libraries (pure Rust where possible) and limits on size and time
   per file.
3. **Search and UI.** `has:pdf`-style shortcuts, attachment text in result
   snippets, a list of a message's attachments with download/open.

## Labels and classification

- Measure precision per label with `mc labels eval` before widening `apply`;
  re-tune the kNN floor (0.8, set on 5 sentences; e5 similarities sit in
  0.8–0.95).
- Weigh sender and List-Id more in the classifier; merge discovery clusters
  that share a dominant sender (Twitch came out as 12 clusters, GitHub as 11).
- Accepting/rejecting suggestions in the UI as training signal.
- Discovery grouping into categories in the app.
- MCP server over the same gRPC operations, for assistants.

## Search

- Semantic search: hybrid Tantivy + vectors (documents are embedded with the
  "passage:" prefix, queries need "query:").
- `from:` on a domain is a phrase over tokens, so look-alike domains such as
  `evil-facile.it` also match; make domain matching exact.
- Reindex the messages of a deleted mailbox (they keep the stale label until
  their next change or a rebuild).

## Accounts and sync

- Detach the per-account execution context from the server (shape to decide).
- Custom per-account icon (upload or URL) for the rail.
- IMAP servers whose hierarchy delimiter is `.` are not translated.
- Windows: ORT worker threads are not niced.

## Tests

- The Gmail integration scenarios other than `gmail_rename_tree` expect Spam
  and Trash to be synced, but the test account keeps them out of LSUB and
  mail-cat follows the server's subscription since 0b17d73: subscribe them in
  the harness.
