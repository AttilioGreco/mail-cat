# Search

One query language for the search box, `mc messages search` and the gRPC `Search` RPC. It follows Gmail's: words are ANDed, `OR` joins two terms, a leading `-` excludes, `"quotes"` keep a phrase together and `( )` group. `OR` binds tighter than the implicit AND, so `a b OR c` means `a AND (b OR c)`.

| Operator | Matches | Example |
|---|---|---|
| `from:` | sender address or name; a domain matches its subdomains | `from:facile.it` |
| `to:` `cc:` | recipient addresses | `to:anna@example.com` |
| `subject:` (`sbj:`) | subject | `subject:fattura` |
| `label:` | label, case-insensitive; a parent matches its descendants | `label:Dev` finds `Dev/GitHub` |
| `in:` | `inbox` `sent` `trash` `spam` `drafts` `archive`, or a mailbox path | `in:inbox`, `in:"Clienti/Rossi"` |
| `account:` | account name, email or id | `account:lavoro` |
| `is:` | `unread` `read` `starred` `answered` | `is:unread` |
| `has:attachment` | has an attachment | |
| `filename:` | attachment file name | `filename:fattura` |
| `after:` `before:` | `YYYY-MM-DD`, UTC; `after` includes the day, `before` excludes it | `after:2026-01-01 before:2026-02-01` |
| `newer_than:` `older_than:` | `Nd`, `Nm` or `Ny` back from now | `newer_than:7d` |
| `list:` | `List-Id` of a mailing list | `list:github` |

Quote values with spaces: `label:"Newsletter/The Space Cinema"`. Any other `word:text`, or an operator with a value it does not know (`is:maybe`), is searched as plain text. A malformed date is an error.

## Labels

`label:` covers the custom mailboxes of label-model accounts (Gmail, JMAP) and the keywords of folder-model accounts. Keys are the lowercased path, as in label curation (`docs/mc-labels-cli.md`), and every parent path is indexed too.

## Examples

```
label:Dev -is:read after:2026-01-01
from:facile.it (fattura OR invoice) has:attachment
in:inbox older_than:1y
account:lavoro "weekly report" -from:me@example.com
```

Results with free text are ranked by relevance; a query of filters only lists the newest first.

## Index

The index is rebuilt from the read model when its schema changes (first start after an upgrade): it runs in the background, logs `search index rebuild progress`, and searches return partial results until `search index rebuild complete`. Reindexing a message follows `MessageReceived`, `MessageBodyFetched`, `MessageFlagged`, `MessageMailboxesChanged`, `MailboxRenamed` and `MailboxTypeChanged`.
