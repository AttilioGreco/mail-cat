#!/usr/bin/env python3
"""Reset the sandbox of the Gmail test mailbox to a known fixture (idempotent).

Only messages carrying the label mc-itest and labels named mc-itest/... are
deleted or created (api.Gmail.mutate enforces it); other mail is never touched.
"""
import os
import sys
from email.message import EmailMessage
from email.utils import format_datetime
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from gmail import api  # noqa: E402

ID_PREFIX = "mcitest-"

# ponytail: names Gmail may reject (quotes, backslash, case twin) are tried and skipped if refused.
LABELS = ["Clienti/Café", "A/B/C", "Presentations", "Old Trash", "Inbox Zero", "work", "Work",
          'Say "hi"', "back\\slash"]

# (n, subject, label names: a LABELS entry or a Gmail system id); every message also gets the sandbox label
MESSAGES = [
    (1, "inbox only", ["INBOX", "UNREAD"]),
    (2, "multi label", ["INBOX", "Clienti/Café", "A/B/C"]),
    (3, "archived", ["Presentations"]),
    (4, "archived bare", []),
    (5, "starred", ["INBOX", "STARRED"]),
    (6, "important", ["INBOX", "IMPORTANT"]),
    (7, "draft", ["DRAFT"]),
    (8, "spam", ["SPAM"]),
    (9, "trash", ["TRASH"]),
    (10, "inbox zero", ["INBOX", "Inbox Zero", "Old Trash"]),
    (11, "case twin", ["work"]),
    (12, "nasty", ['Say "hi"', "back\\slash"]),
]


def message_id(n):
    return f"{ID_PREFIX}{n:03d}@mc-itest.local"


def label(name):
    return api.PREFIX + name


def rfc822(n, subject):
    m = EmailMessage()
    m["From"], m["To"] = "sender@mc-itest.local", "me@mc-itest.local"
    m["Subject"] = f"[{ID_PREFIX}{n:03d}] {subject}"
    m["Message-ID"] = f"<{message_id(n)}>"
    m["Date"] = format_datetime(datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=n))
    m.set_content(f"Body of message {n}.\n")
    return m.as_bytes()


def reset(g=None):
    """Delete the sandbox's messages and labels, then reseed. Returns {full label name: id}."""
    g = g or api.Gmail()
    prof = g.profile()
    api.check_address(g.confirm, prof["emailAddress"])
    labels = g.labels()
    sandbox = next((l["id"] for l in labels if l["name"] == api.SANDBOX), None)
    old = g.message_ids(labelIds=[sandbox]) if sandbox else []
    print(f"gmail seed: {prof['emailAddress']} ({prof['messagesTotal']} messages); "
          f"resetting {len(old)} under {api.SANDBOX!r}")
    g.batch_delete(old)
    for l in labels:
        if l["name"].startswith(api.PREFIX):
            g.label_delete(l["id"])
    sandbox = sandbox or g.label_create(api.SANDBOX)
    ids = {}
    for name in LABELS:
        try:
            ids[label(name)] = g.label_create(label(name))
        except api.GmailError as e:
            print(f"gmail seed: label {name!r} refused by Gmail, skipped ({str(e)[-80:]})")
    system = {n for _, _, ls in MESSAGES for n in ls if n.isupper()}
    for n, subject, names in MESSAGES:
        wanted = [ids.get(label(x), x) for x in names if label(x) in ids or x in system]
        g.insert(rfc822(n, subject), [sandbox] + wanted)
    return ids


if __name__ == "__main__":
    reset()
