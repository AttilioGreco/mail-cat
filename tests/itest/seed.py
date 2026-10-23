#!/usr/bin/env python3
"""APPEND N deterministic messages to the itest server, round-robin over the
given mailboxes (default: INBOX 3 of every 5, Archive and Projects 1 each).

Message i has the subject tag [itest-iii]; every 4th replies to the previous
one (In-Reply-To/References), every 7th has no Message-ID, every 5th carries
an attachment. Flags: i%3==0 \\Seen, i%5==0 \\Flagged, i%8==0 \\Answered.
"""
import argparse
import imaplib
import time
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import format_datetime, make_msgid

import imapctl

SENDERS = ["Carol Rossi <carol@example.net>", "Dan Bianchi <dan@example.org>",
           "bob@itest.local", "Newsletter <news@lists.example.com>"]
TOPICS = ["Quarterly report", "Lunch on Friday", "Server migration", "Invoice 2026-10",
          "Holiday plans", "Code review", "Release notes"]


def build(i, prev_msgid, now):
    msg = EmailMessage()
    reply = i % 4 == 0 and prev_msgid
    topic = TOPICS[(i - 1 if reply else i) % len(TOPICS)]
    msg["Subject"] = f"{'Re: ' if reply else ''}{topic} [itest-{i:03}]"
    msg["From"] = SENDERS[i % len(SENDERS)]
    msg["To"] = "Alice <alice@itest.local>"
    when = now - (100 - i) * 3600  # one an hour, oldest first
    msg["Date"] = format_datetime(datetime.fromtimestamp(when, timezone.utc))
    msgid = None
    if i % 7:
        msgid = make_msgid(f"itest{i}", "example.net")
        msg["Message-ID"] = msgid
    if reply:
        msg["In-Reply-To"] = prev_msgid
        msg["References"] = prev_msgid
    msg.set_content(f"Body of message {i}.\n\nSome text so it is not empty.\n")
    if i % 5 == 0:
        msg.add_attachment(f"attachment of message {i}\n".encode() * 20, maintype="application",
                           subtype="octet-stream", filename=f"report-{i}.bin")
    flags = [f for f, on in ((r"\Seen", i % 3 == 0), (r"\Flagged", i % 5 == 0),
                             (r"\Answered", i % 8 == 0)) if on]
    return msg, msgid, flags, when


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("-n", type=int, default=20)
    p.add_argument("--user", default="alice", choices=imapctl.USERS)
    p.add_argument("--start", type=int, default=1, help="first tag number")
    p.add_argument("mailboxes", nargs="*", default=["INBOX", "INBOX", "Archive", "INBOX", "Projects"])
    a = p.parse_args()
    m = imapctl.connect(a.user)
    now, prev = int(time.time()), None
    for i in range(a.start, a.start + a.n):
        msg, msgid, flags, when = build(i, prev, now)
        prev = msgid or prev
        box = a.mailboxes[(i - a.start) % len(a.mailboxes)]
        imapctl.check(m.append(imapctl.quote(box), f"({' '.join(flags)})",
                               imaplib.Time2Internaldate(when), msg.as_bytes()))
    m.logout()
    print(f"seeded {a.n} messages into {', '.join(dict.fromkeys(a.mailboxes))}")


if __name__ == "__main__":
    main()
