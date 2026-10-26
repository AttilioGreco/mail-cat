#!/usr/bin/env python3
"""Deterministic benchmark corpus APPENDed straight to Stalwart (alice).

~3000 messages: INBOX 1500, Archive 1000, Projects 500. Text bodies 1-30 KB,
30% multipart/alternative with HTML, 20% with 1-3 attachments of 100 KB-5 MB
(random bytes, log-uniform sizes), reply chains (some In-Reply-To only).
Prints and saves byte totals to .state/bench/corpus.json.
"""
import argparse
import imaplib
import json
import random
import time
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import format_datetime, make_msgid

import imapctl

LAYOUT = (("INBOX", 0.5), ("Archive", 1 / 3), ("Projects", 1 / 6))
WORDS = ("report invoice meeting server release review budget lunch deploy migration quarterly "
         "customer contract holiday schedule update patch ticket roadmap cluster backup network "
         "alice bob carol dan please thanks regards attached forward summary status").split()
SENDERS = [("Carol Rossi", "carol@example.net"), ("Dan Bianchi", "dan@example.org"),
           ("Bob", "bob@itest.local"), ("Newsletter", "news@lists.example.com"),
           ("Eve Verdi", "eve@example.com"), ("Build Bot", "ci@build.example.org")]


def text(rng, size):
    out, n = [], 0
    while n < size:
        line = " ".join(rng.choices(WORDS, k=rng.randint(6, 14))).capitalize() + ".\n"
        out.append(line)
        n += len(line)
    return "".join(out)


def build(rng, i, when, parent, stats):
    msg = EmailMessage()
    name, addr = rng.choice(SENDERS)
    subject = " ".join(rng.choices(WORDS, k=rng.randint(2, 6))).capitalize()
    msgid = make_msgid(f"b{i}", "example.net")
    if parent:
        subject = "Re: " + parent[1]
        msg["In-Reply-To"] = parent[0]
        if rng.random() < 0.75:
            msg["References"] = parent[2] + " " + parent[0]
    msg["Subject"] = subject
    msg["From"] = f"{name} <{addr}>"
    msg["To"] = "Alice <alice@itest.local>"
    if rng.random() < 0.3:
        msg["Cc"] = "Dan Bianchi <dan@example.org>"
    msg["Date"] = format_datetime(datetime.fromtimestamp(when, timezone.utc))
    msg["Message-ID"] = msgid
    if rng.random() < 0.15:
        msg["List-Id"] = "<list.example.com>"
    body = text(rng, rng.randint(1_000, 30_000))
    msg.set_content(body)
    stats["text_plain"] += len(body)
    if rng.random() < 0.3:
        html = "<html><body>" + "".join(f"<p>{l}</p>\n" for l in body.splitlines()) + "</body></html>"
        msg.add_alternative(html, subtype="html")
        stats["text_html"] += len(html)
    if rng.random() < 0.2:
        for k in range(rng.randint(1, 3)):
            size = int(100_000 * 50 ** rng.random())
            msg.add_attachment(rng.randbytes(size), maintype="application",
                               subtype="octet-stream", filename=f"file-{i}-{k}.bin")
            stats["attachments"] += size
            stats["attachment_count"] += 1
    flags = [f for f, p in ((r"\Seen", 0.6), (r"\Flagged", 0.1), (r"\Answered", 0.15)) if rng.random() < p]
    return msg, (msgid, subject.removeprefix("Re: "), parent[2] + " " + msgid if parent else msgid), flags


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("-n", type=int, default=3000)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    rng = random.Random(a.seed)
    stats = dict(messages=0, total=0, text_plain=0, text_html=0, attachments=0, attachment_count=0)
    now = int(time.time())
    pool = []  # recent (msgid, subject, references) chain heads to reply to
    m = imapctl.connect()
    t0 = time.time()
    for box, share in LAYOUT:
        count = round(a.n * share)
        whens = sorted(now - rng.randint(3600, 700 * 86400) for _ in range(count))
        for i, when in enumerate(whens):
            parent = rng.choice(pool[-200:]) if pool and rng.random() < 0.4 else None
            msg, node, flags = build(rng, stats["messages"], when, parent, stats)
            pool.append(node)
            raw = msg.as_bytes()
            imapctl.check(m.append(imapctl.quote(box), f"({' '.join(flags)})",
                                   imaplib.Time2Internaldate(when), raw))
            stats["messages"] += 1
            stats["total"] += len(raw)
        print(f"{box}: {count} appended ({time.time() - t0:.0f}s)", flush=True)
    m.logout()
    stats["text"] = stats["text_plain"] + stats["text_html"]
    out = imapctl.HERE / ".state" / "bench"
    out.mkdir(parents=True, exist_ok=True)
    (out / "corpus.json").write_text(json.dumps(stats, indent=1))
    print(json.dumps(stats))


if __name__ == "__main__":
    main()
