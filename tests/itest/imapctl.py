#!/usr/bin/env python3
"""The "other client" (a phone, Thunderbird...): acts on the itest Stalwart
directly over IMAP, behind mail-cat's back. Also the shared library for
seed.py and scenarios.py.

A message selector is a UID or `tag:N`, the [itest-NNN] tag seed.py puts in
every subject (e.g. `tag:7`).
"""
import argparse
import email
import email.policy
import imaplib
import re
import ssl
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CA = HERE / ".state" / "certs" / "ca.pem"
HOST, PORT = "localhost", 19993
USERS = {
    "alice": ("alice@itest.local", "alice-itest-horse-battery"),
    "bob": ("bob@itest.local", "bob-itest-horse-battery"),
}
# Stalwart provisions "Deleted Items", "Sent Items"...: renamed to the common
# names (special-use kept), the missing ones created. Projects is the custom one.
STANDARD_MAILBOXES = {"Archive": r"\Archive", "Trash": r"\Trash", "Sent": r"\Sent",
                      "Drafts": r"\Drafts", "Junk": r"\Junk", "Projects": None}


def connect(user="alice"):
    m = imaplib.IMAP4_SSL(HOST, PORT, ssl_context=ssl.create_default_context(cafile=CA))
    m.login(*USERS[user])
    return m


def check(resp):
    typ, data = resp
    if typ != "OK":
        raise RuntimeError(f"IMAP {typ}: {data}")
    return data


def quote(name):
    return '"' + name.replace("\\", "\\\\").replace('"', '\\"') + '"'


def mailboxes(m):
    """{name: [attributes]}"""
    out = {}
    for line in check(m.list()):
        # (\HasNoChildren \Trash) "/" "Deleted Items"
        match = re.match(rb'\((.*?)\) (?:"[^"]*"|NIL) (.+)$', line)
        name = match.group(2).decode()
        out[name[1:-1] if name.startswith('"') else name] = match.group(1).decode().split()
    return out


def messages(m, mailbox):
    """[{uid, flags, msgid, subject}] for every message in `mailbox`."""
    if check(m.select(quote(mailbox), readonly=True))[0] == b"0":
        return []
    data = check(m.uid("FETCH", "1:*", "(UID FLAGS BODY.PEEK[HEADER.FIELDS (MESSAGE-ID SUBJECT)])"))
    out = []
    for item in data:
        if not isinstance(item, tuple):
            continue
        meta, raw = item
        hdr = email.message_from_bytes(raw, policy=email.policy.default)
        out.append({
            "uid": int(re.search(rb"UID (\d+)", meta).group(1)),
            "flags": re.search(rb"FLAGS \((.*?)\)", meta).group(1).decode().split(),
            "msgid": hdr.get("Message-ID", "") or "",
            "subject": str(hdr.get("Subject", "")),
        })
    return out


def tag_of(subject):
    match = re.search(r"\[itest-(\d+)\]", subject)
    return int(match.group(1)) if match else None


def resolve(m, mailbox, selector):
    """UID for a selector in `mailbox`, or exit if it matches nothing."""
    if selector.startswith("tag:"):
        want = int(selector[4:])
        uids = [x["uid"] for x in messages(m, mailbox) if tag_of(x["subject"]) == want]
        if len(uids) != 1:
            sys.exit(f"{selector} matches {len(uids)} messages in {mailbox}")
        return str(uids[0])
    return selector


def select_rw(m, mailbox):
    check(m.select(quote(mailbox)))


def cmd_caps(m, a):
    print(" ".join(m.capabilities))


def cmd_mailboxes(m, a):
    for name in mailboxes(m):
        print(check(m.status(quote(name), "(MESSAGES UIDNEXT UIDVALIDITY)"))[0].decode())


def cmd_ensure_mailboxes(m, a):
    have = mailboxes(m)
    for name, use in STANDARD_MAILBOXES.items():
        current = next((n for n, attrs in have.items() if use in attrs), None) if use else None
        if current and current != name:
            check(m.rename(quote(current), quote(name)))
        elif name not in have:
            check(m._simple_command("CREATE", quote(name), f"(USE ({use}))") if use else m.create(quote(name)))
        m.subscribe(quote(name))  # NO if already subscribed: fine


def lsub(m):
    """Names the server lists as subscribed."""
    out = set()
    for line in check(m.lsub()):
        match = re.match(rb'\((.*?)\) (?:"[^"]*"|NIL) (.+)$', line)
        name = match.group(2).decode()
        out.add(name[1:-1] if name.startswith('"') else name)
    return out


def cmd_lsub(m, a):
    print("\n".join(sorted(lsub(m))))


def cmd_subscribe(m, a):
    check(m.subscribe(quote(a.mailbox)))


def cmd_unsubscribe(m, a):
    check(m.unsubscribe(quote(a.mailbox)))


def cmd_create(m, a):
    check(m.create(quote(a.mailbox)))


def cmd_list(m, a):
    for x in messages(m, a.mailbox):
        print(f'{x["uid"]:>5}  {" ".join(x["flags"]):<28} {x["msgid"] or "-":<44} {x["subject"]}')


def cmd_move(m, a):
    uid = resolve(m, a.mailbox, a.msg)
    select_rw(m, a.mailbox)
    check(m.uid("MOVE", uid, quote(a.dest)))


def cmd_copy(m, a):
    uid = resolve(m, a.mailbox, a.msg)
    select_rw(m, a.mailbox)
    check(m.uid("COPY", uid, quote(a.dest)))


def cmd_store(m, a):
    uid = resolve(m, a.mailbox, a.msg)
    select_rw(m, a.mailbox)
    for f in a.flags:  # +\Seen  -\Flagged
        op = "+FLAGS" if f[0] == "+" else "-FLAGS"
        check(m.uid("STORE", uid, op, f"({f[1:]})"))


def cmd_delete(m, a):
    """\\Deleted + UID EXPUNGE of just that message, like a phone does."""
    uid = resolve(m, a.mailbox, a.msg)
    select_rw(m, a.mailbox)
    check(m.uid("STORE", uid, "+FLAGS", r"(\Deleted)"))
    check(m.uid("EXPUNGE", uid))


def cmd_expunge(m, a):
    select_rw(m, a.mailbox)
    check(m.expunge())


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--user", default="alice", choices=USERS)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("caps", help="CAPABILITY dump")
    sub.add_parser("mailboxes", help="mailboxes with MESSAGES/UIDNEXT/UIDVALIDITY")
    sub.add_parser("ensure-mailboxes", help="create " + ", ".join(STANDARD_MAILBOXES) + " if missing")
    sub.add_parser("lsub", help="subscribed mailboxes")
    sub.add_parser("subscribe").add_argument("mailbox")
    sub.add_parser("unsubscribe").add_argument("mailbox")
    sub.add_parser("create").add_argument("mailbox")
    sub.add_parser("list", help="UID, flags, Message-ID, subject").add_argument("mailbox")
    sub.add_parser("expunge").add_argument("mailbox")
    for name in ("move", "copy"):
        s = sub.add_parser(name)
        s.add_argument("mailbox"), s.add_argument("msg"), s.add_argument("dest")
    s = sub.add_parser("store", help=r"store MAILBOX MSG +\Seen -\Flagged ...")
    s.add_argument("mailbox"), s.add_argument("msg"), s.add_argument("flags", nargs="+")
    s = sub.add_parser("delete", help="flag \\Deleted and UID EXPUNGE one message")
    s.add_argument("mailbox"), s.add_argument("msg")
    a = p.parse_args(argv)
    m = connect(a.user)
    try:
        globals()["cmd_" + a.cmd.replace("-", "_")](m, a)
    finally:
        m.logout()


if __name__ == "__main__":
    main()
