#!/usr/bin/env python3
"""End-to-end scenarios against the itest Stalwart + isolated mail-cat-server.

Each scenario starts from reset.sh + seed.py + `mc sync`, acts through mc-cli
and/or imapctl, then asserts that mail-cat and the server agree. Exits 1 if
any scenario fails. Usage: scenarios.py [name ...]   (default: all)

Seed layout (seed.py defaults, tag = [itest-NNN] in the subject):
  INBOX 1 2 4 6 7 9 11 12 14 16 17 19 | Archive 3 8 13 18 | Projects 5 10 15 20
"""
import imaplib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from email.message import EmailMessage

import gmail_scenarios
import imapctl

HERE = imapctl.HERE
ROOT = HERE.parent.parent
LOGS = HERE / ".logs"
TARGET = os.environ.get("CARGO_TARGET_DIR", ROOT / "target")
MC = [os.path.join(TARGET, "debug/mc"), "--server", "http://127.0.0.1:50061"]
ENV = {**os.environ, "NO_COLOR": "1"}


class Fail(Exception):
    pass


def run(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True, env=ENV)
    if p.returncode:
        raise Fail(f"`{' '.join(cmd)}` exited {p.returncode}:\n{p.stdout}{p.stderr}")
    return p.stdout


def mc(*args):
    return run(MC + list(args))


def mc_refuses(*args):
    """The refusal text of a `mc` call that must fail."""
    p = subprocess.run(MC + list(args), capture_output=True, text=True, env=ENV)
    if p.returncode == 0:
        raise Fail(f"`mc {' '.join(args)}` succeeded, expected a refusal")
    return p.stdout + p.stderr


def table(out):
    """Rows of a tabled `+---+` table as dicts keyed by header."""
    rows = [[c.strip() for c in line.strip()[1:-1].split("|")]
            for line in out.splitlines() if line.startswith("|")]
    return [dict(zip(rows[0], r)) for r in rows[1:]] if rows else []


def account_id(name):
    return next(a["id"] for a in json.loads(mc("--json", "accounts", "list")) if a["name"] == name)


class World:
    def __init__(self):
        self.account = account_id("alice")
        self.boxes = {r["Path"]: r for r in table(mc("mailboxes", "list", self.account))}

    def sync(self):
        out = mc("sync", self.account)
        if "Error:" in out:
            raise Fail(f"sync reported an error:\n{out}")

    # ── mail-cat side ────────────────────────────────────────────────────
    def mc_list(self, path):
        return [{"id": r["ID"], "tag": imapctl.tag_of(r["Subject"]), "seen": r["R"] != "●"}
                for r in table(mc("messages", "list", "-v", self.boxes[path]["ID"]))]

    def mc_id(self, path, tag):
        ids = [m["id"] for m in self.mc_list(path) if m["tag"] == tag]
        if len(ids) != 1:
            raise Fail(f"mail-cat has {len(ids)} rows for tag {tag} in {path}")
        return ids[0]

    # ── server side ──────────────────────────────────────────────────────
    def srv_list(self, path):
        m = imapctl.connect()
        try:
            return [{"uid": x["uid"], "tag": imapctl.tag_of(x["subject"]),
                     "seen": r"\Seen" in x["flags"], "flags": x["flags"]}
                    for x in imapctl.messages(m, path) if r"\Deleted" not in x["flags"]]
        finally:
            m.logout()

    def srv_flags(self, path, tag):
        return next(x["flags"] for x in self.srv_list(path) if x["tag"] == tag)

    def where(self, tag):
        """{path: (copies on server, copies in mail-cat)} for one tag."""
        out = {}
        for path in self.boxes:
            s = sum(x["tag"] == tag for x in self.srv_list(path))
            c = sum(x["tag"] == tag for x in self.mc_list(path))
            if s or c:
                out[path] = (s, c)
        return out

    # ── assertions ───────────────────────────────────────────────────────
    def assert_consistent(self):
        """Every mailbox: same (tag, seen) multiset on both sides, and the
        mailbox Total matches the server count. Catches dups, ghosts, flag drift."""
        boxes = {r["Path"]: r for r in table(mc("mailboxes", "list", self.account))}
        problems = []
        for path in self.boxes:
            srv = Counter((x["tag"], x["seen"]) for x in self.srv_list(path))
            loc = Counter((x["tag"], x["seen"]) for x in self.mc_list(path))
            if srv != loc:
                problems.append(f"{path}: only on server {dict(srv - loc)}, "
                                f"only in mail-cat {dict(loc - srv)}  ((tag, seen): n)")
            if int(boxes[path]["Total"]) != sum(srv.values()):
                problems.append(f"{path}: mailbox Total {boxes[path]['Total']} != server {sum(srv.values())}")
        if problems:
            raise Fail("mail-cat and server disagree:\n  " + "\n  ".join(problems))

    def assert_where(self, tag, expected):
        got = self.where(tag)
        if got != expected:
            raise Fail(f"tag {tag}: expected {{path: (server, mail-cat)}} {expected}, got {got}")


def wait(cond, what, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.2)
    raise Fail(f"timed out after {timeout}s waiting for: {what}")


def idle_syncs():
    """Partial (single-mailbox) syncs the server has finished so far."""
    return (HERE / ".state" / "server.log").read_text(errors="replace").count("sync_mailbox: done")


def on_server(w, tag, path):
    return any(x["tag"] == tag for x in w.srv_list(path))


# ── scenarios ────────────────────────────────────────────────────────────

def basic_sync(w):
    """Counts, tags and read state per mailbox match the server after a sync."""
    w.assert_consistent()
    total = sum(len(w.mc_list(p)) for p in w.boxes)
    if total != 20:
        raise Fail(f"mail-cat holds {total} messages, seeded 20")


def flags(w):
    """mark-read/star via mc-cli land on the server, and so do their inverses."""
    mid = w.mc_id("INBOX", 1)
    mc("messages", "mark-read", mid)
    mc("messages", "star", mid)
    wait(lambda: {r"\Seen", r"\Flagged"} <= set(w.srv_flags("INBOX", 1)), r"\Seen \Flagged on server")
    mc("messages", "mark-unread", mid)
    mc("messages", "unstar", mid)
    wait(lambda: not {r"\Seen", r"\Flagged"} & set(w.srv_flags("INBOX", 1)), r"\Seen \Flagged cleared on server")
    w.sync()
    w.assert_consistent()


def move_delete(w):
    """mc move to Archive, delete (to Trash) and permanent delete: server and
    mail-cat agree afterwards, one copy each, no ghost left in INBOX."""
    mc("messages", "move", w.mc_id("INBOX", 2), "--mailbox", w.boxes["INBOX"]["ID"], w.boxes["Archive"]["ID"])
    mc("messages", "delete", w.mc_id("INBOX", 4), "--mailbox", w.boxes["INBOX"]["ID"])
    mc("messages", "delete", "--permanent", w.mc_id("INBOX", 6), "--mailbox", w.boxes["INBOX"]["ID"])
    wait(lambda: on_server(w, 2, "Archive") and not on_server(w, 2, "INBOX"), "tag 2 moved on server")
    wait(lambda: on_server(w, 4, "Trash") and not on_server(w, 4, "INBOX"), "tag 4 in Trash on server")
    wait(lambda: not on_server(w, 6, "INBOX"), "tag 6 expunged on server")
    w.sync()
    w.assert_where(2, {"Archive": (1, 1)})
    w.assert_where(4, {"Trash": (1, 1)})
    w.assert_where(6, {})
    w.assert_consistent()


def phone_moved_first(w):
    """Another client moves M INBOX→Archive; mail-cat, not yet reconciled,
    moves its stale INBOX row to Archive too. One copy, in Archive, both sides.
    (IDLE's partial sync doesn't reconcile, so the stale INBOX row survives
    until a full sync.)"""
    stale = w.mc_id("INBOX", 7)
    seen = idle_syncs()
    imapctl.main(["move", "INBOX", "tag:7", "Archive"])
    # the INBOX IDLE watcher reacts, as it would in real life
    wait(lambda: idle_syncs() > seen, "the INBOX IDLE watcher to run its partial sync")
    try:
        mc("messages", "move", stale, "--mailbox", w.boxes["INBOX"]["ID"], w.boxes["Archive"]["ID"])
    except Fail as e:
        print("    (mc move of the stale row failed, tolerated: " + " ".join(str(e).splitlines()[1:]) + ")")
    wait(lambda: on_server(w, 7, "Archive") and not on_server(w, 7, "INBOX"),
         "the background IMAP move of tag 7 to land on the server")
    w.sync()
    w.assert_where(7, {"Archive": (1, 1)})
    w.assert_consistent()


def remote_delete(w):
    """Another client flags \\Deleted + expunges; the next sync drops the row."""
    w.mc_id("INBOX", 9)
    imapctl.main(["delete", "INBOX", "tag:9"])
    w.sync()
    w.assert_where(9, {})
    w.assert_consistent()


def reappear(w):
    """mail-cat moves M INBOX→Archive, then another client copies it back
    into INBOX. The copy must be ingested (RFC 3501: a COPY always gets a UID
    >= UIDNEXT, so this exercises the cursor/reconcile boundary right
    after mail-cat's own move, not a UID strictly below the stored one)."""
    mc("messages", "move", w.mc_id("INBOX", 11), "--mailbox", w.boxes["INBOX"]["ID"], w.boxes["Archive"]["ID"])
    wait(lambda: on_server(w, 11, "Archive") and not on_server(w, 11, "INBOX"), "tag 11 moved on server")
    w.sync()
    w.assert_where(11, {"Archive": (1, 1)})
    imapctl.main(["copy", "Archive", "tag:11", "INBOX"])
    w.sync()
    w.assert_where(11, {"Archive": (1, 1), "INBOX": (1, 1)})
    w.assert_consistent()


def delete_twice(w):
    """Two deletes of the same message at once (double click): the second,
    asked while it was still in INBOX, must not purge it from Trash. It may
    fail as not found (the message left INBOX before its turn); it ends in
    Trash, both sides agree. (Sequential deletes are a different case: the
    second one is asked with the message in Trash.)"""
    mid = w.mc_id("INBOX", 12)
    clicks = [subprocess.Popen(MC + ["messages", "delete", mid, "--mailbox", w.boxes["INBOX"]["ID"]], stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, env=ENV) for _ in range(2)]
    outs = [(p.communicate()[0], p.returncode) for p in clicks]
    failed = [out for out, code in outs if code]
    if len(failed) == 2 or any("not found" not in out.lower() for out in failed):
        raise Fail("the double click failed otherwise than one 'not found':\n" + "\n".join(failed))
    wait(lambda: on_server(w, 12, "Trash") and not on_server(w, 12, "INBOX"), "tag 12 in Trash on server")
    w.sync()
    w.assert_where(12, {"Trash": (1, 1)})
    w.assert_consistent()


def db(sql, params=()):
    """One row of the isolated server's database, read-only."""
    con = sqlite3.connect(f"file:{HERE / '.state' / 'mc' / 'mail-cat.db'}?mode=ro", uri=True)
    try:
        return con.execute(sql, params).fetchone()
    finally:
        con.close()


def backfill_text_only(w):
    """A 4 MB attachment and a 1 MB text part cost the backfill about 256 KiB:
    only the start of the text comes down, flagged truncated, and opening the
    message fetches the rest. An All Mail-style copy is never downloaded."""
    big = EmailMessage()
    big["Subject"], big["From"], big["To"] = "Big log [itest-901]", "carol@example.net", "alice@itest.local"
    big["Message-ID"] = "<big901@example.net>"
    big.set_content("a line of a long pasted log\n" * 40_000)  # ~1.1 MB of text
    att = EmailMessage()
    att["Subject"], att["From"], att["To"] = "Scans [itest-902]", "dan@example.org", "alice@itest.local"
    att["Message-ID"] = "<att902@example.net>"
    att.set_content("Scans attached.\n")
    att.add_attachment(os.urandom(4 << 20), maintype="application", subtype="pdf", filename="scans.pdf")
    m = imapctl.connect()
    for box, msg in (("INBOX", big), ("INBOX", att), ("Archive", big)):
        imapctl.check(m.append(imapctl.quote(box), "", imaplib.Time2Internaldate(time.time()), msg.as_bytes()))
    m.logout()
    before = db("SELECT COALESCE(SUM(bytes), 0) FROM downloads")[0]
    w.sync()
    pending = ("SELECT COUNT(DISTINCT m.id) FROM messages m JOIN message_mailboxes mm ON mm.message_id = m.id "
               "WHERE m.body_state = 'pending' AND mm.remote_id IS NOT NULL")
    # The worker naps a minute when its queue runs dry.
    wait(lambda: db(pending)[0] == 0, "every body backfilled", timeout=90)
    spent = db("SELECT COALESCE(SUM(bytes), 0) FROM downloads")[0] - before
    if spent > 300 << 10:
        raise Fail(f"backfill downloaded {spent} bytes for 3 new rows, want under 300 KiB")
    truncated = db("SELECT COUNT(*) FROM message_bodies b JOIN messages m ON m.id = b.message_id "
                   "WHERE m.rfc_message_id = '<big901@example.net>' AND b.truncated = 1")[0]
    if truncated != 2:
        raise Fail(f"{truncated} copies of the big text stored truncated, want both")
    out = mc("messages", "get", "--text", w.mc_id("INBOX", 901), "--mailbox", w.boxes["INBOX"]["ID"])
    if out.count("a line of a long pasted log") != 40_000:
        raise Fail(f"opening the message did not fetch its whole text ({out.count('a line')} lines)")
    w.assert_consistent()


def jmap_snapshot(account):
    """({display_path: mailbox}, {tag: (display_path, message)}) as mail-cat has them."""
    boxes = {m["display_path"]: m for m in json.loads(mc("--json", "mailboxes", "list", account))}
    msgs = {}
    for path, box in boxes.items():
        for m in json.loads(mc("--json", "messages", "list", "-v", box["id"])):
            msgs[imapctl.tag_of(m["subject"])] = (path, m)
    return boxes, msgs


def jmap_sync_all(account):
    for box in jmap_snapshot(account)[0].values():
        mc("sync", account, "--mailbox", box["id"])


def jmap_sync(w):
    """bob, a JMAP account, syncs from scratch and incrementally: mailbox list,
    per-mailbox counts, read state, a body fetch, then flag and delete changes
    made by another client arrive through Email/changes."""
    bob = account_id("bob")
    snapshot, sync_bob = (lambda: jmap_snapshot(bob)), (lambda: jmap_sync_all(bob))

    def check(boxes, msgs, expected):
        for path, n in expected.items():
            if int(boxes[path]["total_messages"]) != n:
                raise Fail(f"{path}: mailbox total {boxes[path]['total_messages']}, expected {n}")
        where = {tag: path for tag, (path, _) in msgs.items()}
        want = {tag: path for path, tags in EXPECT.items() for tag in tags}
        if where != want:
            raise Fail(f"messages by mailbox: expected {want}, got {where}")

    # seed.py's default layout over six messages; 3 and 6 are \Seen.
    EXPECT = {"Inbox": [1, 2, 4, 6], "Archive": [3], "Projects": [5]}
    run([sys.executable, str(HERE / "seed.py"), "--user", "bob", "-n", "6"])
    mc("sync", bob)
    boxes, msgs = snapshot()
    if not {"Inbox", "Archive", "Projects", "Sent", "Drafts", "Trash", "Junk"} <= set(boxes):
        raise Fail(f"mailboxes: {sorted(boxes)}")
    check(boxes, msgs, {"Inbox": 4, "Archive": 1, "Projects": 1})
    read = sorted(tag for tag, (_, m) in msgs.items() if m["is_read"])
    if read != [3, 6]:
        raise Fail(f"read messages {read}, expected [3, 6]")
    inbox = boxes["Inbox"]["id"]
    out = mc("messages", "get", "--text", msgs[2][1]["id"], "--mailbox", inbox)
    if "Body of message 2." not in out:
        raise Fail(f"body of tag 2 not fetched:\n{out}")

    # Another client: flags tag 2 read, deletes tag 1, delivers two more.
    imapctl.main(["--user", "bob", "store", "INBOX", "tag:2", r"+\Seen"])
    imapctl.main(["--user", "bob", "delete", "INBOX", "tag:1"])
    run([sys.executable, str(HERE / "seed.py"), "--user", "bob", "--start", "7", "-n", "2"])
    EXPECT["Inbox"] = [2, 4, 6, 7, 8]
    sync_bob()
    boxes, msgs = snapshot()
    check(boxes, msgs, {"Inbox": 5, "Archive": 1, "Projects": 1})
    if not msgs[2][1]["is_read"]:
        raise Fail("tag 2 was marked read on the server but not in mail-cat")


def jmap_write(w):
    """bob, a JMAP account, changes flags, moves, deletes (soft and permanent),
    saves and replaces a draft and sends to alice; the server sees each. New
    mail from another client then appears through push, without `mc sync`."""
    bob = account_id("bob")
    run([sys.executable, str(HERE / "seed.py"), "--user", "bob", "-n", "6"])
    jmap_sync_all(bob)
    boxes, msgs = jmap_snapshot(bob)

    def server(path, user="bob"):
        m = imapctl.connect(user)
        try:
            return imapctl.messages(m, path)
        finally:
            m.logout()

    def by_tag(path, tag):
        return [x for x in server(path) if imapctl.tag_of(x["subject"]) == tag]

    def subjects(path, user="bob"):
        return [x["subject"] for x in server(path, user)]

    def msg(tag):
        return msgs[tag][1]["id"]

    mc("messages", "mark-read", msg(1))
    mc("messages", "star", msg(1))
    flags = by_tag("INBOX", 1)[0]["flags"]
    if r"\Seen" not in flags or r"\Flagged" not in flags:
        raise Fail(f"tag 1 flags on the server: {flags}")
    mc("messages", "mark-unread", msg(1))
    if r"\Seen" in by_tag("INBOX", 1)[0]["flags"]:
        raise Fail("tag 1 still \\Seen on the server after mark-unread")

    inbox, archive = boxes["Inbox"]["id"], boxes["Archive"]["id"]
    mc("messages", "move", msg(2), "--mailbox", inbox, archive)
    if by_tag("INBOX", 2) or not by_tag("Archive", 2):
        raise Fail("tag 2 did not move from INBOX to Archive on the server")

    mc("messages", "delete", msg(4), "--mailbox", inbox)
    if by_tag("INBOX", 4) or not by_tag("Trash", 4):
        raise Fail("soft delete did not file tag 4 into Trash")
    mc("messages", "delete", msg(6), "--mailbox", inbox, "--permanent")
    if any(by_tag(p, 6) for p in ("INBOX", "Trash", "Archive")):
        raise Fail("permanent delete left tag 6 on the server")

    args = ["messages", "draft", bob, "--to", "alice@itest.local", "--body", "draft body"]
    draft = mc(*args, "--subject", "draft one").strip().splitlines()[-1]
    wait(lambda: subjects("Drafts") == ["draft one"], "draft one on the server")
    if r"\Draft" not in server("Drafts")[0]["flags"]:
        raise Fail("the saved draft lacks \\Draft")
    mc(*args, "--subject", "draft two", "--replace", draft)
    wait(lambda: subjects("Drafts") == ["draft two"], "draft two to replace draft one")

    mc("messages", "send-test", bob, "--to", "alice@itest.local")
    wait(lambda: any(s.startswith("[mail-cat test]") for s in subjects("INBOX", "alice")),
         "the sent mail in alice's INBOX")
    wait(lambda: any(s.startswith("[mail-cat test]") for s in subjects("Sent")),
         "the sent copy in bob's Sent")
    if any(s.startswith("[mail-cat test]") for s in subjects("Drafts")):
        raise Fail("the sent mail was left in Drafts")

    # What mail-cat believes after all that matches the server.
    jmap_sync_all(bob)
    boxes, msgs = jmap_snapshot(bob)
    for path, name in (("INBOX", "Inbox"), ("Archive", "Archive"), ("Trash", "Trash")):
        got = sorted(tag for tag, (p, _) in msgs.items() if p == name and tag is not None)
        want = sorted(t for t in map(imapctl.tag_of, subjects(path)) if t is not None)
        if got != want:
            raise Fail(f"{name}: mail-cat has {got}, the server {want}")

    if os.environ.get("MAIL_CAT_NO_IDLE"):
        return
    run([sys.executable, str(HERE / "seed.py"), "--user", "bob", "--start", "30", "-n", "1"])
    wait(lambda: 30 in jmap_snapshot(bob)[1], "pushed mail to appear without `mc sync`", timeout=20)


def jmap_labels(w):
    """bob, a JMAP account on the Labels model, adds and removes mailbox labels
    (one message row in several mailboxes), archives by dropping the Inbox
    label, refuses to unfile a message entirely, and round-trips tags with
    another client. Switching the model changes no data."""
    bob = account_id("bob")
    run([sys.executable, str(HERE / "seed.py"), "--user", "bob", "-n", "6"])
    jmap_sync_all(bob)
    boxes, msgs = jmap_snapshot(bob)
    inbox, archive, projects = (boxes[n]["id"] for n in ("Inbox", "Archive", "Projects"))

    def server(path):
        m = imapctl.connect("bob")
        try:
            return imapctl.messages(m, path)
        finally:
            m.logout()

    def where(tag):
        return sorted(p for p in ("INBOX", "Archive", "Projects")
                      if any(imapctl.tag_of(x["subject"]) == tag for x in server(p)))

    def msg(tag):
        return msgs[tag][1]["id"]

    def rows(tag):
        mine = ("subject like '%[itest-{:03d}]%' and account_id = "
                "(select id from accounts where name = 'bob')").format(tag)
        return (db(f"select count(*) from messages where {mine}")[0],
                db("select count(*) from message_mailboxes where message_id = "
                   f"(select id from messages where {mine})")[0])

    account = next(a for a in json.loads(mc("--json", "accounts", "list")) if a["id"] == bob)
    if account["mailbox_model"] != "Labels":
        raise Fail(f"bob's model is {account['mailbox_model']}, expected Labels")

    mc("messages", "labels", msg(1), "--add", projects)
    if where(1) != ["INBOX", "Projects"]:
        raise Fail(f"tag 1 after adding a label: {where(1)}")
    jmap_sync_all(bob)
    if rows(1) != (1, 2):
        raise Fail(f"tag 1 has (message rows, mailbox rows) {rows(1)}, expected (1, 2)")
    jmap_sync_all(bob)
    boxes, msgs = jmap_snapshot(bob)
    if int(boxes["Projects"]["total_messages"]) != 2 or rows(1) != (1, 2):
        raise Fail("syncing after a label change duplicated or lost a row")

    mc("messages", "labels", msg(1), "--remove", projects)
    if where(1) != ["INBOX"]:
        raise Fail(f"tag 1 after removing the label: {where(1)}")
    try:
        mc("messages", "labels", msg(1), "--remove", inbox)
    except Fail:
        pass
    else:
        raise Fail("removing the last label was accepted")
    if where(1) != ["INBOX"]:
        raise Fail("a refused unfile still changed tag 1")

    # Archive: a lone Inbox message goes to Archive; one with another label keeps it only.
    mc("messages", "archive", msg(4))
    if where(4) != ["Archive"]:
        raise Fail(f"tag 4 after archive: {where(4)}")
    mc("messages", "labels", msg(2), "--add", projects)
    mc("messages", "archive", msg(2))
    if where(2) != ["Projects"]:
        raise Fail(f"tag 2 after archive: {where(2)}")
    jmap_sync_all(bob)
    if rows(4) != (1, 1) or rows(2) != (1, 1):
        raise Fail(f"rows after archive: tag 4 {rows(4)}, tag 2 {rows(2)}")

    # Tags round-trip through the other client.
    def server_flags(tag):
        return next(x["flags"] for p in ("INBOX", "Archive", "Projects") for x in server(p)
                    if imapctl.tag_of(x["subject"]) == tag)

    def tags(tag):
        box = {1: inbox, 4: archive}[tag]
        line = next(l for l in mc("messages", "show", msg(tag), "--mailbox", box).splitlines()
                    if l.startswith("tags:"))
        return line.split()[1:]

    mc("messages", "tag", msg(1), "--add", "work")
    wait(lambda: any(f.lower() == "work" for f in server_flags(1)), "tag work on the server")
    imapctl.main(["--user", "bob", "store", "INBOX", "tag:1", "+phone"])
    jmap_sync_all(bob)
    if sorted(tags(1)) != ["phone", "work"]:
        raise Fail(f"tags of tag 1 after the phone added one: {tags(1)}")
    mc("messages", "tag", msg(1), "--remove", "work")
    wait(lambda: not any(f.lower() == "work" for f in server_flags(1)), "tag work removed on the server")

    # The model is presentation only.
    before = jmap_snapshot(bob)[1]
    mc("accounts", "set-model", "bob", "folders")
    account = next(a for a in json.loads(mc("--json", "accounts", "list")) if a["id"] == bob)
    if account["mailbox_model"] != "Folders" or account["supports_tags"] is not True:
        raise Fail(f"after set-model folders: {account}")
    after = jmap_snapshot(bob)[1]
    if {t: (p, m["mailbox_ids"]) for t, (p, m) in before.items()} != \
            {t: (p, m["mailbox_ids"]) for t, (p, m) in after.items()}:
        raise Fail("switching the model changed stored data")
    mc("accounts", "set-model", "bob", "labels")


def subscribed_on_server(user="alice"):
    m = imapctl.connect(user)
    try:
        return imapctl.lsub(m)
    finally:
        m.logout()


def subscribed_in_mc(account):
    return {m["display_path"]: m["subscribed"] for m in json.loads(mc("--json", "mailboxes", "list", account))}


def subscriptions_imap(w):
    """alice unsubscribes Projects: its mail leaves mail-cat, the server's LSUB
    drops it and syncs leave it alone; deleting into an unsubscribed Trash
    only removes the message here; subscribing again brings the mail back; an
    unsubscription made by another client is followed."""
    projects = w.boxes["Projects"]["ID"]
    others = {p: len(w.mc_list(p)) for p in ("INBOX", "Archive")}
    if len(w.mc_list("Projects")) != 4 or not {"INBOX", "Projects", "Trash"} <= subscribed_on_server():
        raise Fail("expected Projects synced (4) and subscribed on the server to start with")

    mc("mailboxes", "unsubscribe", w.account, "Projects")
    if subscribed_in_mc(w.account)["Projects"]:
        raise Fail("Projects still subscribed after unsubscribe")
    wait(lambda: not w.mc_list("Projects"), "Projects' messages dropped")
    wait(lambda: "Projects" not in subscribed_on_server(), "Projects gone from the server's LSUB")
    w.sync()
    if w.mc_list("Projects"):
        raise Fail("a sync brought the unsubscribed Projects back")
    for path, n in others.items():
        if len(w.mc_list(path)) != n:
            raise Fail(f"{path} changed with Projects' subscription: {len(w.mc_list(path))} != {n}")

    # Trash unsubscribed: a delete files the message there on the server and just drops it here.
    mc("mailboxes", "unsubscribe", w.account, "Trash")
    wait(lambda: not w.mc_list("Trash"), "Trash's messages dropped")
    mc("messages", "delete", w.mc_id("INBOX", 1), "--mailbox", w.boxes["INBOX"]["ID"])
    wait(lambda: on_server(w, 1, "Trash") and not on_server(w, 1, "INBOX"), "tag 1 in Trash on the server")
    w.sync()
    w.assert_where(1, {"Trash": (1, 0)})

    mc("mailboxes", "subscribe", w.account, "Projects")
    wait(lambda: sorted(m["tag"] for m in w.mc_list("Projects")) == [5, 10, 15, 20],
         "Projects' messages back after subscribing again")
    wait(lambda: "Projects" in subscribed_on_server(), "Projects back in the server's LSUB")

    # Another client unsubscribes a folder: mail-cat has no explicit choice there, so it follows.
    imapctl.main(["create", "Extra"])
    imapctl.main(["subscribe", "Extra"])
    w.sync()
    if not subscribed_in_mc(w.account)["Extra"]:
        raise Fail("Extra, subscribed on the server, is not subscribed in mail-cat")
    imapctl.main(["unsubscribe", "Extra"])
    w.sync()
    if subscribed_in_mc(w.account)["Extra"]:
        raise Fail("Extra unsubscribed elsewhere is still subscribed in mail-cat")
    # The mandatory mailboxes cannot be unsubscribed, from here or from the server.
    for name in ("Sent", "Drafts", "Archive"):
        out = mc_refuses("mailboxes", "unsubscribe", w.account, name)
        if "always subscribed" not in out:
            raise Fail(f"unsubscribing {name} was refused for another reason: {out}")
    imapctl.main(["unsubscribe", "Archive"])
    w.sync()
    if not subscribed_in_mc(w.account)["Archive"]:
        raise Fail("Archive unsubscribed elsewhere was followed")
    imapctl.main(["subscribe", "Archive"])
    # An explicit choice is not overridden: the phone's LSUB still lists Projects.
    mc("mailboxes", "unsubscribe", w.account, "Projects")
    imapctl.main(["subscribe", "Projects"])
    w.sync()
    if subscribed_in_mc(w.account)["Projects"]:
        raise Fail("the server's subscription overrode an explicit unsubscribe")


def subscriptions_jmap(w):
    """bob, a JMAP account, unsubscribes Projects (Mailbox/set isSubscribed,
    seen by IMAP's LSUB) and subscribes it again; another client's
    unsubscription of Archive arrives with the next sync."""
    bob = account_id("bob")
    run([sys.executable, str(HERE / "seed.py"), "--user", "bob", "-n", "6"])
    jmap_sync_all(bob)
    boxes, msgs = jmap_snapshot(bob)
    if "Projects" not in subscribed_on_server("bob") or not boxes["Projects"]["subscribed"]:
        raise Fail("expected Projects subscribed to start with")

    mc("mailboxes", "unsubscribe", bob, "Projects")
    wait(lambda: not any(p == "Projects" for p, _ in jmap_snapshot(bob)[1].values()),
         "Projects' messages dropped")
    if jmap_snapshot(bob)[0]["Projects"]["subscribed"]:
        raise Fail("Projects still subscribed after unsubscribe")
    if "Projects" in subscribed_on_server("bob"):
        raise Fail("the server still lists Projects as subscribed")
    jmap_sync_all(bob)
    if any(p == "Projects" for p, _ in jmap_snapshot(bob)[1].values()):
        raise Fail("a sync brought the unsubscribed Projects back")

    mc("mailboxes", "subscribe", bob, "Projects")
    wait(lambda: [t for t, (p, _) in jmap_snapshot(bob)[1].items() if p == "Projects"] == [5],
         "Projects' messages back after subscribing again")
    if not jmap_snapshot(bob)[0]["Projects"]["subscribed"]:
        raise Fail("Projects not subscribed again")

    imapctl.main(["--user", "bob", "create", "Extra"])
    imapctl.main(["--user", "bob", "subscribe", "Extra"])
    mc("sync", bob)  # a full sync lists the mailboxes; per-mailbox syncs do not
    imapctl.main(["--user", "bob", "unsubscribe", "Extra"])
    mc("sync", bob)
    if jmap_snapshot(bob)[0]["Extra"]["subscribed"]:
        raise Fail("Extra unsubscribed elsewhere is still subscribed in mail-cat")
    imapctl.main(["--user", "bob", "unsubscribe", "Archive"])
    mc("sync", bob)
    if not jmap_snapshot(bob)[0]["Archive"]["subscribed"]:
        raise Fail("Archive (mandatory) unsubscribed elsewhere was followed")
    imapctl.main(["--user", "bob", "subscribe", "Archive"])


def subscriptions_bulk(w):
    """Unsubscribing a mailbox of 3000 messages returns at once and the
    messages leave mail-cat in the background; Sent cannot be unsubscribed."""
    run([sys.executable, str(HERE / "seed.py"), "-n", "3000", "--start", "1000", "Projects"])
    w.sync()
    projects = w.boxes["Projects"]["ID"]
    rows = lambda: db("SELECT COUNT(*) FROM message_mailboxes WHERE lower(hex(mailbox_id)) = ?",
                      (projects.replace("-", "").lower(),))[0]
    if rows() < 3000:
        raise Fail(f"expected 3000+ rows in Projects after the sync, got {rows()}")

    started = time.time()
    mc("mailboxes", "unsubscribe", w.account, "Projects")
    took = time.time() - started
    if took > 2:
        raise Fail(f"unsubscribing took {took:.1f}s, want under 2s")
    wait(lambda: rows() == 0, "Projects' 3000 rows dropped in the background", timeout=120)
    if subscribed_in_mc(w.account)["Projects"]:
        raise Fail("Projects still subscribed")
    total = next(m["total_messages"] for m in json.loads(mc("--json", "mailboxes", "list", w.account))
                 if m["display_path"] == "Projects")
    if total != 0:
        raise Fail(f"an unsubscribed mailbox keeps the server's total: {total}")

    if "always subscribed" not in mc_refuses("mailboxes", "unsubscribe", w.account, "Sent"):
        raise Fail("unsubscribing Sent was refused for another reason")
    if not subscribed_in_mc(w.account)["Sent"]:
        raise Fail("Sent lost its subscription")


def hot_add(w):
    """An account added to the running server syncs, and gets push, without a restart."""
    stalwart = HERE / "stalwart-cli"
    domain = json.loads(run([str(stalwart), "query", "Domain", "--json"]).splitlines()[0])["id"]
    run([str(stalwart), "create", "Account/User", "--json", json.dumps({
        "name": "carol", "domainId": domain,
        "credentials": {"0": {"@type": "Password", "secret": imapctl.USERS["carol"][1]}}})])

    def deliver(n):
        msg = EmailMessage()
        msg["Subject"], msg["From"], msg["To"] = f"Hot [itest-95{n}]", "dan@example.org", "carol@itest.local"
        msg["Message-ID"] = f"<hot95{n}@example.org>"
        msg.set_content("hello\n")
        m = imapctl.connect("carol")
        imapctl.check(m.append("INBOX", "", imaplib.Time2Internaldate(time.time()), msg.as_bytes()))
        m.logout()

    for n in range(3):
        deliver(n)
    pid = (HERE / ".state" / "server.pid").read_text()
    log = HERE / ".state" / "server.log"

    def watchers():
        return log.read_text(errors="replace").count("IDLE active for 'INBOX'")

    watching = watchers()  # alice's

    p = subprocess.run(MC + ["accounts", "add", "--email", "carol@itest.local", "--name", "carol",
                             "--imap-host", "localhost", "--imap-port", "19993", "--password-stdin"],
                       input=imapctl.USERS["carol"][1] + "\n", capture_output=True, text=True, env=ENV)
    if p.returncode or "Riavvia" in p.stdout:
        raise Fail(f"mc accounts add failed or still asks for a restart:\n{p.stdout}{p.stderr}")

    def inbox_total():
        carol = account_id("carol")
        boxes = {r["Path"]: r for r in table(mc("mailboxes", "list", carol))}
        return int(boxes["INBOX"]["Total"]) if "INBOX" in boxes else 0

    wait(lambda: inbox_total() == 3, "carol's three messages synced without a restart", timeout=60)
    wait(lambda: watchers() > watching, "carol's INBOX watcher running", timeout=30)
    deliver(3)
    wait(lambda: inbox_total() == 4, "a message delivered after the add arrives by push", timeout=30)
    if (HERE / ".state" / "server.pid").read_text() != pid:
        raise Fail("the server was restarted")


SCENARIOS = [basic_sync, flags, move_delete, phone_moved_first, remote_delete, reappear, delete_twice, backfill_text_only, jmap_sync, jmap_write, jmap_labels, subscriptions_imap, subscriptions_jmap, subscriptions_bulk, hot_add]
SCENARIOS += gmail_scenarios.make(sys.modules[__name__])


def fresh_world(gmail=False):
    if gmail:  # the mailbox is seeded first so the server's first sync sees the fixture
        from gmail import seed
        seed.reset()
    reset_env = {**os.environ, "ITEST_GMAIL": "1"} if gmail else None
    for cmd in ([str(HERE / "reset.sh")], [sys.executable, str(HERE / "seed.py")]):
        p = subprocess.run(cmd, capture_output=True, text=True, env=reset_env)
        if p.returncode:
            sys.exit(f"{cmd[-1]} failed:\n{p.stdout}{p.stderr}")
    w = World()
    w.sync()
    return w


def main():
    wanted = sys.argv[1:] or [s.__name__ for s in SCENARIOS]
    unknown = set(wanted) - {s.__name__ for s in SCENARIOS}
    if unknown:
        sys.exit(f"unknown scenario(s): {', '.join(unknown)}")
    LOGS.mkdir(exist_ok=True)
    results = []
    from gmail import api
    gmail_ok, gmail_why = api.available()
    for s in SCENARIOS:
        if s.__name__ not in wanted:
            continue
        if getattr(s, "gmail", False) and not gmail_ok:
            print(f"── {s.__name__}\n   SKIP: {gmail_why}")
            continue
        print(f"── {s.__name__}: {' '.join(s.__doc__.split()).split('. ')[0]}")
        try:
            s(fresh_world(getattr(s, "gmail", False)))
            results.append((s.__name__, None))
            print("   PASS")
        except (Exception, SystemExit) as e:  # SystemExit: imapctl selector misses
            results.append((s.__name__, str(e)))
            print("   FAIL: " + str(e).replace("\n", "\n   "))
        shutil.copy(HERE / ".state" / "server.log", LOGS / f"{s.__name__}.log")
    print("\n" + "\n".join(f"{'FAIL' if err else 'PASS'}  {name}" for name, err in results))
    print(f"server logs: {LOGS}/<scenario>.log")
    sys.exit(1 if any(err for _, err in results) else 0)


if __name__ == "__main__":
    main()
