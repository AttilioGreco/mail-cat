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


def db(sql):
    """One row of the isolated server's database, read-only."""
    con = sqlite3.connect(f"file:{HERE / '.state' / 'mc' / 'mail-cat.db'}?mode=ro", uri=True)
    try:
        return con.execute(sql).fetchone()
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


def jmap_sync(w):
    """bob, a JMAP account, syncs from scratch and incrementally: mailbox list,
    per-mailbox counts, read state, a body fetch, then flag and delete changes
    made by another client arrive through Email/changes."""
    bob = account_id("bob")

    def snapshot():
        boxes = {m["display_path"]: m for m in json.loads(mc("--json", "mailboxes", "list", bob))}
        msgs = {}
        for path, box in boxes.items():
            for m in json.loads(mc("--json", "messages", "list", "-v", box["id"])):
                msgs[imapctl.tag_of(m["subject"])] = (path, m)
        return boxes, msgs

    def sync_bob():
        for box in snapshot()[0].values():
            mc("sync", bob, "--mailbox", box["id"])

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


SCENARIOS = [basic_sync, flags, move_delete, phone_moved_first, remote_delete, reappear, delete_twice, backfill_text_only, jmap_sync]


def fresh_world():
    for cmd in ([str(HERE / "reset.sh")], [sys.executable, str(HERE / "seed.py")]):
        p = subprocess.run(cmd, capture_output=True, text=True)
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
    for s in SCENARIOS:
        if s.__name__ not in wanted:
            continue
        print(f"── {s.__name__}: {' '.join(s.__doc__.split()).split('. ')[0]}")
        try:
            s(fresh_world())
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
