"""Gmail scenarios: mail-cat (Gmail over IMAP, labels) against the Gmail REST API.

The API is the oracle: expectations come from it, never from IMAP. Only run
when MAILCAT_ITEST_GMAIL_CONFIRM names the test address and the app's keychain
holds a login for it (see README). They only touch messages seeded with the
Message-ID prefix mcitest-. `make(h)` takes the scenarios module `h` for
its helpers, which avoids importing scenarios.py from here.
"""
import imaplib
import json
import re
import time

from gmail import api, compare, seed


def make(h):
    state = {}

    def gm():
        state.setdefault("g", api.Gmail())
        return state["g"]

    def acct():
        return h.account_id("gmail")

    def boxes():
        return json.loads(h.mc("--json", "mailboxes", "list", acct()))

    def sync():
        out = h.mc("sync", acct())
        if "Error:" in out:
            raise h.Fail(f"sync reported an error:\n{out}")

    def mc_state():
        """({Message-ID: labels}, {Message-ID: mail-cat id}, {label name: mailbox id}, {Message-ID: mailbox it was read from})"""
        bx = boxes()
        by_id = {b["id"]: b for b in bx}
        ids = {compare.label_name(b): b["id"] for b in bx if compare.label_name(b)}
        labels, mcid, home = {}, {}, {}
        for b in bx:
            if b["mailbox_type"] not in ("All", "Trash", "Spam"):
                continue
            for m in json.loads(h.mc("--json", "messages", "list", "-v", b["id"])):
                if f"[{seed.ID_PREFIX}" not in m["subject"]:
                    continue  # not ours: never show, let alone touch
                full = json.loads(h.mc("--json", "messages", "show", m["id"], "--mailbox", b["id"]))
                mid = compare.norm_message_id(full["rfc_message_id"])
                names = {compare.label_name(by_id[x["mailbox_id"]]) for x in full["memberships"]}
                labels[mid], mcid[mid], home[mid] = names - {None}, m["id"], b["id"]
        return labels, mcid, ids, home

    def assert_equal(what):
        problems = compare.diff_labels(gm().snapshot(seed.ID_PREFIX), mc_state()[0])
        if problems:
            raise h.Fail(f"{what}: Gmail API and mail-cat disagree:\n  " + "\n  ".join(problems))

    L = seed.label

    def mid(n):
        return seed.message_id(n)

    def api_labels(n):
        return gm().snapshot(seed.ID_PREFIX)[mid(n)]

    def gmail_sync(w):
        """First sync of the Gmail account: All Mail/Spam/Trash are the real mailboxes, memberships equal the API's labelIds."""
        sync()
        kinds = {b["mailbox_type"] for b in boxes()}
        if not {"All", "Spam", "Trash", "Inbox"} <= kinds:
            raise h.Fail(f"mailbox kinds: {sorted(kinds)}")
        assert_equal("after first sync")
        ids = mc_state()[2]
        for name in (L("Clienti/Café"), L("A/B/C"), L("Presentations"), L("Old Trash"), L("Inbox Zero")):
            if name not in ids:
                raise h.Fail(f"label {name!r} is not a mailbox in mail-cat: {sorted(ids)}")

    def gmail_labels_mc_to_server(w):
        """Label changes, archive and delete made in mail-cat show up in the API's labelIds; nasty names survive."""
        sync()
        labels, mcid, ids, _ = mc_state()
        h.mc("messages", "labels", mcid[mid(1)], "--add", ids[L("Clienti/Café")], "--add", ids[L("Presentations")])
        h.mc("messages", "labels", mcid[mid(2)], "--remove", ids[L("A/B/C")])
        for nasty in (L('Say "hi"'), L("back\\slash")):
            if nasty in ids:
                h.mc("messages", "labels", mcid[mid(4)], "--add", ids[nasty])
        h.mc("messages", "archive", mcid[mid(5)])
        box = api.SANDBOX
        want = {1: {box, "INBOX", L("Clienti/Café"), L("Presentations")}, 2: {box, "INBOX", L("Clienti/Café")},
                5: {box, "STARRED"}}
        for n, expected in want.items():
            if api_labels(n) != expected:
                raise h.Fail(f"message {n}: API has {sorted(api_labels(n))}, expected {sorted(expected)}")
        for nasty in (L('Say "hi"'), L("back\\slash")):
            if nasty in ids and nasty not in api_labels(4):
                raise h.Fail(f"{nasty!r} did not reach message 4: {sorted(api_labels(4))}")
        sync()
        assert_equal("after mail-cat edits")
        h.mc("messages", "delete", mcid[mid(6)], "--mailbox", mc_state()[3][mid(6)])
        if "TRASH" not in api_labels(6):
            raise h.Fail(f"message 6 after delete: {sorted(api_labels(6))}")
        sync()
        assert_equal("after delete")

    def gmail_server_to_mc(w):
        """Changes made through the API (labels, archive, trash, label rename/delete/create) reach mail-cat after a sync."""
        sync()
        g = gm()
        by_name = {l["name"]: l["id"] for l in g.labels()}
        gid = lambda n: g.find(mid(n))
        g.modify(gid(1), add=[by_name[L("Presentations")]])
        g.modify(gid(5), remove=["INBOX"])
        g.trash(gid(6))
        g.label_rename(by_name[L("Presentations")], L("Slides"))
        g.label_delete(by_name[L("Old Trash")])
        fresh = g.label_create(L("Fresh/New"))
        g.modify(gid(4), add=[fresh])
        sync()
        # IMAP can lag the API: a second sync tells a lag from a lost update.
        if compare.diff_labels(gm().snapshot(seed.ID_PREFIX), mc_state()[0]):
            time.sleep(20)
            sync()
        assert_equal("after API edits")
        names = set(mc_state()[2])
        if {L("Slides"), L("Fresh/New")} - names or {L("Presentations"), L("Old Trash")} & names:
            raise h.Fail(f"mail-cat labels after rename/delete/create: {sorted(names)}")

    def gmail_delete_permanent(w):
        """Permanent delete in mail-cat leaves nothing in All Mail or Trash according to the API."""
        sync()
        _, mcid, _, home = mc_state()
        for n in (3, 9):  # an archived message, and one already in Trash
            h.mc("messages", "delete", mcid[mid(n)], "--mailbox", home[mid(n)], "--permanent")
        sync()
        left = {n for n in (3, 9) if gm().find(mid(n))}
        if left:
            raise h.Fail(f"messages {sorted(left)} still exist in Gmail after a permanent delete")
        if {mid(3), mid(9)} & mc_state()[0].keys():
            raise h.Fail("mail-cat still has a permanently deleted message")
        assert_equal("after permanent delete")

    def gmail_model_default(w):
        """The Gmail account defaults to the Labels model, recorded once."""
        sync()
        sync()
        account = next(a for a in json.loads(h.mc("--json", "accounts", "list")) if a["id"] == acct())
        if account["mailbox_model"] != "Labels":
            raise h.Fail(f"mailbox_model is {account['mailbox_model']}, expected Labels")
        n = h.db(f"select count(*) from events where event_type = 'AccountMailboxModelChanged' "
                 f"and aggregate_id = '{acct()}'")[0]
        if n != 1:
            raise h.Fail(f"{n} AccountMailboxModelChanged events, expected 1")

    def imap_view():
        """({LIST names}, {X-GM-LABELS of each seeded rename message}) read over IMAP, the way mail-cat reads them."""
        g = gm()
        token = g._access_token()
        c = imaplib.IMAP4_SSL("imap.gmail.com")
        c.authenticate("XOAUTH2", lambda _: f"user={g.address}\x01auth=Bearer {token}\x01\x01".encode())
        try:
            listed, all_mail = set(), None
            for line in c.list()[1]:
                m = re.match(rb'\((.*?)\) "(.)" (.*)$', line)
                name = m.group(3).decode().strip('"')
                listed.add(name)
                if b"\\All" in m.group(1):
                    all_mail = name
            c.select(f'"{all_mail}"', readonly=True)
            labels = {}
            for n in RENAME_MESSAGES:
                uid = c.uid("SEARCH", None, "HEADER", "Message-ID", mid(n))[1][0].split()
                if not uid:
                    continue
                fetched = c.uid("FETCH", uid[0], "(X-GM-LABELS)")[1][0]
                # Gmail may put UID after the list, so stop at the list's own ')' (quotes may hold one).
                inner = re.search(rb'X-GM-LABELS \(((?:"(?:[^"\\]|\\.)*"|[^()"])*)\)', fetched).group(1).decode()
                labels[n] = {re.sub(r"\\(.)", r"\1", a) if a else b
                             for a, b in re.findall(r'"((?:[^"\\]|\\.)*)"|(\S+)', inner)}
            return listed, labels
        finally:
            c.logout()

    RENAME_MESSAGES = (101, 102, 103, 104)

    def itest_cleanup():
        """Remove what gmail_rename_tree makes, under whatever name it ended with. The harness guards still apply."""
        g = gm()
        found = [g.find(mid(n)) for n in RENAME_MESSAGES]
        g.batch_delete([i for i in found if i])
        keep = L("Itest").lower()
        for label in g.labels():
            if label["name"].lower().startswith(keep):
                g.label_delete(label["id"])

    def wait_for_job():
        for _ in range(120):
            if not json.loads(h.mc("--json", "labels", "stats"))["applying"]:
                return
            time.sleep(1)
        raise h.Fail("the label job did not finish in 2 minutes")

    def synced_equal(what):
        sync()
        # IMAP can lag the API: a second sync tells a lag from a lost update.
        if compare.diff_labels(gm().snapshot(seed.ID_PREFIX), mc_state()[0]):
            time.sleep(20)
            sync()
        assert_equal(what)

    def gmail_rename_tree(w):
        """Renaming a label in mail-cat renames it on Gmail with its child (labels, Inbox membership and mail kept), a case-only rename works, merging the child into an existing label and deleting the empty parent leave the mail alone, and no sync duplicates or rereads anything."""
        g = gm()
        itest_cleanup()
        sandbox = g.label_id(api.SANDBOX)
        parent, child, target = (g.label_create(L(n)) for n in ("ItestRename", "ItestRename/Child", "ItestTarget"))
        for n, extra in ((101, []), (102, []), (103, ["INBOX"])):
            g.insert(seed.rfc822(n, "rename tree"), [sandbox, parent, child] + extra)
        g.insert(seed.rfc822(104, "already in the target"), [sandbox, target])
        try:
            # Labels made through the API can take well over 20s to reach IMAP LIST.
            fresh = {L("ItestRename"), L("ItestRename/Child"), L("ItestTarget")}
            for _ in range(12):
                if fresh <= imap_view()[0]:
                    break
                time.sleep(10)
            else:
                raise h.Fail(f"IMAP LIST still lacks {sorted(fresh - imap_view()[0])} after 2 minutes")
            # The test account leaves Spam and Trash out of LSUB and mail-cat follows the
            # server's subscription, but the comparison expects the seeded Spam/Trash mail.
            sync()
            for box in ("[Gmail]/Spam", "[Gmail]/Cestino"):
                h.mc("mailboxes", "subscribe", acct(), box)
            synced_equal("before the rename")
            before = mc_state()
            ids, mcid = before[2], before[1]
            received = h.db("select count(*) from events where event_type = 'MessageReceived'")[0]

            def expect_tree(top, what):
                """mail-cat's mailboxes, Gmail's labels and the IMAP view agree on `top` and its child."""
                names = {top, f"{top}/Child"}
                now = mc_state()
                if {n: now[2].get(n) for n in names} != {top: ids[L("ItestRename")], f"{top}/Child": ids[L("ItestRename/Child")]}:
                    raise h.Fail(f"{what}: mail-cat mailboxes {sorted(n for n in now[2] if 'itest' in n.lower())}, "
                                 f"expected {sorted(names)} with the ids they had")
                if now[1] != mcid:
                    raise h.Fail(f"{what}: mail-cat reread or lost messages")
                if h.db("select count(*) from events where event_type = 'MessageReceived'")[0] != received:
                    raise h.Fail(f"{what}: messages were received again")
                have = {l["name"] for l in g.labels()}
                if not names <= have:
                    raise h.Fail(f"{what}: Gmail labels {sorted(n for n in have if 'itest' in n.lower())}")
                listed, per_message = imap_view()
                if not names <= listed:
                    raise h.Fail(f"{what}: IMAP LIST lacks {sorted(names - listed)}")
                for n in (101, 102, 103):
                    want = {api.SANDBOX, top, f"{top}/Child"}
                    imap_want = want | ({"\\Inbox"} if n == 103 else set())
                    if per_message.get(n) != imap_want:
                        raise h.Fail(f"{what}: message {n} has X-GM-LABELS {sorted(per_message.get(n, []))}, expected {sorted(imap_want)}")
                    if api_labels(n) != want | ({"INBOX"} if n == 103 else set()):
                        raise h.Fail(f"{what}: Gmail API labels of {n}: {sorted(api_labels(n))}")

            h.mc("mailboxes", "rename", acct(), L("ItestRename"), L("ItestRenamed"))
            expect_tree(L("ItestRenamed"), "after the rename (before any sync)")
            synced_equal("after the rename and a sync")
            expect_tree(L("ItestRenamed"), "after the rename and a sync")

            h.mc("mailboxes", "rename", acct(), L("ItestRenamed"), L("itestrenamed"))
            expect_tree(L("itestrenamed"), "after the case-only rename")
            synced_equal("after the case-only rename and a sync")
            expect_tree(L("itestrenamed"), "after the case-only rename and a sync")

            h.mc("labels", "rename", L("itestrenamed/Child"), L("ItestTarget"), "--account", acct(), "--yes")
            wait_for_job()
            h.mc("labels", "delete", L("itestrenamed"), "--account", acct(), "--yes")
            wait_for_job()
            synced_equal("after the merge and the delete")
            names = {l["name"] for l in g.labels()}
            left = {n for n in names if n.lower().startswith(L("itestrenamed").lower())}
            if left or L("ItestTarget") not in names:
                raise h.Fail(f"labels after merge and delete: {sorted(n for n in names if 'itest' in n.lower())}")
            for n in (101, 102, 103, 104):
                want = {api.SANDBOX, L("ItestTarget")} | ({"INBOX"} if n == 103 else set())
                if api_labels(n) != want:
                    raise h.Fail(f"message {n}: API has {sorted(api_labels(n))}, expected {sorted(want)}")
        finally:
            itest_cleanup()

    scenarios = [gmail_sync, gmail_labels_mc_to_server, gmail_server_to_mc, gmail_delete_permanent, gmail_model_default,
                 gmail_rename_tree]
    for s in scenarios:
        s.gmail = True
    return scenarios
