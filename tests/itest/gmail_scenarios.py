"""Gmail scenarios: mail-cat (Gmail over IMAP, labels) against the Gmail REST API.

The API is the oracle: expectations come from it, never from IMAP. Only run
when MAILCAT_ITEST_GMAIL_CONFIRM names the test address and the app's keychain
holds a login for it (see README). They only touch messages seeded with the
Message-ID prefix mcitest-. `make(h)` takes the scenarios module `h` for
its helpers, which avoids importing scenarios.py from here.
"""
import json
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

    scenarios = [gmail_sync, gmail_labels_mc_to_server, gmail_server_to_mc, gmail_delete_permanent, gmail_model_default]
    for s in scenarios:
        s.gmail = True
    return scenarios
