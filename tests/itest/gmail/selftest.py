#!/usr/bin/env python3
"""Offline checks of the pure helpers and the safety guards: `python3 gmail/selftest.py`."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from gmail import api, compare, seed  # noqa: E402


def refused(f, *a):
    try:
        f(*a)
    except api.Refused:
        return True
    return False


def box(kind, path, display=None):
    return {"mailbox_type": kind, "path": path, "display_path": display or path}


# address guard: the env confirmation must be set and equal the live profile
api.check_address("t@gmail.com", None)
api.check_address("t@gmail.com", "T@gmail.com")
assert refused(api.check_address, None, None)
assert refused(api.check_address, "", "t@gmail.com")
assert refused(api.check_address, "t@gmail.com", "real@gmail.com")

# keychain lookup: one exact lookup, clear refusal when absent or without a token
calls = []
def fake_run(cmd, **k):
    calls.append(cmd)
    out = os.environ.get("FAKE_SECRET", "")
    return type("P", (), {"returncode": 0 if out else 1, "stdout": out})()
api.subprocess.run = fake_run
os.environ["FAKE_SECRET"] = ""
assert refused(api.keychain_token, "t@gmail.com")
os.environ["FAKE_SECRET"] = '{"imap_password":"x"}'
assert refused(api.keychain_token, "t@gmail.com")
os.environ["FAKE_SECRET"] = '{"oauth":{"access_token":"a","refresh_token":"r1","expires_at":"2026-01-01T00:00:00Z"}}'
assert api.keychain_token("t@gmail.com") == "r1"
assert all(c[:2] == ["secret-tool", "lookup"] for c in calls) and calls[-1][-1] == "t@gmail.com"

# sandbox: every write is checked against a fake in-memory mailbox
class Fake(api.Gmail):
    def __init__(self, live="t@gmail.com"):
        self.address, self.confirm, self.live, self.sent = "t@gmail.com", "t@gmail.com", live, []
        self.labels_ = [{"id": "L1", "name": "mc-itest"}, {"id": "L2", "name": "mc-itest/Café"},
                        {"id": "L3", "name": "Work"}, {"id": "L4", "name": "mc-itest-not"}]
        self.msgs = {"mine": ["L1", "INBOX"], "real": ["INBOX", "L3"]}

    def profile(self):
        return {"emailAddress": self.live}

    def call(self, method, path, body=None, **query):
        if method == "GET" and path == "labels":
            return {"labels": self.labels_}
        if method == "GET" and path.startswith("messages/"):
            return {"labelIds": self.msgs[path.split("/")[1]]}
        self.sent.append((method, path))
        return {"id": "new"}


def refuses(f, *a, **k):
    g = Fake()
    try:
        f(g, *a, **k)
    except api.Refused:
        return not g.sent
    return False


def allows(f, *a, **k):
    g = Fake()
    f(g, *a, **k)
    return len(g.sent) == 1


assert allows(api.Gmail.trash, "mine") and allows(api.Gmail.delete, "mine")
assert allows(api.Gmail.modify, "mine", add=["L3"], remove=["INBOX"])
assert refuses(api.Gmail.trash, "real") and refuses(api.Gmail.delete, "real")
assert refuses(api.Gmail.modify, "real", add=["L2"])
assert refuses(api.Gmail.modify, "mine", remove=["L1"])  # may not leave the sandbox
assert allows(api.Gmail.batch_delete, ["mine"]) and allows(api.Gmail.batch_modify, ["mine"], add=["L2"])
assert refuses(api.Gmail.batch_delete, ["mine", "real"]) and refuses(api.Gmail.batch_modify, ["real", "mine"])
assert allows(api.Gmail.label_delete, "L2") and allows(api.Gmail.label_rename, "L2", "mc-itest/New")
assert refuses(api.Gmail.label_delete, "L3") and refuses(api.Gmail.label_delete, "L1")
assert refuses(api.Gmail.label_delete, "L4")  # "mc-itest-not" is not under "mc-itest/"
assert refuses(api.Gmail.label_rename, "L2", "Top level") and refuses(api.Gmail.label_rename, "L3", "mc-itest/x")
assert allows(api.Gmail.label_create, "mc-itest/x") and allows(api.Gmail.label_create, "mc-itest")
assert refuses(api.Gmail.label_create, "Top level")
assert allows(api.Gmail.insert, b"x", ["L1", "INBOX"]) and refuses(api.Gmail.insert, b"x", ["INBOX"])
# wrong mailbox behind the token: nothing is sent
bad = Fake("real@gmail.com")
assert refused(bad.trash, "mine") and not bad.sent

# label names
assert compare.label_name(box("All", "[Gmail]/All Mail")) is None
assert compare.label_name(box("Inbox", "INBOX")) == "INBOX"
assert compare.label_name(box("Custom", "[Gmail]/Starred", "Starred")) == "STARRED"
assert compare.label_name(box("Important", "[Gmail]/Importanti")) == "IMPORTANT"
assert compare.label_name(box("Custom", "[Gmail]/Speciali")) == "STARRED"
assert compare.label_name(box("Custom", "[Gmail]", "[Gmail]")) == "[Gmail]"
assert compare.diff_labels({"1": {"SPAM", "mc-itest"}, "2": {"INBOX", "mc-itest"}}, {"1": {"SPAM"}, "2": {"INBOX", "mc-itest"}}) == []
assert compare.label_name(box("Custom", "Clienti/Caf&AOk-", "Clienti/Café")) == "Clienti/Café"
assert compare.norm_message_id(" <a@b> ") == "a@b"
assert not api.keep_label("UNREAD") and not api.keep_label("CATEGORY_PROMOTIONS") and api.keep_label("INBOX")

# diffing
a = {"1": {"INBOX"}, "2": {"A"}, "3": set()}
assert compare.diff_labels(a, dict(a)) == []
d = compare.diff_labels(a, {"1": {"INBOX", "X"}, "2": {"A"}, "4": set()})
assert len(d) == 3 and "3: only in the API" in d and "4: only in mail-cat" in d, d
print("ok")
