"""Thin Gmail REST client: the independent oracle/seeder for the Gmail itests.

Every call that changes the mailbox goes through `Gmail.mutate`, which re-reads
the address from users.getProfile (must be the confirmed one) and refuses
anything outside the sandbox: messages must carry the label SANDBOX, labels must
be named PREFIX..., so real mail and real labels can never be touched.
"""
import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import subprocess

KEYCHAIN_SERVICE = "mail-cat"
CONFIRM_ENV = "MAILCAT_ITEST_GMAIL_CONFIRM"
BASE = "https://gmail.googleapis.com/gmail/v1/users/me/"
TOKEN_URL = "https://oauth2.googleapis.com/token"
SANDBOX = "mc-itest"
PREFIX = SANDBOX + "/"
IGNORED = ("UNREAD", "CATEGORY_")  # UNREAD is a flag in IMAP; categories are not exposed as labels


class GmailError(Exception):
    pass


class Refused(GmailError):
    """A safety guard refused to touch the mailbox."""


def check_address(confirm, actual):
    """Raise Refused unless the env confirmation names the mailbox the token opens.
    `actual` is None before the first profile read."""
    if not confirm:
        raise Refused(f"set {CONFIRM_ENV} to the address of the throwaway Gmail test mailbox")
    if actual == "":
        raise Refused("the profile returned an empty address")
    if actual is not None and actual.lower() != confirm.lower():
        raise Refused(f"the token opens {actual}, not the confirmed {confirm}")


def keychain_token(address):
    """Refresh token the app stored for exactly this address (one lookup, never a search).

    Reads the entry written by the keyring crate: service "mail-cat", username =
    the account email, value = JSON AccountSecrets with .oauth.refresh_token."""
    p = subprocess.run(["secret-tool", "lookup", "service", KEYCHAIN_SERVICE, "username", address],
                       capture_output=True, text=True)
    if p.returncode or not p.stdout.strip():
        raise Refused(f"no OAuth login for {address} in the keychain (service {KEYCHAIN_SERVICE!r}): "
                      f"run `mc accounts add-oauth google {address}` once, signing in as that mailbox")
    try:
        return json.loads(p.stdout)["oauth"]["refresh_token"]
    except (ValueError, KeyError, TypeError):
        raise Refused(f"the keychain entry for {address} holds no OAuth refresh token") from None


def available():
    """(ok, why not): confirmation set, keychain login present, Google client configured."""
    try:
        confirm = os.environ.get(CONFIRM_ENV)
        check_address(confirm, None)
        if not os.environ.get("MAILCAT_GOOGLE_CLIENT_ID"):
            raise Refused("set MAILCAT_GOOGLE_CLIENT_ID / MAILCAT_GOOGLE_CLIENT_SECRET (same client as the app)")
        keychain_token(confirm)
    except Refused as e:
        return False, str(e)
    return True, ""


def keep_label(label_id):
    return not label_id.startswith(IGNORED)


def b64url(raw):
    return base64.urlsafe_b64encode(raw).decode()


class Gmail:
    def __init__(self):
        self.confirm = os.environ.get(CONFIRM_ENV)
        check_address(self.confirm, None)
        self.address = self.confirm
        self.refresh_token = keychain_token(self.address)
        self._token, self._expires = None, 0

    # ── transport ────────────────────────────────────────────────────────
    def _access_token(self):
        if time.time() < self._expires - 60:
            return self._token
        body = urllib.parse.urlencode({
            "client_id": os.environ["MAILCAT_GOOGLE_CLIENT_ID"],
            "client_secret": os.environ.get("MAILCAT_GOOGLE_CLIENT_SECRET", ""),
            "refresh_token": self.refresh_token, "grant_type": "refresh_token"}).encode()
        tok = self._send(urllib.request.Request(TOKEN_URL, body))
        self._token, self._expires = tok["access_token"], time.time() + tok["expires_in"]
        return self._token

    def _send(self, req, retries=3):
        for attempt in range(retries + 1):
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    data = r.read()
                    return json.loads(data) if data else {}
            except urllib.error.HTTPError as e:
                if e.code in (429, 500, 503) and attempt < retries:
                    time.sleep(2 ** attempt)
                    continue
                raise GmailError(f"{req.get_method()} {req.full_url.split('?')[0]}: {e.code} {e.read().decode()[:300]}") from None

    def call(self, method, path, body=None, **query):
        url = BASE + path + (("?" + urllib.parse.urlencode(query, doseq=True)) if query else "")
        req = urllib.request.Request(url, json.dumps(body).encode() if body is not None else None, method=method)
        req.add_header("Authorization", f"Bearer {self._access_token()}")
        if body is not None:
            req.add_header("Content-Type", "application/json")
        return self._send(req)

    def mutate(self, method, path, body=None, *, messages=(), labels=(), new_label=None,
               insert_labels=None, **query):
        """The only way to change the mailbox. `messages`/`labels` are the existing
        targets (must be inside the sandbox); `new_label` a label name being
        created or renamed to; `insert_labels` the labels of a message being inserted."""
        check_address(self.confirm, self.profile()["emailAddress"] or "")
        names = {l["id"]: l["name"] for l in self.call("GET", "labels").get("labels", [])}
        sandbox = next((i for i, n in names.items() if n == SANDBOX), None)
        for mid in messages:
            have = self.call("GET", f"messages/{mid}", format="minimal").get("labelIds", [])
            if sandbox is None or sandbox not in have:
                raise Refused(f"message {mid} does not carry the {SANDBOX!r} label")
        for lid in labels:
            if not names.get(lid, "").startswith(PREFIX):
                raise Refused(f"label {names.get(lid, lid)!r} is outside {PREFIX!r}")
        if new_label is not None and new_label != SANDBOX and not new_label.startswith(PREFIX):
            raise Refused(f"label name {new_label!r} is outside {PREFIX!r}")
        if insert_labels is not None and (sandbox is None or sandbox not in insert_labels):
            raise Refused(f"a new message must carry the {SANDBOX!r} label")
        if sandbox is not None and sandbox in (body or {}).get("removeLabelIds", []):
            raise Refused(f"the {SANDBOX!r} label may not be removed from a message")
        return self.call(method, path, body, **query)

    # ── reads ────────────────────────────────────────────────────────────
    def profile(self):
        return self.call("GET", "profile")

    def labels(self):
        return self.call("GET", "labels").get("labels", [])

    def message_ids(self, **query):
        """Ids of every message, Spam and Trash included."""
        ids, token = [], None
        while True:
            page = self.call("GET", "messages", includeSpamTrash="true", maxResults=500,
                             **({"pageToken": token} if token else {}), **query)
            ids += [m["id"] for m in page.get("messages", [])]
            token = page.get("nextPageToken")
            if not token:
                return ids

    def get(self, mid):
        """{id, labelIds, message_id} (RFC Message-ID without <>), or None if gone."""
        try:
            m = self.call("GET", f"messages/{mid}", format="metadata", metadataHeaders="Message-ID")
        except GmailError as e:
            if " 404 " in str(e):
                return None
            raise
        hdr = {h["name"].lower(): h["value"] for h in m["payload"].get("headers", [])}
        return {"id": mid, "labelIds": m.get("labelIds", []), "message_id": hdr.get("message-id", "").strip("<>")}

    def history(self, start_history_id):
        out, token = [], None
        while True:
            page = self.call("GET", "history", startHistoryId=start_history_id,
                             **({"pageToken": token} if token else {}))
            out += page.get("history", [])
            token = page.get("nextPageToken")
            if not token:
                return out

    def snapshot(self, id_prefix):
        """{RFC Message-ID: set of Gmail label names} of the sandbox messages whose Message-ID starts with id_prefix."""
        labels = self.labels()
        names = {l["id"]: l["name"] for l in labels}
        sandbox = next(l["id"] for l in labels if l["name"] == SANDBOX)
        snap = {}
        for mid in self.message_ids(labelIds=[sandbox]):
            m = self.get(mid)
            if m and m["message_id"].startswith(id_prefix):
                snap[m["message_id"]] = {names.get(i, i) for i in m["labelIds"] if keep_label(i)}
        return snap

    def label_id(self, name):
        return next(l["id"] for l in self.labels() if l["name"] == name)

    def find(self, message_id):
        """Gmail id of the message with this RFC Message-ID, or None."""
        found = self.message_ids(q=f"rfc822msgid:{message_id}")
        return found[0] if found else None

    # ── writes (all through mutate) ──────────────────────────────────────
    def label_create(self, name):
        return self.mutate("POST", "labels", {"name": name, "labelListVisibility": "labelShow",
                                              "messageListVisibility": "show"}, new_label=name)["id"]

    def label_delete(self, label_id):
        self.mutate("DELETE", f"labels/{label_id}", labels=[label_id])

    def label_rename(self, label_id, name):
        self.mutate("PATCH", f"labels/{label_id}", {"name": name}, labels=[label_id], new_label=name)

    def insert(self, rfc822, label_ids):
        return self.mutate("POST", "messages", {"raw": b64url(rfc822), "labelIds": label_ids},
                           insert_labels=label_ids, internalDateSource="dateHeader")["id"]

    def modify(self, mid, add=(), remove=()):
        self.mutate("POST", f"messages/{mid}/modify", {"addLabelIds": list(add), "removeLabelIds": list(remove)},
                    messages=[mid])

    def trash(self, mid):
        self.mutate("POST", f"messages/{mid}/trash", messages=[mid])

    def untrash(self, mid):
        self.mutate("POST", f"messages/{mid}/untrash", messages=[mid])

    def delete(self, mid):
        self.mutate("DELETE", f"messages/{mid}", messages=[mid])

    def batch_delete(self, ids):
        for i in range(0, len(ids), 1000):
            chunk = ids[i:i + 1000]
            self.mutate("POST", "messages/batchDelete", {"ids": chunk}, messages=chunk)

    def batch_modify(self, ids, add=(), remove=()):
        self.mutate("POST", "messages/batchModify",
                    {"ids": ids, "addLabelIds": list(add), "removeLabelIds": list(remove)}, messages=ids)
