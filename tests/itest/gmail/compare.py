"""Pure helpers to compare mail-cat's view of the Gmail mailbox with the API's."""

SYSTEM_BY_KIND = {"Inbox": "INBOX", "Sent": "SENT", "Drafts": "DRAFT", "Trash": "TRASH", "Spam": "SPAM",
                  "Important": "IMPORTANT"}
SYSTEM_IDS = set(SYSTEM_BY_KIND.values()) | {"STARRED"}


def label_name(mailbox):
    """Gmail label (system id or user label name) a mail-cat mailbox stands for;
    None for All Mail, which is every message rather than a label."""
    kind = mailbox["mailbox_type"]
    if kind == "All":
        return None
    if kind in SYSTEM_BY_KIND:
        return SYSTEM_BY_KIND[kind]
    # Names are localised ("[Gmail]/Speciali"); the only plain mailbox Gmail
    # keeps under [Gmail] is Starred (the product has no flag for it).
    parent, _, leaf = mailbox["path"].partition("/")
    if parent in ("[Gmail]", "[Google Mail]") and leaf:
        return "STARRED"
    return mailbox["display_path"]


def norm_message_id(rfc_message_id):
    return (rfc_message_id or "").strip().strip("<>")


def diff_labels(api, mc):
    """Human-readable differences between {message-id: {labels}} maps; [] when equal.
    Messages in Spam or Trash are synced as those folders, which carry no
    X-GM-LABELS: only their system labels are compared (documented limitation)."""
    api = {m: ({x for x in ls if x in SYSTEM_IDS} if ls & {"SPAM", "TRASH"} else ls) for m, ls in api.items()}
    out = [f"{m}: only in the API" for m in sorted(api.keys() - mc.keys())]
    out += [f"{m}: only in mail-cat" for m in sorted(mc.keys() - api.keys())]
    for m in sorted(api.keys() & mc.keys()):
        if api[m] != mc[m]:
            out.append(f"{m}: API {sorted(api[m])} vs mail-cat {sorted(mc[m])}")
    return out
