"""Pure helpers to compare mail-cat's view of the Gmail mailbox with the API's."""

SYSTEM_BY_KIND = {"Inbox": "INBOX", "Sent": "SENT", "Drafts": "DRAFT", "Trash": "TRASH", "Spam": "SPAM",
                  "Important": "IMPORTANT", "Starred": "STARRED"}
SYSTEM_IDS = set(SYSTEM_BY_KIND.values())


def label_name(mailbox):
    """Gmail label (system id or user label name) a mail-cat mailbox stands for;
    None for All Mail, which is every message rather than a label."""
    kind = mailbox["mailbox_type"]
    if kind == "All":
        return None
    if kind in SYSTEM_BY_KIND:
        return SYSTEM_BY_KIND[kind]
    return mailbox["display_path"]


def norm_message_id(rfc_message_id):
    return (rfc_message_id or "").strip().strip("<>")


def diff_labels(api, mc):
    """Human-readable differences between {message-id: {labels}} maps; [] when equal.
    Messages in Spam or Trash are synced as those folders, which carry no
    X-GM-LABELS: only SPAM and TRASH are compared (documented limitation)."""
    outside = {"SPAM", "TRASH"}
    api = {m: (ls & outside if ls & outside else ls) for m, ls in api.items()}
    mc = {m: (ls & outside if ls & outside else ls) for m, ls in mc.items()}
    out = [f"{m}: only in the API" for m in sorted(api.keys() - mc.keys())]
    out += [f"{m}: only in mail-cat" for m in sorted(mc.keys() - api.keys())]
    for m in sorted(api.keys() & mc.keys()):
        if api[m] != mc[m]:
            out.append(f"{m}: API {sorted(api[m])} vs mail-cat {sorted(mc[m])}")
    return out
