"""Pure helpers to compare mail-cat's view of the Gmail mailbox with the API's."""

SYSTEM_BY_KIND = {"Inbox": "INBOX", "Sent": "SENT", "Drafts": "DRAFT", "Trash": "TRASH", "Spam": "SPAM"}
SYSTEM_VIEWS = {"important": "IMPORTANT", "starred": "STARRED"}


def label_name(mailbox):
    """Gmail label (system id or user label name) a mail-cat mailbox stands for;
    None for All Mail, which is every message rather than a label."""
    kind = mailbox["mailbox_type"]
    if kind == "All":
        return None
    if kind in SYSTEM_BY_KIND:
        return SYSTEM_BY_KIND[kind]
    parent, _, leaf = mailbox["path"].partition("/")
    if parent in ("[Gmail]", "[Google Mail]") and leaf.lower() in SYSTEM_VIEWS:
        return SYSTEM_VIEWS[leaf.lower()]
    return mailbox["display_path"]


def norm_message_id(rfc_message_id):
    return (rfc_message_id or "").strip().strip("<>")


def diff_labels(api, mc):
    """Human-readable differences between {message-id: {labels}} maps; [] when equal."""
    out = [f"{m}: only in the API" for m in sorted(api.keys() - mc.keys())]
    out += [f"{m}: only in mail-cat" for m in sorted(mc.keys() - api.keys())]
    for m in sorted(api.keys() & mc.keys()):
        if api[m] != mc[m]:
            out.append(f"{m}: API {sorted(api[m])} vs mail-cat {sorted(mc[m])}")
    return out
