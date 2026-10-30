import type { Mailbox } from './types';

/** Kinds that are views of mail, not labels worth a chip on every message. */
const NOISE: ReadonlySet<Mailbox['kind']> = new Set(['inbox', 'sent', 'drafts', 'trash', 'spam', 'archive', 'all']);

/** Names of the labels to show on a message: the custom ones (Important and
 *  Starred included), minus the mailbox already being viewed. */
export function labelNames(mailboxes: Mailbox[], mailboxIds: string[], viewedId: string | null): string[] {
  return mailboxes
    .filter((m) => mailboxIds.includes(m.id) && m.id !== viewedId && !NOISE.has(m.kind))
    .map((m) => m.name);
}

/** What the sidebar and the label picker offer: unsubscribed mailboxes are neither shown nor synced. */
export function subscribedOnly(mailboxes: Mailbox[]): Mailbox[] {
  return mailboxes.filter((m) => m.subscribed);
}
