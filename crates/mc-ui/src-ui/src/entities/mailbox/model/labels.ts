import { labelKey } from '@/shared/lib/label-colors';
import type { Mailbox } from './types';

/** Kinds that are views of mail, not labels worth a chip on every message. */
const NOISE: ReadonlySet<Mailbox['kind']> = new Set(['inbox', 'sent', 'drafts', 'trash', 'spam', 'archive', 'all']);

export interface LabelChipData {
  name: string;
  /** Palette key, when the label has a color. */
  color?: string;
}

/** The labels to show on a message: the custom ones (Important and Starred
 *  included), minus the mailbox already being viewed. */
export function labelChips(
  mailboxes: Mailbox[],
  mailboxIds: string[],
  viewedId: string | null,
  colors: Readonly<Record<string, string>>,
): LabelChipData[] {
  return mailboxes
    .filter((m) => mailboxIds.includes(m.id) && m.id !== viewedId && !NOISE.has(m.kind))
    .map((m) => ({ name: m.name, color: colors[labelKey(m.displayPath)] }));
}

/** What the sidebar and the label picker offer: unsubscribed mailboxes are neither shown nor synced. */
export function subscribedOnly(mailboxes: Mailbox[]): Mailbox[] {
  return mailboxes.filter((m) => m.subscribed);
}
