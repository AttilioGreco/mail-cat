/** RFC 6154 special-use classification. Folder names are localized, so this is
 *  the only reliable way to find "the Sent folder". */
export type MailboxKind =
  | 'inbox'
  | 'sent'
  | 'drafts'
  | 'trash'
  | 'spam'
  | 'archive'
  | 'all'
  | 'important'
  | 'starred'
  | 'custom';

export interface Mailbox {
  id: string;
  accountId: string;
  /** Opaque provider id; never display or parse. */
  path: string;
  displayPath: string;
  name: string;
  kind: MailboxKind;
  totalMessages: number;
  unreadMessages: number;
  /** Shown in the sidebar and synced. Unsubscribed ones appear only in the folder dialog. */
  subscribed: boolean;
  attributes: string[];
}
