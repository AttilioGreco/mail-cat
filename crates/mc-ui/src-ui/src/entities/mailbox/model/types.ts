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
  | 'custom';

export interface Mailbox {
  id: string;
  accountId: string;
  path: string;
  name: string;
  kind: MailboxKind;
  totalMessages: number;
  unreadMessages: number;
  attributes: string[];
}
