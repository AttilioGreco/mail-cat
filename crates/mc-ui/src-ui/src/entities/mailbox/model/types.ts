export interface Mailbox {
  id: string;
  accountId: string;
  path: string;
  name: string;
  totalMessages: number;
  unreadMessages: number;
  attributes: string[];
}
