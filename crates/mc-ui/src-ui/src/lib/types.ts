export interface Account {
  id: string;
  name: string;
  email: string;
  isActive: boolean;
}

export interface Mailbox {
  id: string;
  accountId: string;
  path: string;
  name: string;
  totalMessages: number;
  unreadMessages: number;
  attributes: string[];
}

export interface MessageEnvelope {
  uid: string; // server UUID
  subject: string;
  fromName: string;
  fromEmail: string;
  toAddresses: string[];
  date: string; // ISO 8601
  isRead: boolean;
  hasAttachment: boolean;
  preview: string;
}

export interface MessageBody {
  uid: string;
  htmlBody: string | null;
  textBody: string | null;
}
