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

export interface SearchHit {
  messageId: string;
  score: number;
  snippet: string | null;
  subject: string;
  fromName: string | null;
  fromEmail: string;
  date: number;
}

export interface Suggestion {
  field: string;
  value: string;
  docCount: number;
}

export interface ConversationSummary {
  threadId: string;
  accountId: string;
  subject: string;
  messageCount: number;
  unreadCount: number;
  lastMessageAt: string; // ISO 8601
  latestSender: string;
  /** Domain message UUID — pass this to get_thread, NOT threadId. */
  sampleMessageId: string;
}

export interface ThreadMessage {
  id: string;
  fromName: string | null;
  fromEmail: string;
  to: string[];
  subject: string;
  date: string; // ISO 8601
  isRead: boolean;
  htmlBody: string | null;
  textBody: string | null;
}
