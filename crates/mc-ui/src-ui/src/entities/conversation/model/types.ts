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
  rfcMessageId: string | null;
  fromName: string | null;
  fromEmail: string;
  to: string[];
  cc: string[];
  subject: string;
  date: string; // ISO 8601
  isRead: boolean;
  htmlBody: string | null;
  textBody: string | null;
}
