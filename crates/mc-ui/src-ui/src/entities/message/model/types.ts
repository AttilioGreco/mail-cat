export interface MessageEnvelope {
  uid: string; // server UUID
  /** Every mailbox (label) the message is in. */
  mailboxIds: string[];
  tags: string[];
  subject: string;
  fromName: string;
  fromEmail: string;
  toAddresses: string[];
  date: string; // ISO 8601
  isRead: boolean;
  isFlagged: boolean;
  hasAttachment: boolean;
  preview: string;
}

export interface MessageBody {
  uid: string;
  tags: string[];
  htmlBody: string | null;
  textBody: string | null;
  rfcMessageId: string | null;
  inReplyTo: string | null;
  references: string[];
}
