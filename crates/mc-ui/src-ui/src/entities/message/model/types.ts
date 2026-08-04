export interface MessageEnvelope {
  uid: string; // server UUID
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
  htmlBody: string | null;
  textBody: string | null;
  rfcMessageId: string | null;
  inReplyTo: string | null;
  references: string[];
}
