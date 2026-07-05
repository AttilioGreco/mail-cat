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
