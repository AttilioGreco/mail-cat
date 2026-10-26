export interface SearchHit {
  messageId: string;
  /** Every mailbox the message is in. */
  mailboxIds: string[];
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
