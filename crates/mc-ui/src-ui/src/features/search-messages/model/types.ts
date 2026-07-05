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
