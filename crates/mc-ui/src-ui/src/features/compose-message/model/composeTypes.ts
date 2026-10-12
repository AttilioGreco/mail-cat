// Mirrors the backend compose contract (prepare_compose / send_compose /
// save_draft): the backend owns recipients, subject, quoting and identity
// choice, the UI only edits what it is handed.

export type ComposeMode = 'new' | 'reply' | 'replyAll' | 'forward' | 'draft';

export interface Identity {
  accountId: string;
  displayName: string;
  email: string;
  signatureHtml: string | null;
}

export interface ForwardedAttachment {
  id: string;
  filename: string;
  size: number;
  mimeType: string;
}

export interface ComposeDto {
  draftId: string | null;
  accountId: string;
  identities: Identity[];
  /** "Name <email>" or bare email. */
  to: string[];
  cc: string[];
  bcc: string[];
  subject: string;
  bodyMd: string;
  quotedHtml: string | null;
  signatureHtml: string | null;
  forwardedAttachments: ForwardedAttachment[];
  /** A reopened draft's inline images, referenced as cid:<contentId>; empty otherwise. */
  inlineImages: { contentId: string; mimeType: string; dataBase64: string }[];
  mode: ComposeMode;
  sourceMessageId: string | null;
}

export interface ComposeAttachment {
  filename: string;
  mimeType: string;
  /** Set for inline images, referenced as cid:<contentId> in bodyMd. */
  contentId: string | null;
  dataBase64: string;
}

export interface ComposeInput {
  draftId: string | null;
  accountId: string;
  to: string[];
  cc: string[];
  bcc: string[];
  subject: string;
  bodyMd: string;
  mode: ComposeMode;
  sourceMessageId: string | null;
  attachments: ComposeAttachment[];
  forwardedAttachmentIds: string[];
}

export interface InlineImage {
  cid: string;
  blob: Blob;
  mimeType: string;
  /** Object URL the editor's markdown points at until it is swapped for cid: on send. */
  url: string;
}
