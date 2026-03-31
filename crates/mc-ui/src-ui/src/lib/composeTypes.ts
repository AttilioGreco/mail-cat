// ── Address entry ─────────────────────────────────────────────────────────────
// Used everywhere instead of raw "Name <email>" strings.

export interface AddressEntry {
  email: string;
  name?: string;
}

// ── Identità mittente ────────────────────────────────────────────────────────

export interface Identity {
  id: string;
  displayName: string;
  email: string;
  signatureHtml?: string;
}

// ── Stato header compositore ─────────────────────────────────────────────────

export interface HeaderState {
  from: Identity;
  to: AddressEntry[];
  cc: AddressEntry[];
  bcc: AddressEntry[];
  subject: string;
}

// ── Dati iniziali passati alla modale ────────────────────────────────────────

export interface ComposeInitialData {
  mode: 'new' | 'reply' | 'forward';
  /** Bare email of the sender identity to pre-select. */
  from?: string;
  /** Account ID owning this compose session — drives chip search scope. */
  accountId?: string;
  to?: AddressEntry[];
  cc?: AddressEntry[];
  bcc?: AddressEntry[];
  subject?: string;
  quotedHtml?: string;
  inReplyTo?: string;
  references?: string[];
  identities: Identity[];
}

// ── Immagini inline CID ──────────────────────────────────────────────────────

export interface InlineImage {
  cid: string;
  blob: Blob;
  mimeType: string;
}

// ── Bozza persistita ─────────────────────────────────────────────────────────

export interface Draft {
  id: string;
  savedAt: string;
  header: HeaderState;
  bodyMd: string;
  inlineImages: Array<{
    cid: string;
    filePath: string;
    mimeType: string;
  }>;
}

// ── Payload invio ────────────────────────────────────────────────────────────

export interface SendPayload {
  header: HeaderState;
  bodyMd: string;
  quotedHtml?: string;
  signatureHtml?: string;
  inlineImages: InlineImage[];
  attachments: File[];
}
