// ── Identità mittente ────────────────────────────────────────────────────────
// Derivata dagli account configurati — vedi useIdentities.ts. Non è una entity
// a sé stante: è usata solo qui, dentro compose-message.

export interface Identity {
  id: string;
  displayName: string;
  email: string;
  signatureHtml?: string;
}

// ── Stato header compositore ─────────────────────────────────────────────────

export interface HeaderState {
  from: Identity;
  to: string[];
  cc: string[];
  bcc: string[];
  subject: string;
}

// ── Dati iniziali passati alla modale ────────────────────────────────────────

export interface ComposeInitialData {
  mode: 'new' | 'reply' | 'forward';
  /** Bare email of the sender identity to pre-select. */
  from?: string;
  /** Account ID owning this compose session — drives chip search scope. */
  accountId?: string;
  to?: string[];
  cc?: string[];
  bcc?: string[];
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
