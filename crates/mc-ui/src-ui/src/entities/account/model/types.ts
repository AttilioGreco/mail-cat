/** Account health as reported by the server. */
export type AccountStatus = 'active' | 'disabled' | 'error' | 'needs_reauth' | 'unknown';

export interface Account {
  id: string;
  name: string;
  email: string;
  status: AccountStatus;
}

/** How much of an account is stored locally. */
export interface AccountStats {
  /** Messages the server reports across all known folders. */
  totalMessages: number;
  /** Envelopes stored locally. */
  syncedMessages: number;
  /** Stored envelopes whose body is still queued for download. */
  pendingBodies: number;
}
