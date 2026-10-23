/** Account health as persisted by the server. */
export type AccountStatus =
  | 'active'
  | 'disabled'
  | 'error'
  | 'needs_reauth'
  | 'credentials_rejected'
  | 'unknown';

/** Live auth state, owned by the server's per-account auth gate. */
export type AuthState =
  | 'active'
  | 'offline'
  | 'needs_reauth'
  | 'credentials_rejected'
  | 'oauth_client_invalid'
  | 'unknown';

export type OAuthProviderId = 'google' | 'microsoft';

export interface AccountAuth {
  state: AuthState;
  /** Why the account is in `state`; null while active. */
  reason: string | null;
  /** The most recent failure, kept after the account recovers. */
  lastError: string | null;
  since: string | null;
  /** When an offline account is retried (RFC 3339). */
  nextRetry: string | null;
  /** null for a password account. */
  oauthProvider: OAuthProviderId | null;
  tokenExpiresAt: string | null;
}

export interface Account {
  id: string;
  name: string;
  email: string;
  status: AccountStatus;
  /** null when the server runs no auth gate for the account. */
  auth: AccountAuth | null;
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
