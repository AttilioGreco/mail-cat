import { useEffect, useState } from 'react';
import type { Account, AuthState } from './types';

const PROVIDER_NAMES = { google: 'Google', microsoft: 'Microsoft' } as const;

/** The live state, falling back to the persisted status when the server runs no gate. */
export function authState(account: Account): AuthState {
  if (account.auth) return account.auth.state;
  if (account.status === 'needs_reauth' || account.status === 'credentials_rejected') return account.status;
  return account.status === 'active' ? 'active' : 'unknown';
}

/** What the account's auth state means to the user, in one short label. */
export function authLabel(account: Account, secondsLeft: number | null): string {
  switch (authState(account)) {
    case 'active':
      return 'OK';
    case 'offline':
      return secondsLeft === null ? 'Offline' : `Offline · nuovo tentativo tra ${secondsLeft}s`;
    case 'needs_reauth': {
      const provider = account.auth?.oauthProvider;
      return provider ? `Login richiesto (${PROVIDER_NAMES[provider]})` : 'Login richiesto';
    }
    case 'credentials_rejected':
      return 'Password rifiutata';
    case 'oauth_client_invalid':
      return 'Errore configurazione OAuth';
    default:
      return 'sconosciuto';
  }
}

/** Whole seconds until `iso`, ticking every second; null without a time. */
export function useSecondsUntil(iso: string | null | undefined): number | null {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!iso) return;
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [iso]);
  return iso ? Math.max(0, Math.ceil((Date.parse(iso) - now) / 1000)) : null;
}
