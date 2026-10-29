import { useEffect } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import type { Account, AccountAuth, AccountStats, AccountStatus, AuthState, MailboxModel, OAuthProviderId } from './types';

interface AuthStatusDto {
  account_id: string;
  state: AuthState;
  reason: string | null;
  last_error: string | null;
  since: string | null;
  next_retry: string | null;
  oauth_provider: OAuthProviderId | null;
  token_expires_at: string | null;
  smtp_error: string | null;
}

interface AccountDto {
  id: string;
  name: string;
  email: string;
  status: AccountStatus;
  mailbox_model: MailboxModel;
  supports_tags: boolean | null;
  auth: AuthStatusDto | null;
}

function authFromDto(d: AuthStatusDto): AccountAuth {
  return {
    state: d.state,
    reason: d.reason,
    lastError: d.last_error,
    since: d.since,
    nextRetry: d.next_retry,
    oauthProvider: d.oauth_provider,
    tokenExpiresAt: d.token_expires_at,
    smtpError: d.smtp_error,
  };
}

async function fetchAccounts(): Promise<Account[]> {
  const dtos = await invoke<AccountDto[]>('list_accounts');
  return dtos.map((d) => ({
    id: d.id,
    name: d.name,
    email: d.email,
    status: d.status,
    mailboxModel: d.mailbox_model,
    supportsTags: d.supports_tags,
    auth: d.auth && authFromDto(d.auth),
  }));
}

export function useAccounts() {
  return useQuery({
    queryKey: ['accounts'],
    queryFn: fetchAccounts,
    staleTime: 60_000,
  });
}

/**
 * Keeps each account's auth state live: the server pushes every change, which
 * is patched straight into the cached account list instead of waiting for it
 * to go stale.
 */
export function useAuthStatusWatch() {
  const queryClient = useQueryClient();

  useEffect(() => {
    invoke('start_auth_watch').catch((e) => console.error('[auth] start_auth_watch failed:', e));

    // StrictMode unmounts before `listen` resolves: drop the late listener.
    let cancelled = false;
    let unlisten: (() => void) | null = null;
    listen<AuthStatusDto>('account:auth', ({ payload }) => {
      const accounts = queryClient.getQueryData<Account[]>(['accounts']);
      if (!accounts?.some((a) => a.id === payload.account_id)) {
        queryClient.invalidateQueries({ queryKey: ['accounts'] });
        return;
      }
      queryClient.setQueryData<Account[]>(['accounts'], (current) =>
        current?.map((a) => (a.id === payload.account_id ? { ...a, auth: authFromDto(payload) } : a)),
      );
    }).then((fn) => {
      if (cancelled) fn();
      else unlisten = fn;
    });

    return () => {
      cancelled = true;
      invoke('stop_auth_watch').catch(() => {});
      unlisten?.();
    };
  }, [queryClient]);
}

interface AccountStatsDto {
  total_messages: number;
  synced_messages: number;
  pending_bodies: number;
}

/**
 * Download counters for one account. Each call scans the server's read model, so
 * it is fetched on demand (the settings view) rather than kept fresh everywhere.
 */
export function useAccountStats(accountId: string | null) {
  return useQuery({
    queryKey: ['account-stats', accountId],
    enabled: !!accountId,
    queryFn: async (): Promise<AccountStats> => {
      const dto = await invoke<AccountStatsDto>('account_stats', { accountId });
      return {
        totalMessages: dto.total_messages,
        syncedMessages: dto.synced_messages,
        pendingBodies: dto.pending_bodies,
      };
    },
  });
}
