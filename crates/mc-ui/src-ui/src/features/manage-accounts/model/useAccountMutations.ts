import { useEffect, useState } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useSyncStore } from '@/entities/account';
import { useSessionStore } from '@/entities/session';

export interface ImapAccountInput {
  name: string;
  email: string;
  imapHost: string;
  imapPort: number;
  useTls: boolean;
  username: string;
  password: string;
  smtpHost: string | null;
  smtpPort: number | null;
}

export function useAddImapAccount() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (params: ImapAccountInput) => invoke('add_imap_account', { params }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['accounts'] }),
  });
}

export function useRemoveAccount() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (accountId: string) => invoke('remove_account', { accountId }),
    onSuccess: (_, accountId) => {
      if (useSessionStore.getState().selectedAccountId === accountId) {
        useSessionStore.setState({
          selectedAccountId: null,
          selectedMailboxId: null,
          selectedMessageId: null,
          selectedMessageIds: [],
          selectedThreadMessageId: null,
          selectedThreadIds: [],
        });
      }
      useSyncStore.getState().forget(accountId);
      // Keys carry the account id alone or inside a "account::mailbox" key.
      queryClient.removeQueries({
        predicate: (q) =>
          q.queryKey.some((k) => typeof k === 'string' && (k === accountId || k.startsWith(`${accountId}::`))),
      });
      queryClient.invalidateQueries({ queryKey: ['accounts'] });
    },
  });
}

/** Store a new password in the server keychain; the running account uses it at once. */
export function useSetAccountPassword() {
  return useMutation({
    mutationFn: ({ accountId, password }: { accountId: string; password: string }) =>
      invoke('set_account_password', { accountId, password }),
  });
}

/** Presentation only: the server stores nothing differently. */
export function useSetMailboxModel() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ accountId, labels }: { accountId: string; labels: boolean }) =>
      invoke('set_account_mailbox_model', { accountId, labels }),
    onSettled: () => queryClient.invalidateQueries({ queryKey: ['accounts'] }),
  });
}

export function useSyncNow() {
  return useMutation({
    mutationFn: (accountId: string) => invoke('sync_now', { accountId }),
  });
}

/** Restart the server: what an OAuth client the server rejected needs. */
export function useRestartServer() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => invoke('restart_server'),
    onSuccess: () => queryClient.invalidateQueries(),
  });
}

export type OAuthProviderId = 'google' | 'microsoft';

export interface OAuthEventPayload {
  kind: 'awaiting_browser' | 'succeeded' | 'failed';
  authorize_url: string | null;
  email: string | null;
  message: string | null;
}

/**
 * Drive an OAuth2 login. The browser is opened for us on `awaiting_browser`;
 * the URL is returned too, so the UI can offer it when there is no handler.
 */
export function useOAuthLogin() {
  const queryClient = useQueryClient();
  const [event, setEvent] = useState<OAuthEventPayload | null>(null);
  const [pending, setPending] = useState(false);

  useEffect(() => {
    let unlisten: (() => void) | null = null;
    listen<OAuthEventPayload>('oauth:event', (e) => {
      setEvent(e.payload);
      if (e.payload.kind !== 'awaiting_browser') {
        setPending(false);
      }
      if (e.payload.kind === 'succeeded') {
        queryClient.invalidateQueries({ queryKey: ['accounts'] });
      }
    }).then((fn) => {
      unlisten = fn;
    });
    return () => unlisten?.();
  }, [queryClient]);

  const run = async (command: string, args: Record<string, string>) => {
    setEvent(null);
    setPending(true);
    try {
      await invoke(command, args);
    } catch (e) {
      setPending(false);
      setEvent({
        kind: 'failed',
        authorize_url: null,
        email: null,
        message: String(e),
      });
    }
  };

  const start = (name: string, email: string, provider: OAuthProviderId) =>
    run('start_oauth_login', { name, email, provider });

  /**
   * Re-authenticate an existing account. Only the id is needed — the server
   * knows which OAuth provider the account was registered with, which the UI
   * cannot infer from the address alone.
   */
  const reauth = (accountId: string) => run('reauth_account', { accountId });

  return { start, reauth, event, pending, reset: () => setEvent(null) };
}
