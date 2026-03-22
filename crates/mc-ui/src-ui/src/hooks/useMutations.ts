import { useMutation, useQueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';

// ── Set Flags ───────────────────────────────────────────────────────────────

interface SetFlagsParams {
  messageId: string;
  flagsToAdd?: string[];
  flagsToRemove?: string[];
}

export function useSetFlags() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: SetFlagsParams) =>
      invoke('set_flags', {
        messageId: params.messageId,
        flagsToAdd: params.flagsToAdd ?? [],
        flagsToRemove: params.flagsToRemove ?? [],
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['messages'] });
    },
  });
}

/** Convenience: mark message as read. */
export function useMarkAsRead() {
  const setFlags = useSetFlags();
  return {
    ...setFlags,
    mutate: (messageId: string) =>
      setFlags.mutate({ messageId, flagsToAdd: ['seen'] }),
    mutateAsync: (messageId: string) =>
      setFlags.mutateAsync({ messageId, flagsToAdd: ['seen'] }),
  };
}

/** Convenience: mark message as unread. */
export function useMarkAsUnread() {
  const setFlags = useSetFlags();
  return {
    ...setFlags,
    mutate: (messageId: string) =>
      setFlags.mutate({ messageId, flagsToRemove: ['seen'] }),
    mutateAsync: (messageId: string) =>
      setFlags.mutateAsync({ messageId, flagsToRemove: ['seen'] }),
  };
}

/** Convenience: toggle star (flagged). */
export function useToggleStar() {
  const setFlags = useSetFlags();
  return {
    ...setFlags,
    star: (messageId: string) =>
      setFlags.mutate({ messageId, flagsToAdd: ['flagged'] }),
    unstar: (messageId: string) =>
      setFlags.mutate({ messageId, flagsToRemove: ['flagged'] }),
  };
}

// ── Move Message ────────────────────────────────────────────────────────────

export function useMoveMessage() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: { messageId: string; toMailboxId: string }) =>
      invoke('move_message', {
        messageId: params.messageId,
        toMailboxId: params.toMailboxId,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['messages'] });
      qc.invalidateQueries({ queryKey: ['mailboxes'] });
    },
  });
}

// ── Delete Message ──────────────────────────────────────────────────────────

export function useDeleteMessage() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: { messageId: string; permanently?: boolean }) =>
      invoke('delete_message', {
        messageId: params.messageId,
        permanently: params.permanently ?? false,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['messages'] });
      qc.invalidateQueries({ queryKey: ['mailboxes'] });
    },
  });
}

// ── Send Message ────────────────────────────────────────────────────────────

interface SendParams {
  accountId: string;
  fromEmail: string;
  to: string[];
  cc?: string[];
  bcc?: string[];
  subject: string;
  bodyText?: string;
  bodyHtml?: string;
  inReplyTo?: string;
  references?: string[];
}

export function useSendMessage() {
  return useMutation({
    mutationFn: (params: SendParams) =>
      invoke<string>('send_message', {
        accountId: params.accountId,
        fromEmail: params.fromEmail,
        to: params.to,
        cc: params.cc ?? [],
        bcc: params.bcc ?? [],
        subject: params.subject,
        bodyText: params.bodyText ?? null,
        bodyHtml: params.bodyHtml ?? null,
        inReplyTo: params.inReplyTo ?? null,
        references: params.references ?? [],
      }),
  });
}

// ── Search ──────────────────────────────────────────────────────────────────

export interface SearchHitResult {
  message_id: string;
  score: number;
  snippet: string | null;
}

export function useSearch() {
  return useMutation({
    mutationFn: (params: { query: string; accountId?: string; mailboxId?: string }) =>
      invoke<SearchHitResult[]>('search_messages', {
        query: params.query,
        accountId: params.accountId ?? null,
        mailboxId: params.mailboxId ?? null,
      }),
  });
}
