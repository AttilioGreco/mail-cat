import { useMutation, useQueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { Mailbox } from '@/entities/mailbox';
import type { MessageEnvelope } from '@/entities/message';

// ── Set Flags ───────────────────────────────────────────────────────────────

interface SetFlagsParams {
  messageId: string;
  flagsToAdd?: string[];
  flagsToRemove?: string[];
  /** Pass the current isRead state so the badge can be updated optimistically. */
  currentIsRead?: boolean;
  mailboxKey?: string; // "accountId::mailboxId"
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
    onMutate: (params) => {
      // Optimistically update the unread badge when toggling the seen flag.
      const markingRead = params.flagsToAdd?.includes('seen') ?? false;
      const markingUnread = params.flagsToRemove?.includes('seen') ?? false;
      if (!markingRead && !markingUnread) return;
      if (params.currentIsRead === undefined) return;

      // delta: -1 when marking read (was unread), +1 when marking unread (was read)
      const delta = markingRead && !params.currentIsRead ? -1
        : markingUnread && params.currentIsRead ? 1
        : 0;
      if (delta === 0) return;

      const [accountId] = params.mailboxKey?.split('::') ?? [];
      if (!accountId) return;

      // Update the mailboxes cache (sidebar badge)
      qc.setQueriesData<Mailbox[]>({ queryKey: ['mailboxes', accountId] }, (prev) =>
        prev?.map((mb) =>
          mb.id === params.mailboxKey?.split('::')[1]
            ? { ...mb, unreadMessages: Math.max(0, mb.unreadMessages + delta) }
            : mb,
        ),
      );

      // Update the messages cache (toolbar counter + filter)
      const newIsRead = markingRead;
      qc.setQueriesData<MessageEnvelope[]>({ queryKey: ['messages', params.mailboxKey] }, (prev) =>
        prev?.map((msg) =>
          msg.uid === params.messageId ? { ...msg, isRead: newIsRead } : msg,
        ),
      );
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['messages'] });
      qc.invalidateQueries({ queryKey: ['mailboxes'] });
      // Thread and conversation caches must also refresh so read indicators update
      qc.invalidateQueries({ queryKey: ['thread'] });
      qc.invalidateQueries({ queryKey: ['conversations'] });
    },
  });
}

/** Convenience: mark message as read. */
export function useMarkAsRead() {
  const setFlags = useSetFlags();
  return {
    ...setFlags,
    mutate: (messageId: string, mailboxKey?: string) =>
      setFlags.mutate({ messageId, flagsToAdd: ['seen'], currentIsRead: false, mailboxKey }),
    mutateAsync: (messageId: string, mailboxKey?: string) =>
      setFlags.mutateAsync({ messageId, flagsToAdd: ['seen'], currentIsRead: false, mailboxKey }),
  };
}

/** Convenience: mark message as unread. */
export function useMarkAsUnread() {
  const setFlags = useSetFlags();
  return {
    ...setFlags,
    mutate: (messageId: string, mailboxKey?: string) =>
      setFlags.mutate({ messageId, flagsToRemove: ['seen'], currentIsRead: true, mailboxKey }),
    mutateAsync: (messageId: string, mailboxKey?: string) =>
      setFlags.mutateAsync({ messageId, flagsToRemove: ['seen'], currentIsRead: true, mailboxKey }),
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
