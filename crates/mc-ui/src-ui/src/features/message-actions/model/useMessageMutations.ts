import { useMutation, useQueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { ConversationSummary, ThreadMessage } from '@/entities/conversation';
import type { Mailbox } from '@/entities/mailbox';
import type { MessageEnvelope } from '@/entities/message';
import { useSessionStore, mailboxKey } from '@/entities/session';

// ── Optimistic removal ──────────────────────────────────────────────────────

type ListSnapshot = [readonly unknown[], unknown][];

/** Every cached view that lists messages. */
const MESSAGE_LISTS = ['messages', 'conversations', 'search', 'thread'] as const;

/** Moves and deletes now wait for the server, so the rows leave every cached
 *  list at once and come back if the server refuses. */
async function removeFromLists(qc: ReturnType<typeof useQueryClient>, ids: string[]): Promise<ListSnapshot> {
  const gone = new Set(ids);
  await Promise.all(MESSAGE_LISTS.map((key) => qc.cancelQueries({ queryKey: [key] })));
  const snapshot = MESSAGE_LISTS.flatMap((key) => qc.getQueriesData({ queryKey: [key] }));
  qc.setQueriesData<MessageEnvelope[]>({ queryKey: ['messages'] }, (list) =>
    list?.filter((m) => !gone.has(m.uid)),
  );
  qc.setQueriesData<{ messageId: string }[]>({ queryKey: ['search'] }, (hits) =>
    hits?.filter((h) => !gone.has(h.messageId)),
  );
  // A conversation row counts only its own mailbox's messages, while a cached
  // thread spans every mailbox and doesn't say which member lives where. So
  // a row only loses its sample message, the one member known to be its own;
  // the refetch settles the rest. A row left empty goes.
  const threadMembers = qc.getQueriesData<ThreadMessage[]>({ queryKey: ['thread'] }).flatMap(([, t]) => t ?? []);
  qc.setQueriesData<ConversationSummary[]>({ queryKey: ['conversations'] }, (rows) =>
    rows?.flatMap((c) => {
      if (!gone.has(c.sampleMessageId)) return [c];
      if (c.messageCount <= 1) return [];
      const sample = threadMembers.find((m) => m.id === c.sampleMessageId);
      const unread = sample && !sample.isRead ? 1 : 0;
      return [{ ...c, messageCount: c.messageCount - 1, unreadCount: Math.max(0, c.unreadCount - unread) }];
    }),
  );
  // After the conversations: they read the threads as they were.
  qc.setQueriesData<ThreadMessage[]>({ queryKey: ['thread'] }, (thread) =>
    thread?.filter((m) => !gone.has(m.id)),
  );
  return snapshot;
}

function restoreLists(qc: ReturnType<typeof useQueryClient>, snapshot: ListSnapshot | undefined) {
  snapshot?.forEach(([key, list]) => qc.setQueryData(key, list));
}

// ── Selection follow-through ────────────────────────────────────────────────

/**
 * After a message leaves the current list (deleted, archived, moved to spam),
 * move the selection to the row that takes its place instead of dropping it:
 * the next one down, or the previous one when the deleted message was last.
 *
 * Call this *before* the mutation invalidates the cache — it needs the list as
 * it still is to know what "next" was. Pass `list` when the visible list is
 * filtered (unread-only): the cache holds the unfiltered one, and picking a
 * neighbour out of that would select a row the user cannot see.
 */
export function useSelectNeighbour() {
  const qc = useQueryClient();
  const selectedAccountId = useSessionStore((s) => s.selectedAccountId);
  const selectedMailboxId = useSessionStore((s) => s.selectedMailboxId);
  const selectMessage = useSessionStore((s) => s.selectMessage);

  return (messageId: string, list?: MessageEnvelope[]) => {
    const messages =
      list ??
      qc.getQueryData<MessageEnvelope[]>([
        'messages',
        mailboxKey(selectedAccountId, selectedMailboxId),
      ]) ??
      [];
    const at = messages.findIndex((m) => m.uid === messageId);
    // Not in the list (stale view, filtered out): clearing beats keeping a
    // selection that points at a message about to disappear.
    if (at === -1) {
      selectMessage(null);
      return;
    }
    const neighbour = messages[at + 1] ?? (at > 0 ? messages[at - 1] : undefined);
    selectMessage(neighbour?.uid ?? null);
  };
}

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

// ── Mark Mailbox Read ───────────────────────────────────────────────────────

export function useMarkMailboxRead() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (mailboxId: string) => invoke<number>('mark_mailbox_read', { mailboxId }),
    onMutate: (mailboxId) => {
      // Optimistically zero the unread badge for this mailbox across all accounts' caches.
      qc.setQueriesData<Mailbox[]>({ queryKey: ['mailboxes'] }, (prev) =>
        prev?.map((mb) => (mb.id === mailboxId ? { ...mb, unreadMessages: 0 } : mb)),
      );
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['messages'] });
      qc.invalidateQueries({ queryKey: ['mailboxes'] });
      qc.invalidateQueries({ queryKey: ['thread'] });
      qc.invalidateQueries({ queryKey: ['conversations'] });
    },
  });
}

// ── Move Message ────────────────────────────────────────────────────────────

export function useMoveMessage() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: { messageId: string; fromMailboxId: string; toMailboxId: string }) =>
      invoke('move_message', {
        messageId: params.messageId,
        fromMailboxId: params.fromMailboxId,
        toMailboxId: params.toMailboxId,
      }),
    onMutate: (params) => removeFromLists(qc, [params.messageId]),
    onError: (_e, _params, snapshot) => restoreLists(qc, snapshot),
    onSettled: () => invalidateMessageViews(qc),
  });
}

// ── Bulk actions ────────────────────────────────────────────────────────────

/** A message and the mailbox a bulk action takes it from. */
export interface MessageRef {
  messageId: string;
  mailboxId: string;
}

/** `[succeeded, failed]`. The backend walks past messages it cannot touch
 *  (already gone, stale search index) instead of aborting the batch, so a
 *  non-zero `failed` is a normal outcome the caller must surface. */
export type BulkResult = [number, number];

function invalidateMessageViews(qc: ReturnType<typeof useQueryClient>) {
  for (const key of [...MESSAGE_LISTS, 'mailboxes']) qc.invalidateQueries({ queryKey: [key] });
}

/** Delete many messages in one provider round trip. */
export function useDeleteMessages() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: { messages: MessageRef[]; permanently?: boolean }) =>
      invoke<BulkResult>('delete_messages', {
        messages: params.messages,
        permanently: params.permanently ?? false,
      }),
    onMutate: (params) => removeFromLists(qc, params.messages.map((m) => m.messageId)),
    onError: (_e, _params, snapshot) => restoreLists(qc, snapshot),
    onSettled: () => invalidateMessageViews(qc),
  });
}

/** Move many messages to one mailbox in one provider round trip. */
export function useMoveMessages() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: { messages: MessageRef[]; toMailboxId: string }) =>
      invoke<BulkResult>('move_messages', {
        messages: params.messages,
        toMailboxId: params.toMailboxId,
      }),
    onMutate: (params) => removeFromLists(qc, params.messages.map((m) => m.messageId)),
    onError: (_e, _params, snapshot) => restoreLists(qc, snapshot),
    onSettled: () => invalidateMessageViews(qc),
  });
}

// ── Delete Message ──────────────────────────────────────────────────────────

export function useDeleteMessage() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: { messageId: string; mailboxId: string; permanently?: boolean }) =>
      invoke('delete_message', {
        messageId: params.messageId,
        mailboxId: params.mailboxId,
        permanently: params.permanently ?? false,
      }),
    onMutate: (params) => removeFromLists(qc, [params.messageId]),
    onError: (_e, _params, snapshot) => restoreLists(qc, snapshot),
    onSettled: () => invalidateMessageViews(qc),
  });
}
