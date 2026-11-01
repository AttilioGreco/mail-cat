import { useMutation, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { MessageEnvelope } from '@/entities/message';

/** Every cached view that shows labels or tags. Label changes arrive on the
 *  bus but WatchMailbox doesn't carry them, so the views are re-queried. */
function requery(qc: QueryClient) {
  for (const key of ['messages', 'conversations', 'thread', 'search', 'mailboxes', 'background-stats'])
    qc.invalidateQueries({ queryKey: [key] });
}

function patchMessages(qc: QueryClient, ids: string[], patch: (m: MessageEnvelope) => Partial<MessageEnvelope>) {
  qc.setQueriesData<MessageEnvelope[]>({ queryKey: ['messages'] }, (list) =>
    list?.map((m) => (ids.includes(m.uid) ? { ...m, ...patch(m) } : m)),
  );
}

interface ChangeLabelsParams {
  messageIds: string[];
  add?: string[];
  remove?: string[];
}

/** Rejects when the server refuses (e.g. removing a message's last label). */
export function useChangeLabels() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async ({ messageIds, add = [], remove = [] }: ChangeLabelsParams) => {
      const [, failed] = await invoke<[number, number]>('change_labels', {
        messageIds,
        addMailboxIds: add,
        removeMailboxIds: remove,
      });
      if (failed > 0) throw new Error('Modifica etichette non riuscita');
    },
    onMutate: ({ messageIds, add = [], remove = [] }) =>
      patchMessages(qc, messageIds, (m) => ({
        mailboxIds: [...m.mailboxIds.filter((id) => !remove.includes(id)), ...add.filter((id) => !m.mailboxIds.includes(id))],
      })),
    onSettled: () => requery(qc),
  });
}

export function useModifyTags() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ messageIds, add = [], remove = [] }: ChangeLabelsParams) =>
      Promise.all(messageIds.map((messageId) => invoke('modify_tags', { messageId, add, remove }))),
    onMutate: ({ messageIds, add = [], remove = [] }) =>
      patchMessages(qc, messageIds, (m) => ({
        tags: [...m.tags.filter((t) => !remove.includes(t)), ...add.filter((t) => !m.tags.includes(t))],
      })),
    onSettled: () => {
      qc.invalidateQueries({ queryKey: ['body'] });
      requery(qc);
    },
  });
}

/** Creates (or finds) the label `path` on the account; resolves to its mailbox id. */
export function useCreateLabel() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async ({ accountId, path }: { accountId: string; path: string }) =>
      (await invoke<{ id: string }>('create_label', { accountId, path })).id,
    onSettled: () => qc.invalidateQueries({ queryKey: ['mailboxes'] }),
  });
}
