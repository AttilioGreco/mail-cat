import { useMutation, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { MessageEnvelope } from '@/entities/message';

/** Every cached view that shows labels or tags. Label changes arrive on the
 *  bus but WatchMailbox doesn't carry them, so the views are re-queried. */
function requery(qc: QueryClient) {
  for (const key of ['messages', 'conversations', 'thread', 'search', 'mailboxes'])
    qc.invalidateQueries({ queryKey: [key] });
}

function patchMessage(qc: QueryClient, id: string, patch: (m: MessageEnvelope) => Partial<MessageEnvelope>) {
  qc.setQueriesData<MessageEnvelope[]>({ queryKey: ['messages'] }, (list) =>
    list?.map((m) => (m.uid === id ? { ...m, ...patch(m) } : m)),
  );
}

interface ChangeLabelsParams {
  messageId: string;
  add?: string[];
  remove?: string[];
}

/** Rejects when the server refuses (e.g. removing a message's last label). */
export function useChangeLabels() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async ({ messageId, add = [], remove = [] }: ChangeLabelsParams) => {
      const [, failed] = await invoke<[number, number]>('change_labels', {
        messageIds: [messageId],
        addMailboxIds: add,
        removeMailboxIds: remove,
      });
      if (failed > 0) throw new Error('Modifica etichette non riuscita');
    },
    onMutate: ({ messageId, add = [], remove = [] }) =>
      patchMessage(qc, messageId, (m) => ({
        mailboxIds: [...m.mailboxIds.filter((id) => !remove.includes(id)), ...add.filter((id) => !m.mailboxIds.includes(id))],
      })),
    onSettled: () => requery(qc),
  });
}

export function useModifyTags() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ messageId, add = [], remove = [] }: ChangeLabelsParams) =>
      invoke('modify_tags', { messageId, add, remove }),
    onMutate: ({ messageId, add = [], remove = [] }) =>
      patchMessage(qc, messageId, (m) => ({
        tags: [...m.tags.filter((t) => !remove.includes(t)), ...add.filter((t) => !m.tags.includes(t))],
      })),
    onSettled: () => {
      qc.invalidateQueries({ queryKey: ['body'] });
      requery(qc);
    },
  });
}
