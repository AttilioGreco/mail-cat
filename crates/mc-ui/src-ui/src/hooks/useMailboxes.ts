import { useQuery } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { Mailbox } from '@/lib/types';

async function fetchMailboxes(accountId: string): Promise<Mailbox[]> {
  const dtos = await invoke<{
    id: string;
    account_id: string;
    path: string;
    display_name: string;
    total_messages: number;
    unread_messages: number;
  }[]>('list_mailboxes', { accountId });

  return dtos.map((d) => ({
    id: d.id,
    accountId: d.account_id,
    path: d.path,
    name: d.display_name,
    totalMessages: d.total_messages,
    unreadMessages: d.unread_messages,
    attributes: [],
  }));
}

export function useMailboxes(accountId: string | null) {
  return useQuery({
    queryKey: ['mailboxes', accountId],
    queryFn: () => fetchMailboxes(accountId!),
    enabled: accountId !== null,
    staleTime: 30_000,
  });
}
