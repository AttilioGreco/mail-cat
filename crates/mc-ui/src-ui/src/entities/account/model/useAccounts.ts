import { useQuery } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { Account, AccountStats, AccountStatus } from './types';

interface AccountDto {
  id: string;
  name: string;
  email: string;
  status: AccountStatus;
}

async function fetchAccounts(): Promise<Account[]> {
  const dtos = await invoke<AccountDto[]>('list_accounts');
  return dtos.map((d) => ({
    id: d.id,
    name: d.name,
    email: d.email,
    status: d.status,
  }));
}

export function useAccounts() {
  return useQuery({
    queryKey: ['accounts'],
    queryFn: fetchAccounts,
    staleTime: 60_000,
  });
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
