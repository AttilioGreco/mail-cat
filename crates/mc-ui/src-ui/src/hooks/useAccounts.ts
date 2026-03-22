import { useQuery } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { Account } from '@/lib/types';

interface AccountDto {
  id: string;
  name: string;
  email: string;
  is_active: boolean;
}

async function fetchAccounts(): Promise<Account[]> {
  const dtos = await invoke<AccountDto[]>('list_accounts');
  return dtos.map((d) => ({
    id: d.id,
    name: d.name,
    email: d.email,
    isActive: d.is_active,
  }));
}

export function useAccounts() {
  return useQuery({
    queryKey: ['accounts'],
    queryFn: fetchAccounts,
    staleTime: 60_000,
  });
}
