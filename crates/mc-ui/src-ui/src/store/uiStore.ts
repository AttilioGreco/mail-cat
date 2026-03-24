import { create } from 'zustand';
import type { Mailbox } from '@/lib/types';

interface UIState {
  selectedAccountId: string | null;
  /** Full mailbox object: id for API calls, path for display */
  selectedMailbox: Mailbox | null;
  selectedMessageId: string | null;
  /** Active search query — when set, MessageList shows search results */
  searchQuery: string | null;

  selectAccount: (id: string) => void;
  selectMailbox: (mailbox: Mailbox) => void;
  selectMessage: (id: string | null) => void;
  setSearchQuery: (query: string | null) => void;
}

export const useUIStore = create<UIState>((set) => ({
  selectedAccountId: null,
  selectedMailbox: null,
  selectedMessageId: null,
  searchQuery: null,

  selectAccount: (id) =>
    set({ selectedAccountId: id, selectedMailbox: null, selectedMessageId: null, searchQuery: null }),

  selectMailbox: (mailbox) =>
    set({ selectedMailbox: mailbox, selectedMessageId: null, searchQuery: null }),

  selectMessage: (id) => set({ selectedMessageId: id }),

  setSearchQuery: (query) => set({ searchQuery: query, selectedMessageId: null }),
}));

/** Query key for messages: "accountId::mailboxId" */
export function mailboxKey(accountId: string | null, mailboxId: string | null): string | null {
  if (!accountId || !mailboxId) return null;
  return `${accountId}::${mailboxId}`;
}
