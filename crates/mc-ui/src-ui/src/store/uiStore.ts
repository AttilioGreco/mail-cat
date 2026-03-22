import { create } from 'zustand';
import type { Mailbox } from '@/lib/types';

interface UIState {
  selectedAccountId: string | null;
  /** Full mailbox object: id for API calls, path for display */
  selectedMailbox: Mailbox | null;
  selectedMessageId: string | null;

  selectAccount: (id: string) => void;
  selectMailbox: (mailbox: Mailbox) => void;
  selectMessage: (id: string | null) => void;
}

export const useUIStore = create<UIState>((set) => ({
  selectedAccountId: null,
  selectedMailbox: null,
  selectedMessageId: null,

  selectAccount: (id) =>
    set({ selectedAccountId: id, selectedMailbox: null, selectedMessageId: null }),

  selectMailbox: (mailbox) =>
    set({ selectedMailbox: mailbox, selectedMessageId: null }),

  selectMessage: (id) => set({ selectedMessageId: id }),
}));

/** Query key for messages: "accountId::mailboxId" */
export function mailboxKey(accountId: string | null, mailboxId: string | null): string | null {
  if (!accountId || !mailboxId) return null;
  return `${accountId}::${mailboxId}`;
}
