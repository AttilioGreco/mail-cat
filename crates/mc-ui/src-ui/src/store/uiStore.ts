import { create } from 'zustand';
import type { Mailbox } from '@/lib/types';
import type { ComposeInitialData } from '@/lib/composeTypes';

interface UIState {
  selectedAccountId: string | null;
  /** Full mailbox object: id for API calls, path for display */
  selectedMailbox: Mailbox | null;
  selectedMessageId: string | null;
  /** Thread ID of the selected conversation (first message id in thread) */
  selectedThreadMessageId: string | null;
  /** When true, the middle panel shows conversations instead of individual messages */
  conversationMode: boolean;
  /** Active search query — when set, MessageList shows search results */
  searchQuery: string | null;
  /** When true, MessageList shows only unread messages */
  showUnreadOnly: boolean;

  selectAccount: (id: string) => void;
  selectMailbox: (mailbox: Mailbox) => void;
  selectMessage: (id: string | null) => void;
  selectThread: (messageId: string | null) => void;
  setConversationMode: (on: boolean) => void;
  setSearchQuery: (query: string | null) => void;
  toggleUnreadOnly: () => void;

  composeOpen: boolean;
  composeData: ComposeInitialData | null;
  openCompose: (data: ComposeInitialData) => void;
  closeCompose: () => void;

  settingsOpen: boolean;
  openSettings: () => void;
  closeSettings: () => void;
}

export const useUIStore = create<UIState>((set) => ({
  selectedAccountId: null,
  selectedMailbox: null,
  selectedMessageId: null,
  selectedThreadMessageId: null,
  conversationMode: true,
  searchQuery: null,
  showUnreadOnly: false,

  selectAccount: (id) =>
    set({ selectedAccountId: id, selectedMailbox: null, selectedMessageId: null, selectedThreadMessageId: null, searchQuery: null }),

  selectMailbox: (mailbox) =>
    set({ selectedMailbox: mailbox, selectedMessageId: null, selectedThreadMessageId: null, searchQuery: null }),

  selectMessage: (id) => set({ selectedMessageId: id, selectedThreadMessageId: null }),

  selectThread: (messageId) => set({ selectedThreadMessageId: messageId, selectedMessageId: null }),

  setConversationMode: (on) => set({ conversationMode: on, selectedMessageId: null, selectedThreadMessageId: null }),

  setSearchQuery: (query) => set({ searchQuery: query, selectedMessageId: null, selectedThreadMessageId: null }),

  toggleUnreadOnly: () => set((s) => ({ showUnreadOnly: !s.showUnreadOnly, selectedMessageId: null })),

  // ── Compose ──────────────────────────────────────────────────────────────
  composeOpen: false,
  composeData: null as ComposeInitialData | null,

  openCompose: (data: ComposeInitialData) => set({ composeOpen: true, composeData: data }),
  closeCompose: () => set({ composeOpen: false, composeData: null }),

  // ── Local Settings ─────────────────────────────────────────────────────
  settingsOpen: false,
  openSettings: () => set({ settingsOpen: true }),
  closeSettings: () => set({ settingsOpen: false }),
}));

/** Query key for messages: "accountId::mailboxId" */
export function mailboxKey(accountId: string | null, mailboxId: string | null): string | null {
  if (!accountId || !mailboxId) return null;
  return `${accountId}::${mailboxId}`;
}
