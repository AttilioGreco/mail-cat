import { create } from 'zustand';

interface SessionState {
  selectedAccountId: string | null;
  selectedMailboxId: string | null;
  selectedMessageId: string | null;
  /** Thread ID of the selected conversation (first message id in thread) */
  selectedThreadMessageId: string | null;
  /** When true, the middle panel shows conversations instead of individual messages */
  conversationMode: boolean;
  /** When true, MessageList shows only unread messages */
  showUnreadOnly: boolean;

  selectAccount: (id: string) => void;
  selectMailbox: (mailboxId: string) => void;
  selectMessage: (id: string | null) => void;
  selectThread: (messageId: string | null) => void;
  setConversationMode: (on: boolean) => void;
  toggleUnreadOnly: () => void;
}

export const useSessionStore = create<SessionState>((set) => ({
  selectedAccountId: null,
  selectedMailboxId: null,
  selectedMessageId: null,
  selectedThreadMessageId: null,
  conversationMode: true,
  showUnreadOnly: false,

  selectAccount: (id) =>
    set({ selectedAccountId: id, selectedMailboxId: null, selectedMessageId: null, selectedThreadMessageId: null }),

  selectMailbox: (mailboxId) =>
    set({ selectedMailboxId: mailboxId, selectedMessageId: null, selectedThreadMessageId: null }),

  selectMessage: (id) => set({ selectedMessageId: id, selectedThreadMessageId: null }),

  selectThread: (messageId) => set({ selectedThreadMessageId: messageId, selectedMessageId: null }),

  setConversationMode: (on) => set({ conversationMode: on, selectedMessageId: null, selectedThreadMessageId: null }),

  toggleUnreadOnly: () => set((s) => ({ showUnreadOnly: !s.showUnreadOnly, selectedMessageId: null })),
}));

/** Query key for messages: "accountId::mailboxId" */
export function mailboxKey(accountId: string | null, mailboxId: string | null): string | null {
  if (!accountId || !mailboxId) return null;
  return `${accountId}::${mailboxId}`;
}
