import { create } from 'zustand';
import { persist } from 'zustand/middleware';

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
  /** Jump straight to one message (notification deep link). */
  openMessageAt: (accountId: string, mailboxId: string, messageId: string) => void;
  selectThread: (messageId: string | null) => void;
  setConversationMode: (on: boolean) => void;
  toggleUnreadOnly: () => void;
}

export const useSessionStore = create<SessionState>()(
  persist(
    (set) => ({
      selectedAccountId: null,
      selectedMailboxId: null,
      selectedMessageId: null,
      selectedThreadMessageId: null,
      conversationMode: false,
      showUnreadOnly: false,

      selectAccount: (id) =>
        set({ selectedAccountId: id, selectedMailboxId: null, selectedMessageId: null, selectedThreadMessageId: null }),

      selectMailbox: (mailboxId) =>
        set({ selectedMailboxId: mailboxId, selectedMessageId: null, selectedThreadMessageId: null }),

      selectMessage: (id) => set({ selectedMessageId: id, selectedThreadMessageId: null }),

      // Set all three at once: selectAccount/selectMailbox each clear what comes
      // after them, so calling them in sequence would wipe the message we want.
      // Also forces the message list out of conversation mode and off the unread
      // filter, either of which could hide the very message we were asked to show.
      openMessageAt: (accountId, mailboxId, messageId) =>
        set({
          selectedAccountId: accountId,
          selectedMailboxId: mailboxId,
          selectedMessageId: messageId,
          selectedThreadMessageId: null,
          conversationMode: false,
          showUnreadOnly: false,
        }),

      selectThread: (messageId) => set({ selectedThreadMessageId: messageId, selectedMessageId: null }),

      setConversationMode: (on) => set({ conversationMode: on, selectedMessageId: null, selectedThreadMessageId: null }),

      // Clear the thread selection too: the open thread may be filtered out of the
      // list, which would leave the reading pane showing something no longer listed.
      toggleUnreadOnly: () =>
        set((s) => ({
          showUnreadOnly: !s.showUnreadOnly,
          selectedMessageId: null,
          selectedThreadMessageId: null,
        })),
    }),
    {
      name: 'mail-cat.session',
      // Only the location is worth restoring — which message was open is not.
      partialize: (s) => ({
        selectedAccountId: s.selectedAccountId,
        selectedMailboxId: s.selectedMailboxId,
        conversationMode: s.conversationMode,
      }),
    },
  ),
);

/** Query key for messages: "accountId::mailboxId" */
export function mailboxKey(accountId: string | null, mailboxId: string | null): string | null {
  if (!accountId || !mailboxId) return null;
  return `${accountId}::${mailboxId}`;
}
