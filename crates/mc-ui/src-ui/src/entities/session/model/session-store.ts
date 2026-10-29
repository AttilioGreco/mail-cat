import { create } from 'zustand';
import { persist } from 'zustand/middleware';

/** Ctrl-click on `id`: add it to the multi-selection `ids` or take it out.
 *  The first Ctrl-click keeps the row already open in the selection, as every
 *  mail client does; taking the open row out opens whatever is still selected.
 *  A selection shrunk to one row collapses back to just `open`. */
function toggleIn(ids: string[], open: string | null, id: string): { ids: string[]; open: string | null } {
  const current = ids.length > 0 ? ids : open ? [open] : [];
  if (!current.includes(id)) return { ids: [...current, id], open: id };
  const rest = current.filter((x) => x !== id);
  return {
    ids: rest.length > 1 ? rest : [],
    open: open === id ? (rest[rest.length - 1] ?? null) : open,
  };
}

interface SessionState {
  selectedAccountId: string | null;
  selectedMailboxId: string | null;
  selectedMessageId: string | null;
  /** Ctrl/Shift-click multi-selection in the message list, empty when only
   *  `selectedMessageId` is selected. Includes `selectedMessageId` when set. */
  selectedMessageIds: string[];
  /** Thread ID of the selected conversation (first message id in thread) */
  selectedThreadMessageId: string | null;
  /** Same as `selectedMessageIds`, for conversations (their sample message ids). */
  selectedThreadIds: string[];
  /** When true, the middle panel shows conversations instead of individual messages */
  conversationMode: boolean;
  /** When true, MessageList shows only unread messages */
  showUnreadOnly: boolean;

  selectAccount: (id: string) => void;
  selectMailbox: (mailboxId: string) => void;
  selectMessage: (id: string | null) => void;
  /** Ctrl-click: add `id` to the multi-selection, or take it out. */
  toggleMessageInSelection: (id: string) => void;
  /** Shift-click: select every id in `range` (the rows between anchor and click). */
  selectMessageRange: (range: string[], focusId: string) => void;
  /** Jump straight to one message (notification deep link). */
  openMessageAt: (accountId: string, mailboxId: string, messageId: string) => void;
  selectThread: (messageId: string | null) => void;
  toggleThreadInSelection: (messageId: string) => void;
  selectThreadRange: (range: string[], focusId: string) => void;
  setConversationMode: (on: boolean) => void;
  toggleUnreadOnly: () => void;
}

export const useSessionStore = create<SessionState>()(
  persist(
    (set) => ({
      selectedAccountId: null,
      selectedMailboxId: null,
      selectedMessageId: null,
      selectedMessageIds: [],
      selectedThreadMessageId: null,
      selectedThreadIds: [],
      conversationMode: false,
      showUnreadOnly: false,

      selectAccount: (id) =>
        set({ selectedAccountId: id, selectedMailboxId: null, selectedMessageId: null, selectedMessageIds: [], selectedThreadMessageId: null, selectedThreadIds: [] }),

      selectMailbox: (mailboxId) =>
        set({ selectedMailboxId: mailboxId, selectedMessageId: null, selectedMessageIds: [], selectedThreadMessageId: null, selectedThreadIds: [] }),

      selectMessage: (id) => set({ selectedMessageId: id, selectedMessageIds: [], selectedThreadMessageId: null, selectedThreadIds: [] }),

      toggleMessageInSelection: (id) =>
        set((s) => {
          const { ids, open } = toggleIn(s.selectedMessageIds, s.selectedMessageId, id);
          return { selectedMessageIds: ids, selectedMessageId: open, selectedThreadMessageId: null };
        }),

      selectMessageRange: (range, focusId) =>
        set({ selectedMessageIds: range.length > 1 ? range : [], selectedMessageId: focusId, selectedThreadMessageId: null }),

      // Set all three at once: selectAccount/selectMailbox each clear what comes
      // after them, so calling them in sequence would wipe the message we want.
      // Also forces the message list out of conversation mode and off the unread
      // filter, either of which could hide the very message we were asked to show.
      openMessageAt: (accountId, mailboxId, messageId) =>
        set({
          selectedAccountId: accountId,
          selectedMailboxId: mailboxId,
          selectedMessageId: messageId,
          selectedMessageIds: [],
          selectedThreadMessageId: null,
          selectedThreadIds: [],
          conversationMode: false,
          showUnreadOnly: false,
        }),

      selectThread: (messageId) =>
        set({ selectedThreadMessageId: messageId, selectedThreadIds: [], selectedMessageId: null, selectedMessageIds: [] }),

      toggleThreadInSelection: (messageId) =>
        set((s) => {
          const { ids, open } = toggleIn(s.selectedThreadIds, s.selectedThreadMessageId, messageId);
          return { selectedThreadIds: ids, selectedThreadMessageId: open, selectedMessageId: null };
        }),

      selectThreadRange: (range, focusId) =>
        set({ selectedThreadIds: range.length > 1 ? range : [], selectedThreadMessageId: focusId, selectedMessageId: null }),

      setConversationMode: (on) => set({ conversationMode: on, selectedMessageId: null, selectedMessageIds: [], selectedThreadMessageId: null, selectedThreadIds: [] }),

      // Clear the thread selection too: the open thread may be filtered out of the
      // list, which would leave the reading pane showing something no longer listed.
      toggleUnreadOnly: () =>
        set((s) => ({
          showUnreadOnly: !s.showUnreadOnly,
          selectedMessageId: null,
          selectedMessageIds: [],
          selectedThreadMessageId: null,
          selectedThreadIds: [],
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
