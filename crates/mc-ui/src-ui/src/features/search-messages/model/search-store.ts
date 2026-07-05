import { create } from 'zustand';
import { useSessionStore } from '@/entities/session';

interface SearchState {
  /** Active search query — when set, MessageList shows search results */
  searchQuery: string | null;
  setSearchQuery: (query: string | null) => void;
}

export const useSearchStore = create<SearchState>((set) => ({
  searchQuery: null,
  setSearchQuery: (query) => {
    set({ searchQuery: query });
    // Clearing/changing the search resets the current message/thread selection,
    // same as switching mailboxes does.
    useSessionStore.getState().selectMessage(null);
  },
}));
