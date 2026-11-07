import { create } from 'zustand';

interface ContactsState {
  query: string;
  /** A `Listed.key`. */
  selected: string | null;
  setQuery: (query: string) => void;
  select: (key: string | null) => void;
}

/** Global so that leaving the section and coming back finds the same search and contact. */
export const useContactsView = create<ContactsState>((set) => ({
  query: '',
  selected: null,
  setQuery: (query) => set({ query }),
  select: (selected) => set({ selected }),
}));
