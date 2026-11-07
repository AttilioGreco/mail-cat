import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface AgendaState {
  open: boolean;
  toggle: () => void;
  close: () => void;
}

/** Closed by default: a mail-only user must never see a calendar they did not ask for. */
export const useAgendaPane = create<AgendaState>()(
  persist(
    (set) => ({
      open: false,
      toggle: () => set((s) => ({ open: !s.open })),
      close: () => set({ open: false }),
    }),
    { name: 'mail-cat.agenda' },
  ),
);
