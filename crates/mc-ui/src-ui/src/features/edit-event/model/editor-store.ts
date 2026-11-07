import { create } from 'zustand';
import type { AccountEvent } from '@/entities/calendar';

export type EditorTarget =
  | { mode: 'create'; start: Date; end: Date; allDay: boolean }
  | { mode: 'edit'; event: AccountEvent };

interface EditorState {
  target: EditorTarget | null;
  open: (target: EditorTarget) => void;
  close: () => void;
}

export const useEventEditor = create<EditorState>((set) => ({
  target: null,
  open: (target) => set({ target }),
  close: () => set({ target: null }),
}));
