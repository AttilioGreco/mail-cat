import { create } from 'zustand';
import type { RemoteContact } from './types';

export type ContactEditorTarget =
  | { mode: 'create'; prefill?: { name?: string; email?: string } }
  | { mode: 'edit'; contact: RemoteContact };

interface EditorState {
  target: ContactEditorTarget | null;
  open: (target: ContactEditorTarget) => void;
  close: () => void;
}

/** In the entity so the message pane can offer "Aggiungi ai contatti" without importing a feature. */
export const useContactEditor = create<EditorState>((set) => ({
  target: null,
  open: (target) => set({ target }),
  close: () => set({ target: null }),
}));
