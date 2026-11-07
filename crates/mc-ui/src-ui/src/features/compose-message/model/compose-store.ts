import { create } from 'zustand';
import type { ComposeMode } from './composeTypes';

export interface ComposeRequest {
  mode: ComposeMode;
  messageId: string | null;
  accountId: string | null;
  /** Pre-filled recipient of a new message. */
  to: string | null;
}

interface ComposeState {
  request: ComposeRequest | null;
  openCompose: (req: { mode: ComposeMode; messageId?: string | null; accountId?: string | null; to?: string | null }) => void;
  closeCompose: () => void;
}

export const useComposeStore = create<ComposeState>((set) => ({
  request: null,
  // A second open while composing is ignored: there is one compose window, and
  // replacing its session would silently drop what is being written.
  openCompose: ({ mode, messageId = null, accountId = null, to = null }) =>
    set((s) => (s.request ? s : { request: { mode, messageId, accountId, to } })),
  closeCompose: () => set({ request: null }),
}));
