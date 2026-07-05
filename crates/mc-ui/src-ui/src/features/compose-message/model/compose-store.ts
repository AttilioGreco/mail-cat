import { create } from 'zustand';
import type { ComposeInitialData } from './composeTypes';

interface ComposeState {
  composeOpen: boolean;
  composeData: ComposeInitialData | null;
  openCompose: (data: ComposeInitialData) => void;
  closeCompose: () => void;
}

export const useComposeStore = create<ComposeState>((set) => ({
  composeOpen: false,
  composeData: null,
  openCompose: (data) => set({ composeOpen: true, composeData: data }),
  closeCompose: () => set({ composeOpen: false, composeData: null }),
}));
