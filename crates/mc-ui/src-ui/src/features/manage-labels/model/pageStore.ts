import { create } from 'zustand';

interface LabelsPageState {
  labelsOpen: boolean;
  openLabels: () => void;
  closeLabels: () => void;
}

/** The "Etichette" page is a full screen like settings, not an overlay. */
export const useLabelsPage = create<LabelsPageState>((set) => ({
  labelsOpen: false,
  openLabels: () => set({ labelsOpen: true }),
  closeLabels: () => set({ labelsOpen: false }),
}));
