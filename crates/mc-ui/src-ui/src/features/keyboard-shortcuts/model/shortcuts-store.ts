import { create } from 'zustand';
import { PRESETS, type Keymap, type PresetId } from './presets';

const PRESET_KEY = 'mc.shortcutPreset';

function getStoredPreset(): PresetId {
  const v = localStorage.getItem(PRESET_KEY);
  return v === 'vim' || v === 'outlook' || v === 'none' ? v : 'vim';
}

interface ShortcutsState {
  preset: PresetId;
  setPreset: (preset: PresetId) => void;
  /** The F1 help panel. */
  helpOpen: boolean;
  toggleHelp: () => void;
  closeHelp: () => void;
}

/** Client-local, like the theme — no server round-trip. */
export const useShortcutsStore = create<ShortcutsState>((set) => ({
  preset: getStoredPreset(),
  setPreset: (preset) => {
    localStorage.setItem(PRESET_KEY, preset);
    set({ preset });
  },
  helpOpen: false,
  toggleHelp: () => set((s) => ({ helpOpen: !s.helpOpen })),
  closeHelp: () => set({ helpOpen: false }),
}));

/** The active keymap. */
export function useKeymap(): Keymap {
  return PRESETS[useShortcutsStore((s) => s.preset)];
}
