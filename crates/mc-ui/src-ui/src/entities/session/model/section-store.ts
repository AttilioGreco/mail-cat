import { create } from 'zustand';

export type Section = 'mail' | 'calendar' | 'contacts';

interface SectionState {
  section: Section;
  setSection: (section: Section) => void;
}

/** Not persisted: the app always starts on mail, so a mail-only user never meets another section. */
export const useSectionStore = create<SectionState>((set) => ({
  section: 'mail',
  setSection: (section) => set({ section }),
}));
