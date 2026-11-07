import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface VisibilityState {
  /** `calendarKey`s switched off; a calendar never seen before is shown. */
  hidden: string[];
  /** The editor's default calendar. */
  lastCalendar: string | null;
  toggle: (key: string) => void;
  setLastCalendar: (key: string) => void;
}

export const useCalendarVisibility = create<VisibilityState>()(
  persist(
    (set) => ({
      hidden: [],
      lastCalendar: null,
      toggle: (key) =>
        set((s) => ({ hidden: s.hidden.includes(key) ? s.hidden.filter((k) => k !== key) : [...s.hidden, key] })),
      setLastCalendar: (lastCalendar) => set({ lastCalendar }),
    }),
    { name: 'mail-cat.calendars' },
  ),
);
