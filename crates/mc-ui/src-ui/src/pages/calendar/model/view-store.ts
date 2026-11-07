import { create } from 'zustand';
import { stepDate, type View } from '../lib/time';

interface ViewState {
  view: View;
  date: Date;
  setView: (view: View) => void;
  setDate: (date: Date) => void;
  today: () => void;
  step: (dir: 1 | -1) => void;
}

/** Global so that leaving the section and coming back finds the same date and view. */
export const useCalendarView = create<ViewState>((set) => ({
  view: 'week',
  date: new Date(),
  setView: (view) => set({ view }),
  setDate: (date) => set({ date }),
  today: () => set({ date: new Date() }),
  step: (dir) => set((s) => ({ date: stepDate(s.view, s.date, dir) })),
}));
