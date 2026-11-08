import { create } from 'zustand';

export interface PendingEvent {
  accountId: string;
  href: string;
  /** Occurrence start, Unix seconds. */
  start: number;
}

interface PendingEventState {
  pending: PendingEvent | null;
  set: (pending: PendingEvent | null) => void;
}

/** Hand-off from a notification click to the calendar page, which owns the date and the loaded events. */
export const usePendingEvent = create<PendingEventState>((set) => ({
  pending: null,
  set: (pending) => set({ pending }),
}));
