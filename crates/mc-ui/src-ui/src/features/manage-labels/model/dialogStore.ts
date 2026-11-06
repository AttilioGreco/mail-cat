import { create } from 'zustand';

/** What the picker needs to know about a message to show and change its labels. */
export interface LabelTarget {
  id: string;
  mailboxIds: string[];
  tags: string[];
}

interface DialogState {
  picker: { accountId: string; messages: LabelTarget[] } | null;
  sender: { accountId: string; email: string } | null;
  manage: { accountId: string; label: string; mode: 'rename' | 'delete' } | null;
  openManage: (accountId: string, label: string, mode: 'rename' | 'delete') => void;
  openPicker: (accountId: string, messages: LabelTarget[]) => void;
  openSender: (accountId: string, email: string) => void;
  close: () => void;
}

/** Requests to open the label dialogs, made from places that know the messages
 *  (list, thread, hotkeys); `LabelDialogs` renders them once for the page. */
export const useLabelDialogs = create<DialogState>((set) => ({
  picker: null,
  sender: null,
  manage: null,
  openManage: (accountId, label, mode) => set({ manage: { accountId, label, mode }, picker: null, sender: null }),
  openPicker: (accountId, messages) => messages.length > 0 && set({ picker: { accountId, messages }, sender: null, manage: null }),
  openSender: (accountId, email) => set({ sender: { accountId, email }, picker: null, manage: null }),
  close: () => set({ picker: null, sender: null, manage: null }),
}));
