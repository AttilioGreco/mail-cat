/**
 * Shortcut presets.
 *
 * A binding is one chord (`'Mod+N'`), a list of interchangeable chords
 * (`['Delete', 'Shift+3']`), or a Vim-style sequence (`seq('G', 'I')`).
 * Anything omitted from a preset simply has no key.
 *
 * Alternatives exist because the idiomatic key is often unreachable on
 * non-US layouts: Gmail's `#` is Shift+3 only on US and `/` is Shift+7 on an
 * Italian keyboard. Each action gets the idiomatic key *and* a
 * layout-independent one rather than a compromise between them.
 *
 * `ignoreInputs` is left to TanStack's default, which already does the right
 * thing: single keys and Shift/Alt combos are suppressed inside text inputs and
 * contenteditable (so typing "c" in the composer does not open a new message),
 * while Ctrl/Meta chords and Escape still fire.
 */

import type { Hotkey, HotkeySequence } from '@tanstack/react-hotkeys';

/** Marks a binding as "press these in order" rather than "any one of these". */
export interface SequenceBinding {
  sequence: HotkeySequence;
}

/** Typed by the library, so a misspelled key is a compile error rather than a
 *  shortcut that silently never fires. */
export type ShortcutBinding = Hotkey | Hotkey[] | SequenceBinding;

/** `seq('G', 'I')` — reads better than `{ sequence: [...] }` in the tables below. */
function seq(...keys: HotkeySequence): SequenceBinding {
  return { sequence: keys };
}

/** Every action a preset can bind. Add here, then handle it where the data lives. */
export type ShortcutAction =
  // navigation
  | 'nextMessage'
  | 'prevMessage'
  | 'nextUnread'
  | 'prevUnread'
  | 'focusSearch'
  | 'goInbox'
  | 'goSent'
  | 'goDrafts'
  | 'goArchive'
  | 'goSpam'
  | 'goTrash'
  // writing
  | 'compose'
  | 'reply'
  | 'replyAll'
  | 'forward'
  // acting on the current message (or the open conversation)
  | 'toggleRead'
  | 'toggleFlag'
  | 'archiveMessage'
  | 'markSpam'
  | 'deleteMessage'
  | 'deletePermanently'
  // view & app
  | 'toggleUnreadFilter'
  | 'toggleConversations'
  | 'markFolderRead'
  | 'syncNow'
  | 'openSettings'
  | 'showShortcuts';

export type PresetId = 'vim' | 'outlook' | 'none';

export type Keymap = Partial<Record<ShortcutAction, ShortcutBinding>>;

/** Category shown in the help overlay, carried through as hotkey metadata. */
export type ShortcutCategory = 'Navigazione' | 'Scrittura' | 'Messaggio' | 'Vista';

export const ACTION_META: Record<
  ShortcutAction,
  { label: string; category: ShortcutCategory }
> = {
  nextMessage: { label: 'Messaggio successivo', category: 'Navigazione' },
  prevMessage: { label: 'Messaggio precedente', category: 'Navigazione' },
  nextUnread: { label: 'Non letto successivo', category: 'Navigazione' },
  prevUnread: { label: 'Non letto precedente', category: 'Navigazione' },
  focusSearch: { label: 'Cerca', category: 'Navigazione' },
  goInbox: { label: 'Posta in arrivo', category: 'Navigazione' },
  goSent: { label: 'Inviati', category: 'Navigazione' },
  goDrafts: { label: 'Bozze', category: 'Navigazione' },
  goArchive: { label: 'Archivio', category: 'Navigazione' },
  goSpam: { label: 'Spam', category: 'Navigazione' },
  goTrash: { label: 'Cestino', category: 'Navigazione' },

  compose: { label: 'Nuovo messaggio', category: 'Scrittura' },
  reply: { label: 'Rispondi', category: 'Scrittura' },
  replyAll: { label: 'Rispondi a tutti', category: 'Scrittura' },
  forward: { label: 'Inoltra', category: 'Scrittura' },

  toggleRead: { label: 'Letto / non letto', category: 'Messaggio' },
  toggleFlag: { label: 'Stella', category: 'Messaggio' },
  archiveMessage: { label: 'Archivia', category: 'Messaggio' },
  markSpam: { label: 'Segna come spam', category: 'Messaggio' },
  deleteMessage: { label: 'Sposta nel cestino', category: 'Messaggio' },
  deletePermanently: { label: 'Elimina definitivamente', category: 'Messaggio' },

  toggleUnreadFilter: { label: 'Solo non letti', category: 'Vista' },
  toggleConversations: { label: 'Modalità conversazioni', category: 'Vista' },
  markFolderRead: { label: 'Segna cartella come letta', category: 'Vista' },
  syncNow: { label: 'Sincronizza ora', category: 'Vista' },
  openSettings: { label: 'Impostazioni', category: 'Vista' },
  showShortcuts: { label: 'Elenco scorciatoie', category: 'Vista' },
};

/**
 * Vim / Gmail. One preset for both cultures on purpose: Gmail's keymap is
 * vim-derived, so a separate "Gmail" set would be the same keys twice.
 */
const VIM: Keymap = {
  nextMessage: 'J',
  prevMessage: 'K',
  nextUnread: 'N',
  prevUnread: 'P',
  focusSearch: ['/', 'Mod+F'],
  goInbox: seq('G', 'I'),
  goSent: seq('G', 'T'),
  goDrafts: seq('G', 'B'),
  goArchive: seq('G', 'A'),
  goSpam: seq('G', 'S'),
  goTrash: seq('G', 'D'),

  compose: 'C',
  reply: 'R',
  replyAll: 'A',
  forward: 'F',

  toggleRead: 'U',
  toggleFlag: 'S',
  archiveMessage: 'E',
  markSpam: 'Shift+1', // '!' on both US and IT layouts
  deleteMessage: ['Delete', 'Shift+3'], // Gmail's '#' is US-only; Delete always works
  deletePermanently: 'Shift+Delete',

  toggleUnreadFilter: 'Shift+U',
  toggleConversations: 'Shift+C',
  markFolderRead: 'Shift+A',
  syncNow: 'Shift+R',
  openSettings: seq('G', 'P'),
  showShortcuts: 'F1', // '?' is not a registerable key, and it needs Shift+' on IT anyway
};

const OUTLOOK: Keymap = {
  nextMessage: 'ArrowDown',
  prevMessage: 'ArrowUp',
  nextUnread: 'Mod+.',
  prevUnread: 'Mod+,',
  focusSearch: ['Mod+E', 'F3'],
  goInbox: 'Mod+Shift+I',
  goSent: 'Mod+Shift+S',
  goDrafts: 'Mod+Shift+B',
  goArchive: 'Mod+Shift+A',
  goSpam: 'Mod+Shift+J',
  goTrash: 'Mod+Shift+D',

  compose: 'Mod+N',
  reply: 'Mod+R',
  replyAll: 'Mod+Shift+R',
  forward: 'Mod+F', // Outlook really does forward with Ctrl+F
  toggleRead: 'Mod+Q',
  toggleFlag: 'Mod+Shift+G',
  archiveMessage: 'Backspace',
  markSpam: 'Mod+Alt+J',
  deleteMessage: 'Delete',
  deletePermanently: 'Shift+Delete',

  toggleUnreadFilter: 'Mod+Shift+U',
  toggleConversations: 'Mod+Shift+C',
  markFolderRead: 'Mod+Shift+M',
  syncNow: 'F9',
  openSettings: 'Mod+,',
  showShortcuts: 'F1',
};

export const PRESETS: Record<PresetId, Keymap> = {
  vim: VIM,
  outlook: OUTLOOK,
  none: {},
};

export const PRESET_LABELS: Record<PresetId, string> = {
  vim: 'Vim / Gmail',
  outlook: 'Outlook',
  none: 'Disattivate',
};

export const CATEGORY_ORDER: ShortcutCategory[] = [
  'Navigazione',
  'Scrittura',
  'Messaggio',
  'Vista',
];
