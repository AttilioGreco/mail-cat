import {
  useHotkeys,
  useHotkeySequences,
  type UseHotkeyDefinition,
  type UseHotkeySequenceDefinition,
} from '@tanstack/react-hotkeys';
import { useKeymap } from './shortcuts-store';
import { ACTION_META, type ShortcutAction } from './presets';

export type ShortcutHandlers = Partial<Record<ShortcutAction, (() => void) | undefined>>;

/**
 * Bind the active preset's keys to the handlers you pass.
 *
 * Call it wherever the data for those actions already lives (the message list
 * owns navigation, the thread view owns thread-wide actions) — a single global
 * handler would have to reach for state it doesn't have. An action left out, or
 * passed as `undefined`, stays unbound: that's how conversation mode hands
 * message actions over to the thread view without both registering the same key.
 *
 * Both underlying hooks take a dynamic array, so a changing preset or handler
 * set stays a single hook call and never trips the rules of hooks.
 */
export function useShortcuts(handlers: ShortcutHandlers) {
  const keymap = useKeymap();

  const chords: UseHotkeyDefinition[] = [];
  const sequences: UseHotkeySequenceDefinition[] = [];

  for (const [action, handler] of Object.entries(handlers) as [
    ShortcutAction,
    (() => void) | undefined,
  ][]) {
    const binding = keymap[action];
    if (!binding || !handler) continue;

    // Carried into the registry so the help overlay can label what it finds
    // instead of keeping a second copy of these strings.
    const meta = {
      name: ACTION_META[action].label,
      description: ACTION_META[action].category,
    };

    if (typeof binding === 'string') {
      chords.push({ hotkey: binding, callback: handler, options: { meta } });
    } else if (Array.isArray(binding)) {
      // Interchangeable chords: one registration each, same handler.
      for (const hotkey of binding) {
        chords.push({ hotkey, callback: handler, options: { meta } });
      }
    } else {
      sequences.push({ sequence: binding.sequence, callback: handler, options: { meta } });
    }
  }

  useHotkeys(chords);
  useHotkeySequences(sequences);
}
