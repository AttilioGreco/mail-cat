import { X, Keyboard } from 'lucide-react';
import { useHotkey } from '@tanstack/react-hotkeys';
import { useShortcutsStore } from '../model/shortcuts-store';
import { PRESET_LABELS } from '../model/presets';
import { ShortcutList } from './ShortcutList';

/**
 * The "what can I press" panel, opened by the `showShortcuts` action.
 *
 * Contents come from the live hotkey registry, so it shows what is bound *in the
 * current context* — a message action missing here is genuinely not bound.
 */
export function ShortcutsOverlay() {
  const { helpOpen, closeHelp, preset } = useShortcutsStore();

  // Escape is registered rather than hand-listened so it goes through the same
  // manager as everything else. Only live while the panel is open.
  useHotkey('Escape', closeHelp, { enabled: helpOpen });

  if (!helpOpen) return null;

  return (
    <>
      <div className="fixed inset-0 z-40 bg-black/35" onClick={closeHelp} aria-hidden />

      <section
        role="dialog"
        aria-modal
        aria-label="Scorciatoie da tastiera"
        className="fixed left-1/2 top-1/2 z-50 max-h-[85vh] w-[620px] max-w-[95vw] -translate-x-1/2 -translate-y-1/2 overflow-hidden rounded-xl border border-border bg-background shadow-2xl"
      >
        <header className="flex items-center justify-between border-b border-border px-4 py-3">
          <div className="flex items-center gap-2">
            <Keyboard size={15} className="text-muted" />
            <div>
              <h2 className="text-sm font-semibold text-foreground">Scorciatoie</h2>
              <p className="text-xs text-muted">Set attivo: {PRESET_LABELS[preset]}</p>
            </div>
          </div>
          <button
            type="button"
            onClick={closeHelp}
            className="rounded-md p-1 text-muted hover:bg-surface-hover hover:text-foreground"
            title="Chiudi"
          >
            <X size={16} />
          </button>
        </header>

        <div className="max-h-[calc(85vh-60px)] overflow-y-auto p-4">
          <ShortcutList />
        </div>
      </section>
    </>
  );
}
