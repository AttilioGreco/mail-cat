import {
  formatForDisplay,
  useHotkeyRegistrations,
  type HotkeyRegistrationsResult,
} from '@tanstack/react-hotkeys';
import { CATEGORY_ORDER, type ShortcutCategory } from '../model/presets';

interface Row {
  label: string;
  category: string;
  /** One entry per interchangeable binding; a sequence is its own step list. */
  keys: string[][];
}

/**
 * Build the list from what is *actually registered right now*, not from a second
 * copy of the keymap: a hand-written table drifts the moment a binding moves or
 * becomes conditional (message actions, for instance, are unbound in
 * conversation mode because the thread view owns them there).
 */
function buildRows(registrations: HotkeyRegistrationsResult): Row[] {
  const byLabel = new Map<string, Row>();

  const add = (label: string | undefined, category: string | undefined, keys: string[]) => {
    if (!label) return; // not one of ours — skip library/internal registrations
    const row = byLabel.get(label) ?? { label, category: category ?? '', keys: [] };
    row.keys.push(keys);
    byLabel.set(label, row);
  };

  for (const reg of registrations.hotkeys) {
    add(reg.options.meta?.name, reg.options.meta?.description, [formatForDisplay(reg.hotkey)]);
  }
  for (const reg of registrations.sequences) {
    add(
      reg.options.meta?.name,
      reg.options.meta?.description,
      reg.sequence.map((step) => formatForDisplay(step)),
    );
  }

  return [...byLabel.values()];
}

export function ShortcutList() {
  const registrations = useHotkeyRegistrations();
  const rows = buildRows(registrations);

  if (rows.length === 0) {
    return (
      <p className="rounded-lg border border-dashed border-border p-6 text-center text-xs text-muted">
        Nessuna scorciatoia attiva.
      </p>
    );
  }

  const categories = CATEGORY_ORDER.filter((c) => rows.some((r) => r.category === c));
  // Anything with an unexpected category still gets shown rather than swallowed.
  const others = rows.filter((r) => !CATEGORY_ORDER.includes(r.category as ShortcutCategory));

  return (
    <div className="space-y-4">
      {categories.map((category) => (
        <section key={category} className="rounded-lg border border-border bg-surface p-3">
          <div className="mb-2 text-sm font-medium text-foreground">{category}</div>
          <dl className="divide-y divide-border">
            {rows
              .filter((r) => r.category === category)
              .map((row) => (
                <RowLine key={row.label} row={row} />
              ))}
          </dl>
        </section>
      ))}

      {others.length > 0 && (
        <section className="rounded-lg border border-border bg-surface p-3">
          <div className="mb-2 text-sm font-medium text-foreground">Altro</div>
          <dl className="divide-y divide-border">
            {others.map((row) => (
              <RowLine key={row.label} row={row} />
            ))}
          </dl>
        </section>
      )}
    </div>
  );
}

function RowLine({ row }: { row: Row }) {
  return (
    <div className="flex items-baseline justify-between gap-3 py-1.5">
      <dt className="text-xs text-muted">{row.label}</dt>
      <dd className="flex shrink-0 items-center gap-2">
        {row.keys.map((steps, i) => (
          <span key={i} className="flex items-center gap-1">
            {i > 0 && <span className="text-[10px] text-muted">o</span>}
            {steps.map((step, j) => (
              <span key={j} className="flex items-center gap-1">
                {j > 0 && <span className="text-[10px] text-muted">poi</span>}
                <kbd className="rounded border border-border bg-background px-1.5 py-0.5 font-mono text-[11px] text-foreground">
                  {step}
                </kbd>
              </span>
            ))}
          </span>
        ))}
      </dd>
    </div>
  );
}
