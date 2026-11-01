import { useState } from 'react';
import {
  Combobox,
  ComboboxInput,
  ComboboxItem,
  ComboboxList,
} from '@/shared/ui/combobox';

export interface LabelOption {
  id: string;
  /** Full path, "Dev/GitHub". */
  path: string;
  /** Which of the targeted messages carry it. */
  state: 'all' | 'some' | 'none';
}

interface LabelComboboxProps {
  options: LabelOption[];
  onPick: (option: LabelOption) => void;
  /** A name typed that matches no option; offered as "Crea etichetta". */
  onCreate: (path: string) => void;
}

/** Marks the "create" entry among the option ids; a NUL cannot be in either. */
const NEW = '\0new:';

/** Type-ahead over label paths ("dev/gi" finds "Dev/GitHub"). The list is
 *  always open and inline: the caller puts it in a dialog. Picking an entry reports it
 *  and leaves the toggling to the caller. */
export function LabelCombobox({ options, onPick, onCreate }: LabelComboboxProps) {
  const [query, setQuery] = useState('');
  const typed = query.trim();
  const needle = typed.toLowerCase();

  const shown = needle ? options.filter((o) => o.path.toLowerCase().includes(needle)) : options;
  const canCreate = needle !== '' && !options.some((o) => o.path.toLowerCase() === needle);
  const items = [...shown.map((o) => o.id), ...(canCreate ? [NEW + typed] : [])];
  const byId = new Map(options.map((o) => [o.id, o]));
  const checked = options.filter((o) => o.state === 'all').map((o) => o.id);

  return (
    // Inline: a floating popup would sit over the dialog and swallow its clicks.
    <Combobox
      multiple
      inline
      open
      onOpenChange={() => {}}
      items={items}
      filter={null}
      autoHighlight
      value={checked}
      inputValue={query}
      onInputValueChange={(next, details) => {
        if (details.reason !== 'input-clear') setQuery(next);
      }}
      onValueChange={(next: string[]) => {
        const created = next.find((v) => v.startsWith(NEW));
        if (created) {
          setQuery('');
          onCreate(created.slice(NEW.length));
          return;
        }
        // Multiple mode reports the whole value: the entry that differs is the one picked.
        const picked = [...next.filter((v) => !checked.includes(v)), ...checked.filter((v) => !next.includes(v))][0];
        const option = picked && byId.get(picked);
        if (option) onPick(option);
      }}
    >
      <ComboboxInput autoFocus showTrigger={false} placeholder="Cerca o crea un’etichetta…" aria-label="Etichetta" />
      <ComboboxList className="mt-1 max-h-64 rounded-md border">
          {(id: string) => {
            if (id.startsWith(NEW)) {
              return (
                <ComboboxItem key={id} value={id}>
                  Crea etichetta «{id.slice(NEW.length)}»
                </ComboboxItem>
              );
            }
            const option = byId.get(id);
            if (!option) return null;
            return (
              <ComboboxItem key={id} value={id}>
                <span className="truncate">{option.path}</span>
                {option.state === 'some' && <span className="shrink-0 text-[10px] text-muted-foreground">alcuni</span>}
              </ComboboxItem>
            );
          }}
      </ComboboxList>
    </Combobox>
  );
}
