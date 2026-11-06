import { useState } from 'react';
import { Ban } from 'lucide-react';
import { cn } from '@/shared/lib/utils';
import { LABEL_COLORS, labelColorStyle } from '@/shared/lib/label-colors';
import { Popover, PopoverContent, PopoverTrigger } from '@/shared/ui/popover';

interface ColorPickerProps {
  /** Palette key, if the label has a color. */
  value: string | null | undefined;
  label: string;
  onChange: (color: string | null) => void;
}

/** The swatch of a label; clicking it opens the 24-color palette. */
export function ColorPicker({ value, label, onChange }: ColorPickerProps) {
  const [open, setOpen] = useState(false);
  const pick = (color: string | null) => {
    onChange(color);
    setOpen(false);
  };
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        aria-label={`Colore di ${label}`}
        className="flex h-6 w-6 shrink-0 items-center justify-center rounded-md hover:bg-surface-hover"
      >
        <span
          style={labelColorStyle(value)}
          className={cn('h-3.5 w-3.5 rounded-full', !value && 'border border-dashed border-muted')}
        />
      </PopoverTrigger>
      <PopoverContent className="w-auto p-2">
        <div className="grid grid-cols-12 gap-1">
          {LABEL_COLORS.map((c) => (
            <button
              key={c.key}
              type="button"
              title={c.name}
              aria-label={c.name}
              aria-pressed={c.key === value}
              onClick={() => pick(c.key)}
              style={labelColorStyle(c.key)}
              className={cn(
                'h-5 w-5 rounded-full outline-none focus-visible:ring-2 focus-visible:ring-accent',
                c.key === value && 'ring-2 ring-foreground ring-offset-1 ring-offset-surface',
              )}
            />
          ))}
        </div>
        <button
          type="button"
          onClick={() => pick(null)}
          className="mt-2 flex w-full items-center gap-2 rounded-md px-1.5 py-1 text-xs text-muted-foreground hover:bg-surface-hover hover:text-foreground"
        >
          <Ban size={12} />
          Nessun colore
        </button>
      </PopoverContent>
    </Popover>
  );
}
