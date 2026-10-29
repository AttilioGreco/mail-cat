import { X } from 'lucide-react';
import { cn } from '@/shared/lib/utils';

interface LabelChipProps {
  name: string;
  /** A tag (server keyword) rather than a mailbox label. */
  tag?: boolean;
  onRemove?: () => void;
}

export function LabelChip({ name, tag = false, onRemove }: LabelChipProps) {
  return (
    <span
      className={cn(
        'inline-flex max-w-[10rem] shrink-0 items-center gap-1 rounded-full px-1.5 py-px text-[10px] leading-4',
        tag ? 'border border-border text-muted-foreground' : 'bg-accent/15 text-accent',
      )}
    >
      <span className="truncate">{name}</span>
      {onRemove && (
        <button
          type="button"
          aria-label={`Rimuovi ${name}`}
          onClick={onRemove}
          className="rounded-full hover:text-destructive focus-visible:outline focus-visible:outline-1"
        >
          <X size={10} />
        </button>
      )}
    </span>
  );
}
