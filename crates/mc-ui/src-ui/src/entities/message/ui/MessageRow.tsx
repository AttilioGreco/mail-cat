import { Paperclip } from 'lucide-react';
import { cn, formatDate } from '@/shared/lib/utils';
import { LabelChip } from '@/shared/ui/label-chip';
import type { MessageEnvelope } from '../model/types';

interface MessageRowProps {
  message: MessageEnvelope;
  isSelected: boolean;
  onSelect: (e: React.MouseEvent) => void;
  onOpen?: () => void;
  /** Label display names; shown only on Labels accounts. */
  labels?: string[];
  style?: React.CSSProperties;
}

export function MessageRow({ message, isSelected, onSelect, onOpen, labels, style }: MessageRowProps) {
  return (
    <div
      style={style}
      // mousedown, not click: WebKitGTK swallows the click of a Ctrl-click,
      // which is how the list adds a row to a multi-selection.
      onMouseDown={(e) => {
        if (e.button === 0) onSelect(e);
      }}
      onDoubleClick={onOpen}
      className={cn(
        'flex cursor-pointer select-none flex-col gap-0.5 overflow-hidden border-b border-border-subtle px-4 py-3 transition-colors',
        isSelected
          ? 'bg-accent/10 border-l-2 border-l-accent'
          : 'hover:bg-surface-hover border-l-2 border-l-transparent',
      )}
    >
      {/* Row 1: sender + date */}
      <div className="flex items-baseline gap-2">
        <span
          className={cn(
            'flex-1 truncate text-sm',
            message.isRead ? 'text-muted-foreground' : 'font-semibold text-foreground',
          )}
        >
          {message.fromName || message.fromEmail}
        </span>
        <span className="shrink-0 text-[11px] text-muted">{formatDate(message.date)}</span>
      </div>

      {/* Row 2: subject + attachment icon */}
      <div className="flex items-center gap-1.5">
        {!message.isRead && (
          <span className="h-1.5 w-1.5 shrink-0 rounded-full bg-accent" />
        )}
        <span
          className={cn(
            'flex-1 truncate text-sm',
            message.isRead ? 'text-muted-foreground' : 'text-foreground',
          )}
        >
          {message.subject || '(no subject)'}
        </span>
        {message.hasAttachment && (
          <Paperclip size={11} className="shrink-0 text-muted" />
        )}
      </div>

      {(labels?.length || message.tags.length) ? (
        <div className="flex gap-1 overflow-hidden pt-0.5">
          {labels?.map((l) => <LabelChip key={l} name={l} />)}
          {message.tags.map((t) => <LabelChip key={`tag:${t}`} name={t} tag />)}
        </div>
      ) : null}
    </div>
  );
}
