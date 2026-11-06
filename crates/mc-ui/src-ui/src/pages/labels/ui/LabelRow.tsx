import { Pencil, Sparkles, Trash2 } from 'lucide-react';
import { cn } from '@/shared/lib/utils';
import type { LabelMode } from '@/features/auto-label';
import type { LabelNode } from '@/entities/mailbox';
import { ColorPicker } from './ColorPicker';

const MODE_NAMES: Record<LabelMode, string> = {
  off: 'Disattivata',
  suggest: 'Suggerisce',
  apply: 'Applica',
  apply_archive: 'Applica e archivia',
};

interface LabelRowProps {
  node: LabelNode;
  depth: number;
  color: string | undefined;
  mode: LabelMode;
  onColor: (color: string | null) => void;
  onAuto: () => void;
  onRename: () => void;
  onDelete: () => void;
}

const ACTION = 'rounded p-1 text-muted transition-colors hover:bg-surface-hover hover:text-foreground';

/** One label of the tree. A grouping node (a parent no mailbox exists for) is only a heading. */
export function LabelRow({ node, depth, color, mode, onColor, onAuto, onRename, onDelete }: LabelRowProps) {
  const { mailbox } = node;
  return (
    <li
      style={{ paddingLeft: 8 + depth * 20 }}
      className="flex h-10 items-center gap-2 border-b border-border-subtle pr-2 text-sm"
    >
      {mailbox ? (
        <ColorPicker value={color} label={node.key} onChange={onColor} />
      ) : (
        <span className="w-6 shrink-0" />
      )}
      <span className={cn('flex-1 truncate', !mailbox && 'text-muted-foreground')}>{node.segment}</span>
      {mailbox && (
        <>
          <span className="w-16 text-right text-xs tabular-nums text-muted">{mailbox.totalMessages}</span>
          <span
            className={cn(
              'w-32 shrink-0 truncate rounded-full px-2 py-px text-center text-[11px]',
              mode === 'off' ? 'text-muted' : 'bg-accent/15 text-accent',
            )}
          >
            {MODE_NAMES[mode]}
          </span>
          <div className="flex shrink-0 items-center gap-0.5">
            <button type="button" aria-label={`Etichetta automatica di ${node.key}`} title="Etichetta automatica…" onClick={onAuto} className={ACTION}>
              <Sparkles size={14} />
            </button>
            <button type="button" aria-label={`Rinomina o sposta ${node.key}`} title="Rinomina o sposta…" onClick={onRename} className={ACTION}>
              <Pencil size={14} />
            </button>
            <button
              type="button"
              aria-label={`Elimina ${node.key}`}
              title="Elimina…"
              onClick={onDelete}
              className={cn(ACTION, 'hover:text-destructive')}
            >
              <Trash2 size={14} />
            </button>
          </div>
        </>
      )}
    </li>
  );
}
