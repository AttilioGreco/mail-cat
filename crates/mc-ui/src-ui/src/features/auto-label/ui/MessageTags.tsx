import { Check, Undo2, X } from 'lucide-react';
import { toast } from 'sonner';
import { LabelChip } from '@/shared/ui/label-chip';
import { useAcceptSuggestion, useMessageTags, useRejectSuggestion, useUndoAutoLabel } from '../model/useAutoLabel';

const LEVEL = { high: 'alta', medium: 'media' } as const;

const iconButton = 'rounded-full p-0.5 transition-colors hover:bg-surface-hover focus-visible:outline focus-visible:outline-1';

/** What the classifier suggests for a message (accept or reject) and the
 *  labels it put on by itself (marked automatic, undoable). Renders nothing
 *  when it has nothing to say. */
export function MessageTags({ messageId }: { messageId: string }) {
  const { data } = useMessageTags(messageId);
  const accept = useAcceptSuggestion();
  const reject = useRejectSuggestion();
  const undo = useUndoAutoLabel();
  if (!data || (data.suggestions.length === 0 && data.auto.length === 0)) return null;

  const fail = (e: Error) => toast.error(String(e));
  return (
    <div className="mt-2 flex flex-wrap items-center gap-1.5" aria-label="Etichette automatiche">
      {data.auto.map((a) => (
        <span key={a.label} className="inline-flex items-center gap-1">
          <LabelChip name={a.name} />
          <span className="text-[10px] text-muted">
            automatica{a.archived ? ', archiviata' : ''}
          </span>
          <button
            type="button"
            title="Annulla etichetta automatica"
            aria-label={`Annulla ${a.name}`}
            disabled={undo.isPending}
            onClick={() => undo.mutate({ messageId, label: a.label }, { onError: fail })}
            className={`${iconButton} text-muted-foreground`}
          >
            <Undo2 size={11} />
          </button>
        </span>
      ))}
      {data.suggestions.map((s) => (
        <span
          key={s.label}
          className="inline-flex items-center gap-1 rounded-full border border-dashed border-accent/60 px-1.5 py-px text-[10px] leading-4 text-accent"
        >
          <span className="max-w-[10rem] truncate">{s.name}?</span>
          <span className="text-muted">{LEVEL[s.confidence]}</span>
          <button
            type="button"
            title="Applica etichetta"
            aria-label={`Applica ${s.name}`}
            disabled={accept.isPending}
            onClick={() => accept.mutate({ messageId, label: s.label }, { onError: fail })}
            className={`${iconButton} hover:text-accent`}
          >
            <Check size={11} />
          </button>
          <button
            type="button"
            title="Non è questa"
            aria-label={`Rifiuta ${s.name}`}
            disabled={reject.isPending}
            onClick={() => reject.mutate({ messageId, label: s.label }, { onError: fail })}
            className={`${iconButton} hover:text-destructive`}
          >
            <X size={11} />
          </button>
        </span>
      ))}
    </div>
  );
}
