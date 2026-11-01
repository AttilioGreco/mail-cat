import { useState } from 'react';
import { ChevronDown, ChevronRight } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/shared/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/shared/ui/dialog';
import { useApplyAll, useApplyAllPreview } from '../model/useAutoLabel';
import type { LabelApply } from '../model/types';

const rowKey = (l: LabelApply) => `${l.scope}/${l.label}`;

/** Label the existing mail the classifier is sure of, for the labels the user picks. */
export function AutoLabelAllDialog({ onClose }: { onClose: () => void }) {
  const { data: report, isPending, error } = useApplyAllPreview();
  const apply = useApplyAll();
  // Only the rows the user touched; the rest follow the label's mode.
  const [touched, setTouched] = useState<Record<string, boolean>>({});
  const [open, setOpen] = useState<string | null>(null);

  const labels = [...(report?.labels ?? [])].sort((a, b) => b.toLabel - a.toLabel);
  const isOn = (l: LabelApply) => touched[rowKey(l)] ?? l.mode !== 'off';
  const chosen = labels.filter(isOn);
  const total = chosen.reduce((sum, l) => sum + l.toLabel, 0);

  const run = () =>
    apply.mutate(chosen.map(({ scope, label }) => ({ scope, label })), {
      onSuccess: () => {
        toast.success('Etichettatura avviata: l’avanzamento è nella scheda Elaborazione in background');
        onClose();
      },
    });

  return (
    <Dialog open onOpenChange={(o) => !o && !apply.isPending && onClose()}>
      <DialogContent className="max-h-[85vh] grid-rows-[auto_minmax(0,1fr)_auto] sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>Auto-etichetta tutta la posta</DialogTitle>
          <DialogDescription>
            Applica le etichette (almeno 5 esempi ciascuna) ai messaggi esistenti di cui il modello è sicuro. Non
            archivia nulla e non cambia le modalità.
          </DialogDescription>
        </DialogHeader>

        <div className="min-h-0 space-y-1 overflow-y-auto text-xs">
          {isPending && <p className="text-muted">Calcolo in corso…</p>}
          {error && <p className="text-destructive">{String(error)}</p>}
          {report && labels.length === 0 && <p className="text-muted">Nessuna etichetta ha abbastanza esempi.</p>}
          {labels.map((l) => {
            const key = rowKey(l);
            return (
              <div key={key} className="rounded-lg border border-border px-2 py-1.5">
                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    aria-label={l.name}
                    checked={isOn(l)}
                    onChange={(e) => setTouched((t) => ({ ...t, [key]: e.target.checked }))}
                  />
                  <span className="min-w-0 flex-1 truncate text-foreground">{l.name}</span>
                  <span className="shrink-0 text-muted">{l.mode === 'off' ? 'spenta' : l.mode}</span>
                  <span className="w-12 shrink-0 text-right tabular-nums text-foreground">{l.toLabel}</span>
                  <button
                    type="button"
                    aria-expanded={open === key}
                    aria-label={`Esempi di ${l.name}`}
                    disabled={l.sample.length === 0}
                    onClick={() => setOpen(open === key ? null : key)}
                    className="shrink-0 text-muted hover:text-foreground disabled:opacity-30"
                  >
                    {open === key ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                  </button>
                </div>
                {open === key && (
                  <ul className="mt-1 space-y-0.5 pl-6 text-muted">
                    {l.sample.map((s) => (
                      <li key={s.messageId} className="truncate">
                        {s.subject || '(senza oggetto)'} — {s.from}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            );
          })}
        </div>

        {apply.error && <p role="alert" className="text-xs text-destructive">{String(apply.error)}</p>}
        <DialogFooter>
          <Button variant="outline" disabled={apply.isPending} onClick={onClose}>
            Annulla
          </Button>
          <Button disabled={total === 0 || apply.isPending} onClick={run}>
            Etichetta {total} messaggi
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
