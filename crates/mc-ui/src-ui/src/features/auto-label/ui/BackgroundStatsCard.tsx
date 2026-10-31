import { ProgressRow } from '@/shared/ui/progress-row';
import { useBackgroundStats } from '../model/useAutoLabel';
import type { EmbedderState } from '../model/types';

const STATE_LABEL: Record<EmbedderState, string> = {
  off: 'Spento',
  loading: 'Caricamento del modello',
  active: 'Attivo',
  paused_for_retrain: 'In pausa: riaddestramento in corso',
  idle: 'Inattivo',
};

function eta(seconds: number): string {
  if (seconds < 90) return 'meno di 2 minuti';
  const minutes = Math.round(seconds / 60);
  if (minutes < 90) return `circa ${minutes} minuti`;
  return `circa ${(minutes / 60).toFixed(1)} ore`;
}

/** What the background work is doing now; updates every 2 s while the tab is open. */
export function BackgroundStatsCard() {
  const { data: s } = useBackgroundStats();
  if (!s) return null;
  const remaining = Math.max(0, s.eligible - s.embedded);
  const idleNote = s.state === 'idle' && !s.modelLoaded ? ' (modello scaricato dalla memoria)' : '';
  return (
    <div className="space-y-2 rounded-lg border border-border bg-surface p-3 text-[11px]">
      <div className="flex items-baseline justify-between gap-3">
        <span className="font-medium text-foreground">
          {STATE_LABEL[s.state]}
          {idleNote}
        </span>
        <span className="text-muted">
          {s.workers} × {s.threads} thread · gruppi da {s.batch}
          {s.workerBytes > 0 && ` · ~${Math.round(s.workerBytes / 1e6)} MB per modello`}
        </span>
      </div>
      {s.state !== 'off' && (
        <ProgressRow
          label="Messaggi analizzati"
          done={s.embedded}
          total={s.eligible}
          missingLabel={
            remaining === 0
              ? 'Tutti analizzati'
              : `${remaining.toLocaleString()} da analizzare${
                  s.perSecond > 0 ? ` · ${s.perSecond.toFixed(1)} messaggi/s` : ''
                }${s.etaSeconds > 0 ? ` · ${eta(s.etaSeconds)}` : ''}`
          }
        />
      )}
      {s.retrains.map((r) => (
        <ProgressRow
          key={r.scope}
          label={`Riaddestramento: ${r.scope}`}
          done={r.done}
          total={r.total}
          missingLabel={`${Math.max(0, r.total - r.done).toLocaleString()} esempi da imparare`}
        />
      ))}
    </div>
  );
}
