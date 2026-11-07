import { RotateCw, Server } from 'lucide-react';
import { useRestartServerNow, useServerStatus, type ServerStatus } from '../model/useServer';

function describe(status: ServerStatus | undefined) {
  if (!status) return { label: 'Verifica in corso…', tone: 'text-muted' };
  if (status.state === 'down') return { label: 'Non raggiungibile', tone: 'text-destructive' };
  const outdated = status.state === 'legacy' || status.info?.version !== status.appVersion;
  return outdated
    ? { label: 'Raggiungibile, versione diversa dall’app', tone: 'text-amber-500' }
    : { label: 'Raggiungibile', tone: 'text-green-500' };
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex gap-3 text-xs">
      <dt className="w-20 shrink-0 text-muted">{label}</dt>
      <dd className="min-w-0 break-all text-foreground">{value}</dd>
    </div>
  );
}

/** Which server the app talks to, and the way to restart it onto the installed build. */
export function ServerSection() {
  const { data: status } = useServerStatus();
  const restart = useRestartServerNow();
  const { label, tone } = describe(status);
  const info = status?.info;

  return (
    <section className="space-y-4">
      <header>
        <h2 className="text-sm font-semibold text-foreground">Server</h2>
        <p className="text-xs text-muted">Il processo in background che sincronizza la posta</p>
      </header>

      <article className="rounded-lg border border-border bg-surface p-3">
        <div className="flex items-start justify-between gap-3">
          <div className="flex min-w-0 items-center gap-2 text-sm font-medium text-foreground">
            <Server size={15} />
            <span className={tone}>{label}</span>
          </div>
          <button
            type="button"
            disabled={restart.isPending}
            onClick={() => restart.mutate()}
            className="flex shrink-0 items-center gap-1.5 rounded border border-border bg-background px-2 py-1.5 text-xs text-foreground hover:bg-surface-hover disabled:opacity-50"
          >
            <RotateCw size={13} className={restart.isPending ? 'animate-spin' : undefined} />
            {restart.isPending ? 'Riavvio in corso…' : 'Riavvia server'}
          </button>
        </div>

        <dl className="mt-3 space-y-1.5">
          <Row label="Versione" value={info?.version ?? (status?.state === 'legacy' ? 'precedente alla 0.1.3' : '—')} />
          <Row label="App" value={status?.appVersion ?? '—'} />
          <Row label="Eseguibile" value={info?.executablePath ?? '—'} />
        </dl>

        {restart.isError && (
          <p className="mt-3 text-xs text-destructive">
            Riavvio non riuscito: {String(restart.error)}
          </p>
        )}
      </article>
    </section>
  );
}
