import { useState } from 'react';
import { Check, Archive, X } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/shared/ui/button';
import { Input } from '@/shared/ui/input';
import { useProposalMutations } from '../model/useAutoLabel';
import type { LabelProposal } from '../model/types';

function Proposal({ p, showGroup }: { p: LabelProposal; showGroup: boolean }) {
  const { accept, reject } = useProposalMutations();
  const [name, setName] = useState(p.name);
  // Archiving takes mail out of the Inbox, so it asks again before acting.
  const [confirmArchive, setConfirmArchive] = useState(false);
  const busy = accept.isPending || reject.isPending;
  const fail = (e: Error) => toast.error(String(e));
  const take = (archive: boolean) =>
    accept.mutate(
      { id: p.id, name, archive },
      {
        onSuccess: (n) => toast.success(`Etichetta “${name.trim() || p.name}”: ${n} messaggi in arrivo`),
        onError: fail,
      },
    );

  return (
    <li className="space-y-2 rounded-lg border border-border p-3">
      <div className="flex items-center gap-2">
        <Input
          value={name}
          onChange={(e) => setName(e.target.value)}
          aria-label="Nome dell'etichetta"
          className="h-8 text-sm font-medium"
        />
        <span className="shrink-0 text-[11px] text-muted">
          {p.size.toLocaleString()} messaggi
          {p.inbox > 0 && ` · ${p.inbox.toLocaleString()} in arrivo`}
          {showGroup && p.group && ` · ${p.group}`}
        </span>
      </div>
      {p.keywords.length > 0 && (
        <div className="flex flex-wrap gap-1.5">
          {p.keywords.map((k) => (
            <span key={k} className="rounded-full bg-accent/15 px-1.5 py-px text-[10px] leading-4 text-accent">
              {k}
            </span>
          ))}
        </div>
      )}
      <ul className="space-y-0.5 text-[11px] text-muted">
        {p.samples.slice(0, 5).map((s) => (
          <li key={s.messageId} className="truncate">
            <span className="text-foreground">{s.subject || '(senza oggetto)'}</span> — {s.from}
          </li>
        ))}
      </ul>
      <div className="flex flex-wrap items-center justify-end gap-2">
        <Button variant="ghost" size="sm" disabled={busy} onClick={() => reject.mutate(p.id, { onError: fail })}>
          <X />
          Scarta
        </Button>
        {confirmArchive ? (
          <>
            <span className="text-[11px] text-muted">Toglierli da Posta in arrivo ({p.inbox.toLocaleString()})?</span>
            <Button variant="ghost" size="sm" disabled={busy} onClick={() => setConfirmArchive(false)}>
              Annulla
            </Button>
            <Button size="sm" disabled={busy || !name.trim()} onClick={() => take(true)}>
              <Archive />
              Conferma
            </Button>
          </>
        ) : (
          <>
            <Button
              variant="outline"
              size="sm"
              disabled={busy || !name.trim() || p.inbox === 0}
              title={p.inbox === 0 ? 'Nessun messaggio in Posta in arrivo' : undefined}
              onClick={() => setConfirmArchive(true)}
            >
              <Archive />
              Accetta e archivia
            </Button>
            <Button size="sm" disabled={busy || !name.trim()} onClick={() => take(false)}>
              <Check />
              Accetta
            </Button>
          </>
        )}
      </div>
    </li>
  );
}

/** Labels discovery proposes, most useful for the Inbox first. */
export function ProposalsList({ proposals }: { proposals: LabelProposal[] }) {
  const groups = new Set(proposals.map((p) => p.group));
  return (
    <ul className="space-y-2">
      {proposals.map((p) => (
        <Proposal key={p.id} p={p} showGroup={groups.size > 1} />
      ))}
    </ul>
  );
}
