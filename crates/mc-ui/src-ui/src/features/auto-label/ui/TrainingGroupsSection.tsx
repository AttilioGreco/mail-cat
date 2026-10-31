import { useState } from 'react';
import { Plus, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/shared/ui/button';
import { Input } from '@/shared/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/shared/ui/select';
import { useAccounts } from '@/entities/account';
import { BackgroundStatsCard } from './BackgroundStatsCard';
import { useEmbeddings, useGroupMutations, useTrainingGroups } from '../model/useAutoLabel';

const NO_GROUP = '';

const SPEEDS = [
  { value: 'light', label: 'Leggera' },
  { value: 'normal', label: 'Normale' },
  { value: 'max', label: 'Massima' },
] as const;

/** Which accounts learn their labels together. Accounts of a group share what
 *  each label looks like, matched by name. */
export function TrainingGroupsSection() {
  const { data: groups = [] } = useTrainingGroups();
  const { data: accounts = [] } = useAccounts();
  const { create, rename, remove, assign } = useGroupMutations();
  const { status: embeddings, set: setEmbeddings, setSpeed } = useEmbeddings();
  const [name, setName] = useState('');
  const fail = (e: Error) => toast.error(String(e));

  const groupItems = [{ value: NO_GROUP, label: 'Nessun gruppo' }, ...groups.map((g) => ({ value: g.id, label: g.name }))];

  return (
    <section className="space-y-6">
      <header>
        <h2 className="text-sm font-semibold text-foreground">Etichette automatiche</h2>
        <p className="text-xs text-muted">
          Gli account dello stesso gruppo imparano insieme: le etichette con lo stesso nome (maiuscole escluse) valgono
          come una sola. Un account senza gruppo impara da solo.
        </p>
      </header>

      {embeddings?.available && (
        <label className="flex items-start gap-2 text-xs text-foreground">
          <input
            type="checkbox"
            className="mt-0.5"
            checked={embeddings.enabled}
            disabled={setEmbeddings.isPending}
            onChange={(e) => setEmbeddings.mutate(e.target.checked, { onError: fail })}
          />
          <span>
            Riconosci anche i messaggi simili (scarica una volta un modello di ~120 MB; tutto resta sul tuo computer)
          </span>
        </label>
      )}

      {embeddings && (
        <div className="flex items-center justify-between gap-3">
          <div className="text-xs">
            <div className="text-foreground">Velocità elaborazione in background</div>
            <p className="text-muted">
              Quanto computer possono usare apprendimento e analisi dei messaggi; hanno sempre priorità bassa, quindi non
              rallentano le altre app.
            </p>
          </div>
          <Select
            value={embeddings.speed}
            items={SPEEDS}
            onValueChange={(v: string | null) =>
              v !== null && setSpeed.mutate(v as typeof embeddings.speed, { onError: fail })
            }
          >
            <SelectTrigger size="sm" className="w-36 shrink-0">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {SPEEDS.map((s) => (
                <SelectItem key={s.value} value={s.value}>
                  {s.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )}
      {embeddings && <BackgroundStatsCard />}

      <div className="space-y-2">
        <h3 className="text-xs font-medium text-muted-foreground">Account</h3>
        {accounts.map((a) => {
          const current = groups.find((g) => g.accountIds.includes(a.id))?.id ?? NO_GROUP;
          return (
            <div key={a.id} className="flex items-center justify-between gap-3 rounded-lg border border-border px-3 py-2">
              <div className="min-w-0 text-xs">
                <div className="truncate text-foreground">{a.name}</div>
                <div className="truncate text-muted">{a.email}</div>
              </div>
              <Select
                value={current}
                items={groupItems}
                onValueChange={(v: string | null) =>
                  v !== null && assign.mutate({ accountId: a.id, groupId: v === NO_GROUP ? null : v }, { onError: fail })
                }
              >
                <SelectTrigger size="sm" className="w-44 shrink-0">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {groupItems.map((g) => (
                    <SelectItem key={g.value} value={g.value}>
                      {g.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          );
        })}
      </div>

      <div className="space-y-2">
        <h3 className="text-xs font-medium text-muted-foreground">Gruppi</h3>
        {groups.map((g) => (
          <div key={g.id} className="flex items-center gap-2">
            <Input
              defaultValue={g.name}
              aria-label={`Nome del gruppo ${g.name}`}
              onBlur={(e) => {
                const next = e.target.value.trim();
                if (next && next !== g.name) rename.mutate({ id: g.id, name: next }, { onError: fail });
              }}
              className="h-8 text-xs"
            />
            <span className="w-20 shrink-0 text-[11px] text-muted">{g.accountIds.length} account</span>
            <Button
              variant="ghost"
              size="icon-sm"
              title="Elimina gruppo"
              disabled={g.isDefault}
              onClick={() => remove.mutate(g.id, { onError: fail })}
            >
              <Trash2 />
            </Button>
          </div>
        ))}
        <form
          className="flex items-center gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            if (!name.trim()) return;
            create.mutate(name, { onSuccess: () => setName(''), onError: fail });
          }}
        >
          <Input
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="Nuovo gruppo"
            aria-label="Nome del nuovo gruppo"
            className="h-8 text-xs"
          />
          <Button type="submit" size="sm" variant="outline" disabled={!name.trim() || create.isPending}>
            <Plus />
            Crea
          </Button>
        </form>
      </div>
    </section>
  );
}
