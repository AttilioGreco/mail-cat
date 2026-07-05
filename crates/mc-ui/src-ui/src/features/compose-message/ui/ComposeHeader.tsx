import { useEffect, useState } from 'react';
import { ChevronDown } from 'lucide-react';
import { ChipInput } from './ChipInput';
import type { ComposeInitialData, HeaderState, Identity } from '../model/composeTypes';

interface ComposeHeaderProps {
  initialData: ComposeInitialData;
  onChange: (state: HeaderState) => void;
}

export function ComposeHeader({ initialData, onChange }: ComposeHeaderProps) {
  const identities = initialData.identities;

  const [selectedIdentity, setSelectedIdentity] = useState<Identity | null>(null);
  const [to, setTo] = useState<string[]>(initialData.to ?? []);
  const [cc, setCc] = useState<string[]>(initialData.cc ?? []);
  const [bcc, setBcc] = useState<string[]>(initialData.bcc ?? []);
  const [subject, setSubject] = useState(buildSubject(initialData));
  const [showCcBcc, setShowCcBcc] = useState(
    (initialData.cc?.length ?? 0) > 0 || (initialData.bcc?.length ?? 0) > 0,
  );

  // Risincronizza l'identità selezionata ogni volta che le identità vengono
  // caricate (arrivano dopo il primo render) o che initialData.from cambia.
  useEffect(() => {
    if (identities.length === 0) return;
    const match =
      identities.find((i) => i.email === initialData.from) ?? identities[0] ?? null;
    // ponytail: identities load asynchronously after mount, this isn't a pure
    // prop->state derivation the render-time-adjustment pattern would fit.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setSelectedIdentity(match);
  }, [initialData.from, identities]);

  // Propaga lo stato al parent ad ogni modifica
  useEffect(() => {
    if (!selectedIdentity) return;
    onChange({ from: selectedIdentity, to, cc, bcc, subject });
  }, [selectedIdentity, to, cc, bcc, subject]);

  return (
    <div className="flex flex-col border-b border-border">
      {/* From */}
      <div className="flex items-center gap-2 px-4 py-2 border-b border-border">
        <span className="shrink-0 text-xs text-muted w-14">Da</span>
        <div className="relative flex-1">
          <select
            value={selectedIdentity?.id ?? ''}
            onChange={(e) => {
              const identity = identities.find((i) => i.id === e.target.value) ?? null;
              setSelectedIdentity(identity);
            }}
            className="w-full appearance-none bg-transparent text-xs text-foreground outline-none pr-6"
          >
            {identities.length === 0 && (
              <option value="">Nessuna identità configurata</option>
            )}
            {identities.map((i) => (
              <option key={i.id} value={i.id}>
                {i.displayName} &lt;{i.email}&gt;
              </option>
            ))}
          </select>
          <ChevronDown size={12} className="pointer-events-none absolute right-0 top-1/2 -translate-y-1/2 text-muted" />
        </div>
      </div>

      {/* To */}
      <ChipInput
        label="A"
        values={to}
        onChange={setTo}
        placeholder="Destinatari..."
        accountId={initialData.accountId}
        autoFocus
      />

      {/* CC / BCC toggle */}
      {!showCcBcc && (
        <div className="px-4 py-1.5 border-b border-border">
          <button
            type="button"
            onClick={() => setShowCcBcc(true)}
            className="text-xs text-muted hover:text-foreground transition-colors"
          >
            + CC / BCC
          </button>
        </div>
      )}

      {showCcBcc && (
        <>
          <ChipInput label="CC" values={cc} onChange={setCc} placeholder="Cc..." accountId={initialData.accountId} />
          <ChipInput label="BCC" values={bcc} onChange={setBcc} placeholder="Ccn..." accountId={initialData.accountId} />
        </>
      )}

      {/* Subject */}
      <div className="flex items-center gap-2 px-4 py-2">
        <span className="shrink-0 text-xs text-muted w-14">Oggetto</span>
        <input
          type="text"
          value={subject}
          onChange={(e) => setSubject(e.target.value)}
          placeholder="Oggetto..."
          className="flex-1 bg-transparent text-sm font-medium text-foreground outline-none placeholder:text-muted"
        />
      </div>
    </div>
  );
}

function buildSubject(data: ComposeInitialData): string {
  const base = data.subject ?? '';
  if (data.mode === 'reply') return base.startsWith('Re:') ? base : `Re: ${base}`;
  if (data.mode === 'forward') return base.startsWith('Fwd:') ? base : `Fwd: ${base}`;
  return base;
}
