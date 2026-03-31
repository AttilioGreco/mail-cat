import { useState } from 'react';
import { ChevronDown, AlertCircle } from 'lucide-react';
import type { Identity } from '@/lib/composeTypes';

interface ConfirmIdentityDialogProps {
  identities: Identity[];
  /** Identità preselezionata (può non corrispondere a nessuna account configurato) */
  suggestedEmail: string;
  onConfirm: (identity: Identity) => void;
  onCancel: () => void;
}

export function ConfirmIdentityDialog({
  identities,
  suggestedEmail,
  onConfirm,
  onCancel,
}: ConfirmIdentityDialogProps) {
  const best = identities.find((i) => i.email === suggestedEmail) ?? identities[0];
  const [selected, setSelected] = useState<Identity>(best);

  return (
    <>
      {/* Overlay */}
      <div className="fixed inset-0 z-[60] bg-black/50 backdrop-blur-sm" />

      {/* Dialog */}
      <div
        role="dialog"
        aria-modal
        className="fixed left-1/2 top-1/2 z-[70] w-[400px] -translate-x-1/2 -translate-y-1/2 overflow-hidden rounded-xl border border-border bg-background shadow-2xl"
      >
        <div className="flex items-start gap-3 p-5">
          <AlertCircle size={20} className="mt-0.5 shrink-0 text-accent" />
          <div className="flex-1">
            <p className="text-sm font-semibold text-foreground">
              Scegli mittente per la risposta
            </p>
            <p className="mt-1 text-xs text-muted">
              Il messaggio originale era indirizzato a{' '}
              <span className="font-mono text-foreground">{suggestedEmail}</span>.
              Seleziona l'identità da usare per rispondere.
            </p>
          </div>
        </div>

        <div className="px-5 pb-3">
          <div className="relative">
            <select
              value={selected.id}
              onChange={(e) => {
                const identity = identities.find((i) => i.id === e.target.value);
                if (identity) setSelected(identity);
              }}
              className="w-full appearance-none rounded-lg border border-border bg-surface px-3 py-2 pr-8 text-sm text-foreground outline-none focus:border-accent"
            >
              {identities.map((i) => (
                <option key={i.id} value={i.id}>
                  {i.displayName} &lt;{i.email}&gt;
                </option>
              ))}
            </select>
            <ChevronDown
              size={14}
              className="pointer-events-none absolute right-2.5 top-1/2 -translate-y-1/2 text-muted"
            />
          </div>
        </div>

        <div className="flex justify-end gap-2 border-t border-border px-5 py-3">
          <button
            onClick={onCancel}
            className="rounded-lg px-3 py-1.5 text-sm text-muted-foreground transition-colors hover:bg-surface-hover"
          >
            Annulla
          </button>
          <button
            onClick={() => onConfirm(selected)}
            className="rounded-lg bg-accent px-4 py-1.5 text-sm font-medium text-accent-foreground transition-colors hover:bg-accent/90"
          >
            Continua
          </button>
        </div>
      </div>
    </>
  );
}
