import { useState } from 'react';
import { Sparkles } from 'lucide-react';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/shared/ui/dialog';
import { useLabelProposals } from '../model/useAutoLabel';
import { ProposalsList } from './ProposalsList';

/** A button for the folder bar, with how many labels discovery proposes; it
 *  opens them. Renders nothing while there are none. */
export function ProposalsBadge() {
  const { data: proposals = [] } = useLabelProposals();
  const [open, setOpen] = useState(false);
  if (proposals.length === 0) return null;

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="flex w-full items-center gap-2 rounded-md px-2 h-8 text-sm text-accent transition-colors hover:bg-surface-hover"
      >
        <Sparkles size={15} className="shrink-0" />
        <span className="flex-1 truncate text-left">Etichette proposte</span>
        <span className="min-w-[18px] rounded-full bg-accent px-1.5 py-0.5 text-center text-[10px] font-semibold leading-none tabular-nums text-accent-foreground">
          {proposals.length > 99 ? '99+' : proposals.length}
        </span>
      </button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="max-h-[80vh] overflow-y-auto sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Etichette proposte</DialogTitle>
            <DialogDescription>
              Gruppi di messaggi simili senza etichetta. Accettando se ne crea l&apos;etichetta e la si applica ai messaggi.
            </DialogDescription>
          </DialogHeader>
          <ProposalsList proposals={proposals} />
        </DialogContent>
      </Dialog>
    </>
  );
}
