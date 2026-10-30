import { useState } from 'react';
import { FolderTree } from 'lucide-react';
import { FoldersDialog } from './FoldersDialog';

/** Opens the folder subscription dialog of one account. */
export function ManageFoldersButton({ accountId, accountName }: { accountId: string; accountName: string }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button
        type="button"
        title="Gestisci cartelle"
        onClick={() => setOpen(true)}
        className="rounded border border-border bg-background p-1.5 text-muted hover:bg-surface-hover hover:text-foreground"
      >
        <FolderTree size={13} />
      </button>
      <FoldersDialog open={open} accountId={accountId} accountName={accountName} onClose={() => setOpen(false)} />
    </>
  );
}
