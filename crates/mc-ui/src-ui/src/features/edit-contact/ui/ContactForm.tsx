import { useState, type ReactNode } from 'react';
import { toast } from 'sonner';
import { Button } from '@/shared/ui/button';
import { Input } from '@/shared/ui/input';
import { SimpleSelect } from '@/shared/ui/select';
import { DialogFooter } from '@/shared/ui/dialog';
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent,
  AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from '@/shared/ui/alert-dialog';
import { useAccounts } from '@/entities/account';
import {
  useContactGroups, useCreateContact, useDeleteContact, useUpdateContact, type ContactEditorTarget,
} from '@/entities/contact';
import { fieldsFromForm, type ContactForm as Form } from '../lib/form';

const TEXTAREA =
  'min-h-16 w-full resize-y rounded-lg border border-input bg-transparent px-2.5 py-1.5 text-sm outline-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 dark:bg-input/30';

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="grid gap-1 text-xs text-muted-foreground">
      {label}
      {children}
    </label>
  );
}

const bookKey = (accountId: string, collection: string) => `${accountId}::${collection}`;

export function ContactForm({ target, initial, onClose }: { target: ContactEditorTarget; initial: Form; onClose: () => void }) {
  const groups = useContactGroups(useAccounts().data ?? []);
  const create = useCreateContact();
  const update = useUpdateContact();
  const remove = useDeleteContact();
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [form, setForm] = useState<Form>(initial);
  const [book, setBook] = useState('');
  const set = <K extends keyof Form>(key: K, value: Form[K]) => setForm((f) => ({ ...f, [key]: value }));

  const contact = target.mode === 'edit' ? target.contact : null;
  const books = groups.flatMap((g) =>
    g.books.filter((b) => !b.readOnly).map((b) => ({
      value: bookKey(g.account.id, b.collection),
      label: `${b.name} · ${g.account.name}`,
      accountId: g.account.id,
      collection: b.collection,
    })),
  );
  // Resolved at render: the books may still be loading when the dialog opens.
  const chosen = books.find((b) => b.value === book) ?? books[0];
  const fields = fieldsFromForm(form);
  const busy = create.isPending || update.isPending || remove.isPending;
  const canSave = fields !== null && (contact !== null || chosen !== undefined) && !busy;

  const save = async () => {
    if (!fields) return;
    try {
      if (contact) await update.mutateAsync({ accountId: contact.accountId, contactId: contact.id, fields });
      else if (chosen) await create.mutateAsync({ accountId: chosen.accountId, addressBook: chosen.collection, fields });
      onClose();
    } catch (e) {
      toast.error(`Impossibile salvare il contatto: ${String(e)}`);
    }
  };

  const doDelete = async () => {
    if (!contact) return;
    try {
      await remove.mutateAsync({ accountId: contact.accountId, contactId: contact.id });
      onClose();
    } catch (e) {
      toast.error(`Impossibile eliminare il contatto: ${String(e)}`);
    }
  };

  return (
    <form
      className="grid gap-3"
      onSubmit={(e) => {
        e.preventDefault();
        if (canSave) void save();
      }}
    >
      <Input autoFocus placeholder="Nome" value={form.displayName} onChange={(e) => set('displayName', e.target.value)} />
      {!contact && (
        <Field label="Rubrica">
          <SimpleSelect
            value={chosen?.value ?? ''}
            onChange={setBook}
            items={books.length > 0 ? books : [{ value: '', label: 'Nessuna rubrica scrivibile' }]}
          />
        </Field>
      )}
      <Field label="Email (una per riga)">
        <textarea className={TEXTAREA} value={form.emails} onChange={(e) => set('emails', e.target.value)} />
      </Field>
      <Field label="Telefoni (uno per riga)">
        <textarea className={TEXTAREA} value={form.phones} onChange={(e) => set('phones', e.target.value)} />
      </Field>
      <Field label="Organizzazione">
        <Input value={form.organization} onChange={(e) => set('organization', e.target.value)} />
      </Field>
      <Field label="Note">
        <textarea className={TEXTAREA} value={form.notes} onChange={(e) => set('notes', e.target.value)} />
      </Field>

      <DialogFooter>
        {contact && (
          <Button type="button" variant="destructive" className="sm:mr-auto" disabled={busy} onClick={() => setConfirmDelete(true)}>
            Elimina
          </Button>
        )}
        <Button type="button" variant="outline" onClick={onClose}>
          Annulla
        </Button>
        <Button type="submit" disabled={!canSave}>
          Salva
        </Button>
      </DialogFooter>

      <AlertDialog open={confirmDelete} onOpenChange={setConfirmDelete}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Eliminare il contatto?</AlertDialogTitle>
            <AlertDialogDescription>Verrà eliminato anche dal server. L'operazione non si può annullare.</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Annulla</AlertDialogCancel>
            <AlertDialogAction variant="destructive" onClick={() => void doDelete()}>
              Elimina
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </form>
  );
}
