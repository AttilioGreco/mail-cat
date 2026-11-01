import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/shared/ui/dialog';
import { useAccounts, type Account } from '@/entities/account';
import { useMailboxes, subscribedOnly } from '@/entities/mailbox';
import type { LabelTarget } from '../model/dialogStore';
import { useChangeLabels, useCreateLabel, useModifyTags } from '../model/useLabelMutations';
import { LabelCombobox, type LabelOption } from './LabelCombobox';

/** Labels are mailboxes on a Labels account; elsewhere only keywords exist, when the server takes them. */
export function labelMode(account: Account | undefined): 'labels' | 'tags' | null {
  if (account?.mailboxModel === 'labels') return 'labels';
  return account?.supportsTags === true ? 'tags' : null;
}

/** The gRPC status text of a refused last-label removal is not for display. */
function pickerError(e: Error): string {
  return /precondition/i.test(e.message) ? 'Un messaggio deve avere almeno un’etichetta.' : e.message;
}

function stateOf(messages: LabelTarget[], has: (m: LabelTarget) => boolean): LabelOption['state'] {
  const n = messages.filter(has).length;
  if (n === 0) return 'none';
  return n === messages.length ? 'all' : 'some';
}

/** Names the classifier and the user have used on this account's mail, for the keyword picker. */
function useKnownTags(accountId: string, enabled: boolean) {
  return useQuery({
    queryKey: ['label-names', accountId],
    enabled,
    queryFn: async () =>
      (await invoke<{ labels: { name: string }[] }[]>('list_labels', { accountId })).flatMap((s) =>
        s.labels.map((l) => l.name),
      ),
  });
}

interface LabelPickerDialogProps {
  accountId: string;
  messages: LabelTarget[];
  onClose: () => void;
}

/** Toggle labels on `messages` (all of them at once), or create one by typing it. */
export function LabelPickerDialog({ accountId, messages, onClose }: LabelPickerDialogProps) {
  const account = useAccounts().data?.find((a) => a.id === accountId);
  const mode = labelMode(account);
  const { data: mailboxes = [] } = useMailboxes(accountId);
  const { data: knownTags = [] } = useKnownTags(accountId, mode === 'tags');
  const change = useChangeLabels();
  const modify = useModifyTags();
  const createLabel = useCreateLabel();
  // The list this dialog was opened with, kept current by its own changes.
  const [current, setCurrent] = useState(messages);
  const [error, setError] = useState<Error | null>(null);

  const options: LabelOption[] =
    mode === 'labels'
      ? // A label the messages carry stays listed even unsubscribed, so it can still be removed.
        subscribedOnly(mailboxes)
          .concat(mailboxes.filter((m) => !m.subscribed && current.some((c) => c.mailboxIds.includes(m.id))))
          .map((m) => ({ id: m.id, path: m.displayPath, state: stateOf(current, (c) => c.mailboxIds.includes(m.id)) }))
      : [...new Set([...knownTags, ...current.flatMap((c) => c.tags)])]
          .sort()
          .map((t) => ({ id: t, path: t, state: stateOf(current, (c) => c.tags.includes(t)) }));

  const apply = async (id: string, on: boolean) => {
    const ids = current.map((c) => c.id);
    const patch = (list: string[]) => (on ? (list.includes(id) ? list : [...list, id]) : list.filter((x) => x !== id));
    const params = { messageIds: ids, add: on ? [id] : [], remove: on ? [] : [id] };
    setError(null);
    try {
      await (mode === 'labels' ? change : modify).mutateAsync(params);
      setCurrent((list) =>
        list.map((c) => (mode === 'labels' ? { ...c, mailboxIds: patch(c.mailboxIds) } : { ...c, tags: patch(c.tags) })),
      );
    } catch (e) {
      setError(e as Error);
    }
  };

  const create = async (path: string) => {
    if (mode === 'tags') return apply(path, true);
    setError(null);
    try {
      const id = await createLabel.mutateAsync({ accountId, path });
      await apply(id, true);
    } catch (e) {
      setError(e as Error);
    }
  };

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent
        className="min-h-80 content-start"
        onKeyDownCapture={(e) => {
          // The open list swallows Escape; closing is the dialog's job.
          if (e.key === 'Escape') onClose();
        }}
      >
        <DialogHeader>
          <DialogTitle>Etichette</DialogTitle>
          <DialogDescription>
            {current.length === 1 ? 'Un messaggio' : `${current.length} messaggi`}. Invio o clic per aggiungere o
            togliere; scrivi un nome nuovo per crearlo («Dev/GitHub» crea un’etichetta annidata).
          </DialogDescription>
        </DialogHeader>
        {mode ? (
          <LabelCombobox
            options={options}
            onPick={(o) => void apply(o.id, o.state !== 'all')}
            onCreate={(path) => void create(path)}
          />
        ) : (
          <p className="text-xs text-muted">Questo account non supporta le etichette.</p>
        )}
        {error && (
          <p role="alert" className="text-[11px] text-destructive">
            {pickerError(error)}
          </p>
        )}
      </DialogContent>
    </Dialog>
  );
}
