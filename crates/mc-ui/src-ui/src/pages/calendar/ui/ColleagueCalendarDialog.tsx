import { useEffect, useId, useState } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { Button } from '@/shared/ui/button';
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from '@/shared/ui/dialog';
import { Input } from '@/shared/ui/input';
import { useSubscribeCalendar } from '@/entities/calendar';

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

interface ContactHit {
  display_name: string;
  all_emails: string[];
}

const MESSAGES: Record<string, string> = {
  not_found: 'Nessun calendario trovato per questo indirizzo',
  forbidden: 'Il collega non ha condiviso il calendario con te',
};

/** One address, with the contacts the account knows offered as suggestions. */
export function ColleagueCalendarDialog({ accountId, onClose }: { accountId: string | null; onClose: () => void }) {
  const [email, setEmail] = useState('');
  const [hits, setHits] = useState<ContactHit[]>([]);
  const subscribe = useSubscribeCalendar();
  const listId = useId();
  const address = email.trim();

  useEffect(() => {
    if (!accountId || !address) return;
    let live = true;
    const timer = setTimeout(() => {
      invoke<ContactHit[]>('search_contacts', { query: address, limit: 8, requestingAccountId: accountId })
        .then((found) => live && setHits(found))
        .catch(() => live && setHits([]));
    }, 150);
    return () => {
      live = false;
      clearTimeout(timer);
    };
  }, [accountId, address]);

  const close = () => {
    setEmail('');
    subscribe.reset();
    onClose();
  };

  const submit = () => {
    if (!accountId || !EMAIL_RE.test(address)) return;
    subscribe.mutate({ accountId, email: address }, { onSuccess: close });
  };

  const failure = subscribe.error == null ? null : (MESSAGES[String(subscribe.error)] ?? String(subscribe.error));

  return (
    <Dialog open={accountId !== null} onOpenChange={(open) => !open && close()}>
      <DialogContent className="sm:max-w-sm">
        <DialogHeader>
          <DialogTitle>Aggiungi calendario di un collega</DialogTitle>
          <DialogDescription>Indirizzo email del collega che ha condiviso il suo calendario con te.</DialogDescription>
        </DialogHeader>
        <form
          id={listId}
          onSubmit={(e) => {
            e.preventDefault();
            submit();
          }}
        >
          <Input
            type="email"
            autoFocus
            list={`${listId}-contacts`}
            placeholder="collega@azienda.it"
            value={email}
            onChange={(e) => {
              setEmail(e.target.value);
              subscribe.reset();
            }}
          />
          <datalist id={`${listId}-contacts`}>
            {hits.flatMap((h) =>
              h.all_emails.map((a) => (
                <option key={a} value={a}>{h.display_name}</option>
              )),
            )}
          </datalist>
          {failure && <p className="mt-2 text-xs text-destructive">{failure}</p>}
        </form>
        <DialogFooter>
          <Button variant="outline" onClick={close}>Annulla</Button>
          <Button form={listId} type="submit" disabled={!EMAIL_RE.test(address) || subscribe.isPending}>
            {subscribe.isPending ? 'Aggiungo…' : 'Aggiungi'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
