import { useState } from 'react';
import { CalendarCheck, CalendarPlus, Copy, ExternalLink, FolderInput, Sun, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import { ContextMenu } from '@/shared/ui/ContextMenu';
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent,
  AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from '@/shared/ui/alert-dialog';
import { useAccounts } from '@/entities/account';
import { useCalendarGroups, useCreateEvent, useDeleteEvent, useEventReminders, type AccountEvent } from '@/entities/calendar';
import { useEventEditor } from '@/features/edit-event';
import { copyOf, eventActions, moveTargets, type MenuTarget } from '../lib/menu';
import { useCalendarView } from '../model/view-store';

export type MenuState = { x: number; y: number; target: MenuTarget };

const ITEM = 'flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-foreground transition-colors hover:bg-surface-hover';

/** Always mounted: the delete confirmation has to outlive the menu that asked for it. */
export function CalendarMenu({ menu, onClose }: { menu: MenuState | null; onClose: () => void }) {
  const openEditor = useEventEditor((s) => s.open);
  const today = useCalendarView((s) => s.today);
  const groups = useCalendarGroups(useAccounts().data ?? []);
  const create = useCreateEvent();
  const reminders = useEventReminders();
  const remove = useDeleteEvent();
  const [confirming, setConfirming] = useState<AccountEvent | null>(null);

  const run = (action: () => void) => () => {
    onClose();
    action();
  };

  const duplicate = async (e: AccountEvent) => {
    try {
      const href = await create.mutateAsync({ accountId: e.accountId, event: copyOf(e, e.calendar, await reminders(e.accountId, e.href)) });
      openEditor({ mode: 'edit', event: { ...e, href, dirty: true } });
    } catch (err) {
      toast.error(`Impossibile duplicare l'evento: ${String(err)}`);
    }
  };

  const move = async (e: AccountEvent, calendar: string) => {
    try {
      await create.mutateAsync({ accountId: e.accountId, event: copyOf(e, calendar, await reminders(e.accountId, e.href)) });
    } catch (err) {
      toast.error(`Impossibile spostare l'evento: ${String(err)}`);
      return;
    }
    // ponytail: create-then-delete is not atomic: if the delete fails the event exists in both calendars,
    // a duplicate and never a loss. A server-side move would make it atomic.
    try {
      await remove.mutateAsync({ accountId: e.accountId, href: e.href });
    } catch (err) {
      toast.error(`Evento copiato ma non rimosso dal calendario di origine: ${String(err)}`);
    }
  };

  const doDelete = async (e: AccountEvent) => {
    try {
      await remove.mutateAsync({ accountId: e.accountId, href: e.href });
    } catch (err) {
      toast.error(`Impossibile eliminare l'evento: ${String(err)}`);
    }
  };

  const target = menu?.target;
  const event = target?.kind === 'event' ? target.event : null;
  const actions = event ? eventActions(event) : [];
  const targets = event && actions.includes('move')
    ? moveTargets(event, groups.find((g) => g.account.id === event.accountId)?.calendars ?? [])
    : [];

  return (
    <>
      {menu && target && (
        <ContextMenu x={menu.x} y={menu.y} onClose={onClose}>
          {target.kind === 'slot' && (
            <>
              <button type="button" className={ITEM} onClick={run(() => openEditor({ mode: 'create', start: target.start, end: target.end, allDay: target.allDay }))}>
                <CalendarPlus size={14} className="shrink-0" />
                Nuovo evento
              </button>
              <button type="button" className={ITEM} onClick={run(() => openEditor({ mode: 'create', start: target.day, end: target.day, allDay: true }))}>
                <Sun size={14} className="shrink-0" />
                Nuovo evento di tutto il giorno
              </button>
              <button type="button" className={ITEM} onClick={run(today)}>
                <CalendarCheck size={14} className="shrink-0" />
                Vai a oggi
              </button>
            </>
          )}
          {event && (
            <>
              <button type="button" className={ITEM} onClick={run(() => openEditor({ mode: 'edit', event }))}>
                <ExternalLink size={14} className="shrink-0" />
                Apri
              </button>
              {actions.includes('duplicate') && (
                <button type="button" className={ITEM} onClick={run(() => void duplicate(event))}>
                  <Copy size={14} className="shrink-0" />
                  Duplica
                </button>
              )}
              {targets.length > 0 && (
                <>
                  <div className="my-1 border-t border-border" />
                  <div className="flex items-center gap-2 px-3 py-1 text-xs text-muted-foreground">
                    <FolderInput size={12} className="shrink-0" />
                    Sposta in calendario
                  </div>
                  {targets.map((c) => (
                    <button key={c.collection} type="button" className={ITEM} onClick={run(() => void move(event, c.collection))}>
                      <span className="truncate pl-5">{c.name}</span>
                    </button>
                  ))}
                  <div className="my-1 border-t border-border" />
                </>
              )}
              {actions.includes('delete') && (
                <button type="button" className={ITEM} onClick={run(() => setConfirming(event))}>
                  <Trash2 size={14} className="shrink-0" />
                  Elimina
                </button>
              )}
            </>
          )}
        </ContextMenu>
      )}
      <AlertDialog open={confirming !== null} onOpenChange={(open) => !open && setConfirming(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Eliminare l'evento?</AlertDialogTitle>
            <AlertDialogDescription>
              {confirming?.recurring ? 'Verrà eliminata l\'intera serie.' : 'L\'operazione non si può annullare.'}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Annulla</AlertDialogCancel>
            <AlertDialogAction variant="destructive" onClick={() => confirming && void doDelete(confirming)}>
              Elimina
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}
