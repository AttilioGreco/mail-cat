import { useState, type ReactNode } from 'react';
import { toast } from 'sonner';
import { Button } from '@/shared/ui/button';
import { selectedFirst } from '@/shared/lib/utils';
import { DatePicker } from '@/shared/ui/date-picker';
import { Input } from '@/shared/ui/input';
import { TimePicker } from '@/shared/ui/time-picker';
import { SimpleSelect } from '@/shared/ui/select';
import { DialogFooter } from '@/shared/ui/dialog';
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent,
  AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from '@/shared/ui/alert-dialog';
import { useAccounts } from '@/entities/account';
import { useSessionStore } from '@/entities/session';
import {
  calendarKey, useCalendarGroups, useCalendarVisibility, useCreateEvent, useDeleteEvent, useUpdateEvent,
  type AccountEvent,
} from '@/entities/calendar';
import { timesChanged, wireTimes, withStart, type EventForm } from '../lib/form';

const TEXTAREA =
  'min-h-16 w-full resize-y rounded-lg border border-input bg-transparent px-2.5 py-1.5 text-sm outline-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:opacity-50 dark:bg-input/30';

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="grid min-w-0 gap-1 text-xs text-muted-foreground">
      {label}
      {children}
    </label>
  );
}

interface Props {
  initial: EventForm;
  /** Present when editing an existing event. */
  event?: AccountEvent;
  onClose: () => void;
}

export function EventEditor({ initial, event, onClose }: Props) {
  const selected = useSessionStore((s) => s.selectedAccountId);
  const groups = useCalendarGroups(selectedFirst(useAccounts().data ?? [], selected));
  const lastCalendar = useCalendarVisibility((s) => s.lastCalendar);
  const setLastCalendar = useCalendarVisibility((s) => s.setLastCalendar);
  const create = useCreateEvent();
  const update = useUpdateEvent();
  const remove = useDeleteEvent();
  const [confirmDelete, setConfirmDelete] = useState(false);

  // An existing event keeps its calendar: the server has no move, so the select is only informative then.
  const choices = groups.flatMap((g) =>
    g.calendars
      .filter((c) => event || !c.readOnly)
      .map((c) => ({ value: calendarKey(g.account.id, c.collection), label: `${c.name} · ${g.account.name}`, accountId: g.account.id, collection: c.collection })),
  );
  const [form, setForm] = useState<EventForm>(initial);
  // Resolved at render, not at mount: the calendar list may still be loading when the dialog opens.
  const calendar = choices.some((c) => c.value === form.calendar)
    ? form.calendar
    : // A new event goes to the selected account: its last used calendar, else its first.
      (choices.find((c) => c.value === lastCalendar && (!selected || c.accountId === selected)) ?? choices[0])?.value ?? '';
  const set = <K extends keyof EventForm>(key: K, value: EventForm[K]) => setForm((f) => ({ ...f, [key]: value }));

  const readOnly = event?.readOnly ?? false;
  const times = wireTimes(form);
  const durationFromStart = (time: string) => {
    const start = new Date(`${form.startDate}T${form.startTime}`);
    const minutes = (+new Date(`${form.endDate}T${time}`) - +start) / 60_000;
    return minutes > 0 ? minutes : null;
  };
  const target = choices.find((c) => c.value === calendar);
  const busy = create.isPending || update.isPending || remove.isPending;
  const canSave = !readOnly && times !== null && (event !== undefined || target !== undefined) && !busy;

  const save = async () => {
    if (!times) return;
    try {
      if (event) {
        await update.mutateAsync({
          accountId: event.accountId,
          href: event.href,
          changes: {
            summary: form.summary.trim(),
            location: form.location.trim(),
            description: form.description,
            // Untouched times are left out so saving a series never rewrites its recurrence.
            ...(timesChanged(form, initial) ? times : {}),
          },
        });
      } else if (target) {
        await create.mutateAsync({
          accountId: target.accountId,
          event: {
            calendar: target.collection,
            summary: form.summary.trim() || 'Senza titolo',
            location: form.location.trim() || undefined,
            description: form.description || undefined,
            ...times,
          },
        });
        setLastCalendar(target.value);
      }
      onClose();
    } catch (e) {
      toast.error(`Impossibile salvare l'evento: ${String(e)}`);
    }
  };

  const doDelete = async () => {
    if (!event) return;
    try {
      await remove.mutateAsync({ accountId: event.accountId, href: event.href });
      onClose();
    } catch (e) {
      toast.error(`Impossibile eliminare l'evento: ${String(e)}`);
    }
  };

  return (
    // min-w-0: a grid item grows to its widest child, and a long calendar name
    // would push the form, and the footer under it, past the dialog's edge.
    <form
      className="grid min-w-0 gap-3"
      onSubmit={(e) => {
        e.preventDefault();
        if (canSave) void save();
      }}
    >
      <Input
        autoFocus
        placeholder="Titolo"
        value={form.summary}
        disabled={readOnly}
        onChange={(e) => set('summary', e.target.value)}
      />
      {event?.recurring && (
        <p className="text-xs text-muted-foreground">Evento ricorrente: modifica l'intera serie.</p>
      )}
      {readOnly && <p className="text-xs text-muted-foreground">Calendario in sola lettura.</p>}

      <Field label="Calendario">
        <SimpleSelect
          value={calendar}
          onChange={(v) => set('calendar', v)}
          items={choices.length > 0 ? choices : [{ value: '', label: 'Nessun calendario scrivibile' }]}
          disabled={event !== undefined}
          className="min-w-0"
        />
      </Field>

      <label className="flex items-center gap-2 text-sm">
        <input
          type="checkbox"
          className="accent-accent"
          checked={form.allDay}
          disabled={readOnly}
          onChange={(e) => set('allDay', e.target.checked)}
        />
        Tutto il giorno
      </label>

      <div className="grid grid-cols-[1fr_auto] gap-2">
        <Field label="Inizio">
          <DatePicker value={form.startDate} disabled={readOnly} onChange={(v) => setForm((f) => withStart(f, { startDate: v }))} />
        </Field>
        {!form.allDay && (
          <Field label="Ora">
            <TimePicker value={form.startTime} disabled={readOnly} onChange={(v) => setForm((f) => withStart(f, { startTime: v }))} />
          </Field>
        )}
        <Field label="Fine">
          <DatePicker value={form.endDate} disabled={readOnly} onChange={(v) => set('endDate', v)} />
        </Field>
        {!form.allDay && (
          <Field label="Ora">
            <TimePicker value={form.endTime} disabled={readOnly} durationOf={durationFromStart} onChange={(v) => set('endTime', v)} />
          </Field>
        )}
      </div>
      {!readOnly && times === null && (
        <p className="text-xs text-destructive">La fine deve essere successiva all'inizio.</p>
      )}

      <Field label="Luogo">
        <Input value={form.location} disabled={readOnly} onChange={(e) => set('location', e.target.value)} />
      </Field>
      <Field label="Descrizione">
        <textarea className={TEXTAREA} value={form.description} disabled={readOnly} onChange={(e) => set('description', e.target.value)} />
      </Field>

      <DialogFooter>
        {event && !readOnly && (
          <Button type="button" variant="destructive" className="sm:mr-auto" disabled={busy} onClick={() => setConfirmDelete(true)}>
            Elimina
          </Button>
        )}
        <Button type="button" variant="outline" onClick={onClose}>
          {readOnly ? 'Chiudi' : 'Annulla'}
        </Button>
        {!readOnly && (
          <Button type="submit" disabled={!canSave}>
            Salva
          </Button>
        )}
      </DialogFooter>

      <AlertDialog open={confirmDelete} onOpenChange={setConfirmDelete}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Eliminare l'evento?</AlertDialogTitle>
            <AlertDialogDescription>
              {event?.recurring ? 'Verrà eliminata l\'intera serie.' : 'L\'operazione non si può annullare.'}
            </AlertDialogDescription>
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
