import { Dialog, DialogContent, DialogTitle } from '@/shared/ui/dialog';
import { calendarKey, useEvent } from '@/entities/calendar';
import { useEventEditor } from '../model/editor-store';
import { formFromOccurrence, formFromRange } from '../lib/form';
import { EventEditor } from './EventEditor';

/** Mounted once by the main page; renders nothing, and loads nothing, until an event is opened. */
export function EventDialog() {
  const { target, close } = useEventEditor();
  return (
    <Dialog open={target !== null} onOpenChange={(open) => !open && close()}>
      {target && (
        <DialogContent className="sm:max-w-md">
          <DialogTitle>{target.mode === 'create' ? 'Nuovo evento' : target.event.readOnly ? 'Evento' : 'Modifica evento'}</DialogTitle>
          <Loaded />
        </DialogContent>
      )}
    </Dialog>
  );
}

/** A series shows its master's times, which the clicked occurrence does not carry. */
function Loaded() {
  const target = useEventEditor((s) => s.target);
  const close = useEventEditor((s) => s.close);
  const event = target?.mode === 'edit' ? target.event : null;
  const series = useEvent(event?.recurring ? event.accountId : null, event?.recurring ? event.href : null);

  if (target?.mode === 'create') {
    return <EventEditor key="new" initial={formFromRange(target.start, target.end, target.allDay, '')} onClose={close} />;
  }
  if (!event) return null;
  const source = event.recurring ? series.data?.first : event;
  if (!source) {
    return <p className="text-muted-foreground">{series.error ? 'Impossibile caricare la serie.' : 'Caricamento…'}</p>;
  }
  const initial = formFromOccurrence(source, calendarKey(event.accountId, event.calendar));
  return <EventEditor key={event.href} initial={initial} event={event} onClose={close} />;
}
