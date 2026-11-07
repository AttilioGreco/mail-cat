import { useState } from 'react';
import { format } from 'date-fns';
import { it } from 'date-fns/locale';
import { CalendarIcon } from 'lucide-react';
import { Button } from '@/shared/ui/button';
import { Calendar } from '@/shared/ui/calendar';
import { Popover, PopoverContent, PopoverTrigger } from '@/shared/ui/popover';

interface Props {
  /** `yyyy-MM-dd`, the same string the forms keep. */
  value: string;
  onChange: (value: string) => void;
  disabled?: boolean;
}

export function DatePicker({ value, onChange, disabled }: Props) {
  const [open, setOpen] = useState(false);
  // Parsed as local midnight: `new Date('yyyy-MM-dd')` would be UTC and show the previous day west of Greenwich.
  const date = new Date(`${value}T00:00`);
  const valid = !Number.isNaN(+date);
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger render={<Button type="button" variant="outline" disabled={disabled} className="justify-start font-normal" />}>
        <CalendarIcon />
        {valid ? format(date, 'EEE d MMM yyyy', { locale: it }) : 'Scegli una data'}
      </PopoverTrigger>
      <PopoverContent className="w-auto p-0">
        <Calendar
          mode="single"
          selected={valid ? date : undefined}
          defaultMonth={valid ? date : undefined}
          onSelect={(d) => {
            if (!d) return;
            onChange(format(d, 'yyyy-MM-dd'));
            setOpen(false);
          }}
        />
      </PopoverContent>
    </Popover>
  );
}
