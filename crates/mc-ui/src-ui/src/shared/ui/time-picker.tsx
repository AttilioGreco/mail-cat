import { useState } from 'react';
import { Combobox, ComboboxContent, ComboboxInput, ComboboxItem, ComboboxList } from '@/shared/ui/combobox';

const SLOTS = Array.from({ length: 96 }, (_, i) => {
  const m = i * 15;
  return `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`;
});
const HHMM = /^([01]?\d|2[0-3]):([0-5]\d)$/;

const normalize = (text: string) => {
  const m = HHMM.exec(text.trim());
  return m ? `${m[1].padStart(2, '0')}:${m[2]}` : null;
};

interface Props {
  /** `HH:mm`, the same string the forms keep. */
  value: string;
  onChange: (value: string) => void;
  disabled?: boolean;
  /** Minutes to show next to an option, e.g. the length of the event when picking the end. Null hides the option. */
  durationOf?: (time: string) => number | null;
}

function formatDuration(minutes: number) {
  if (minutes < 60) return `${minutes} min`;
  return `${String(minutes / 60).replace('.', ',')} h`;
}

export function TimePicker({ value, onChange, disabled, durationOf }: Props) {
  const [text, setText] = useState(value);
  const [synced, setSynced] = useState(value);
  // An external change (e.g. the end following the start) replaces what is typed.
  if (value !== synced) {
    setSynced(value);
    setText(value);
  }

  const commit = (raw: string) => {
    const t = normalize(raw);
    if (t) onChange(t);
    else setText(value);
  };

  const options = durationOf
    ? SLOTS.flatMap((t) => {
        const d = durationOf(t);
        return d === null ? [] : [{ time: t, hint: formatDuration(d) }];
      })
    : SLOTS.map((t) => ({ time: t, hint: '' }));

  return (
    <Combobox
      items={options.map((o) => o.time)}
      filter={null}
      value={value}
      inputValue={text}
      disabled={disabled}
      onInputValueChange={setText}
      onValueChange={(v) => {
        if (v) commit(v);
      }}
    >
      <ComboboxInput
        className="w-28"
        disabled={disabled}
        placeholder="HH:MM"
        onBlur={(e) => commit(e.currentTarget.value)}
        onKeyDown={(e) => {
          if (e.key !== 'Enter') return;
          // The editor is a form: Enter would submit before the typed time is committed.
          e.preventDefault();
          commit(e.currentTarget.value);
        }}
      />
      <ComboboxContent className="min-w-36">
        <ComboboxList>
          {options.map((o) => (
            <ComboboxItem key={o.time} value={o.time}>
              {o.time}
              {o.hint && <span className="text-muted-foreground">({o.hint})</span>}
            </ComboboxItem>
          ))}
        </ComboboxList>
      </ComboboxContent>
    </Combobox>
  );
}
