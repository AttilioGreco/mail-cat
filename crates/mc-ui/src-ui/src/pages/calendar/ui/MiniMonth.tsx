import { useState } from 'react';
import { isSameMonth } from 'date-fns';
import { Calendar } from '@/shared/ui/calendar';
import { useCalendarView } from '../model/view-store';

/** Keeps its own displayed month so browsing it does not move the main view; only picking a day does. */
export function MiniMonth() {
  const { date, setDate } = useCalendarView();
  const [month, setMonth] = useState(date);
  // The main view can jump elsewhere (today, toolbar arrows): follow it, but only when it leaves the shown month.
  const [seen, setSeen] = useState(date);
  if (+seen !== +date) {
    setSeen(date);
    if (!isSameMonth(date, month)) setMonth(date);
  }
  return (
    <Calendar
      mode="single"
      className="w-full"
      selected={date}
      month={month}
      onMonthChange={setMonth}
      onSelect={(d) => d && setDate(d)}
    />
  );
}
