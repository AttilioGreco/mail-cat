/** A label, a done/total count and a bar: the backlog look used for sync and background work. */
export function ProgressRow({
  label,
  done,
  total,
  missingLabel,
}: {
  label: string;
  done: number;
  total: number;
  missingLabel: string;
}) {
  const percent = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 100;

  return (
    <div>
      <div className="mb-1 flex items-baseline justify-between text-[11px]">
        <span className="text-muted">{label}</span>
        <span className="text-foreground">
          {done.toLocaleString()} / {total.toLocaleString()} · {percent}%
        </span>
      </div>
      <div className="h-1.5 overflow-hidden rounded-full bg-background">
        <div className="h-full rounded-full bg-accent transition-[width] duration-700 ease-out" style={{ width: `${percent}%` }} />
      </div>
      <p className="mt-0.5 text-[11px] text-muted">{missingLabel}</p>
    </div>
  );
}
