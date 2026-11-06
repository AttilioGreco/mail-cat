import { cn } from '@/shared/lib/utils';
import { labelColorStyle } from '@/shared/lib/label-colors';

/** A label's color as a small dot; nothing for a label without one. */
export function LabelDot({ color, className }: { color?: string | null; className?: string }) {
  const style = labelColorStyle(color);
  if (!style) return null;
  return <span aria-hidden style={style} className={cn('inline-block h-2 w-2 shrink-0 rounded-full', className)} />;
}
