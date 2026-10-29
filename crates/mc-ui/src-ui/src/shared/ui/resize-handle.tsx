import { useRef } from 'react';
import { cn } from '@/shared/lib/utils';

interface ResizeHandleProps {
  width: number;
  onResize: (width: number) => void;
  onCommit: (width: number) => void;
  /** Double-click restores the default width. */
  onReset: () => void;
}

/**
 * Vertical 1px divider between two panes; drag it to resize the pane on its left.
 * The hit area is wider than the line so it's easy to grab, but only grows to
 * the right: on the left it would cover the scrollbar of the pane being resized.
 */
export function ResizeHandle({ width, onResize, onCommit, onReset }: ResizeHandleProps) {
  const drag = useRef<{ startX: number; startWidth: number } | null>(null);

  return (
    <div
      role="separator"
      aria-orientation="vertical"
      className="group relative w-px shrink-0 cursor-col-resize bg-border"
      onPointerDown={(e) => {
        if (e.button !== 0) return;
        e.preventDefault();
        e.currentTarget.setPointerCapture(e.pointerId);
        drag.current = { startX: e.clientX, startWidth: width };
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
      }}
      onPointerMove={(e) => {
        const d = drag.current;
        if (!d) return;
        onResize(d.startWidth + e.clientX - d.startX);
      }}
      onPointerUp={(e) => {
        if (!drag.current) return;
        e.currentTarget.releasePointerCapture(e.pointerId);
        drag.current = null;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        onCommit(width);
      }}
      onDoubleClick={onReset}
    >
      <div
        className={cn(
          'absolute inset-y-0 left-0 -right-1.5 z-10',
          'transition-colors group-hover:bg-accent/40 group-active:bg-accent/60',
        )}
      />
    </div>
  );
}
