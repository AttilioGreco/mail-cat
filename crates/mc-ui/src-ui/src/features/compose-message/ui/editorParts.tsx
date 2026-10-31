import { useEffect, useRef, useState } from 'react';
import DOMPurify from 'dompurify';
import { ChevronDown, ChevronRight } from 'lucide-react';
import { cn } from '@/shared/lib/utils';
import type { InlineImage } from '../model/composeTypes';

/** What both editors (visual and markdown) are given by ComposeWindow. */
export interface EditorProps {
  initialBody: string;
  onBodyChange: (body: string) => void;
  onImageAdd: (image: InlineImage) => void;
  onSend: () => void;
  onSaveDraft: () => void;
  onAttach: () => void;
  onError: (msg: string) => void;
  signatureHtml: string | null;
  quotedHtml: string | null;
  autoFocus: boolean;
}

// Pasted pages, quotes and signatures come from other people: never trusted.
// blob: is allowed so inline images still show before they become cid:.
export const sanitize = (html: string) =>
  DOMPurify.sanitize(html, {
    ALLOWED_URI_REGEXP: /^(?:https?:|mailto:|tel:|cid:|blob:|data:image\/|#)/i,
    FORBID_TAGS: ['style', 'script', 'iframe', 'object', 'embed', 'form'],
  });

export function ToolBtn({ icon, label, active, onClick, className }: {
  icon: React.ReactNode; label: string; active?: boolean;
  onClick?: () => void; className?: string;
}) {
  return (
    <button
      type="button"
      title={label}
      aria-label={label}
      // Keeps the caret in the editor; the action itself runs on click so the
      // keyboard (Enter/Space on a focused button) works too.
      onMouseDown={(e) => e.preventDefault()}
      onClick={onClick}
      className={cn(
        'rounded p-1.5 transition-colors text-muted-foreground hover:bg-surface-hover hover:text-foreground',
        active && 'bg-surface-active text-foreground',
        className,
      )}
    >
      {icon}
    </button>
  );
}

export function ToolbarDivider() {
  return <div className="mx-2 h-4 w-px bg-border" />;
}

/** Signature above the quote, as Apple Mail places them; both read-only. */
export function EditorFooter({ signatureHtml, quotedHtml }: {
  signatureHtml: string | null;
  quotedHtml: string | null;
}) {
  return (
    <>
      {signatureHtml && (
        <div className="shrink-0 max-h-24 overflow-y-auto border-t border-border">
          <div className="mx-6 mb-3 mt-3 rounded-md border border-dashed border-border bg-surface/20 px-3 py-2">
            <div className="mb-1 text-xs text-muted">Firma</div>
            <div
              className="overflow-hidden text-sm text-muted-foreground [&_*]:pointer-events-none"
              dangerouslySetInnerHTML={{ __html: sanitize(signatureHtml) }}
            />
          </div>
        </div>
      )}
      {quotedHtml && (
        <div className="shrink-0 max-h-32 overflow-y-auto border-t border-border">
          <QuotedBlockPanel html={quotedHtml} />
        </div>
      )}
    </>
  );
}

function QuotedBlockPanel({ html }: { html: string }) {
  const [collapsed, setCollapsed] = useState(true);
  return (
    <div className="mx-6 mb-3 mt-3 rounded-md border border-border bg-surface/30">
      <button
        type="button"
        onClick={() => setCollapsed((c) => !c)}
        className="flex w-full items-center gap-1.5 px-3 py-1.5 text-xs text-muted transition-colors hover:text-foreground"
      >
        {collapsed ? <ChevronRight size={12} /> : <ChevronDown size={12} />}
        Citazione originale
      </button>
      {!collapsed && (
        <div
          className="overflow-hidden border-t border-border px-3 py-2 text-xs text-muted-foreground [&_*]:pointer-events-none"
          dangerouslySetInnerHTML={{ __html: sanitize(html) }}
        />
      )}
    </div>
  );
}

/** Ctrl/Cmd+Shift+K toggles the palette. */
export function usePaletteShortcut(toggle: () => void) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.shiftKey && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        toggle();
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [toggle]);
}

export function CommandPalette({ items, onClose, onSelect }: {
  items: readonly string[];
  onClose: () => void;
  onSelect: (item: string) => void;
}) {
  const [query, setQuery] = useState('');
  const [cursor, setCursor] = useState(0);
  const [prevQuery, setPrevQuery] = useState(query);
  const inputRef = useRef<HTMLInputElement>(null);
  const filtered = items.filter((i) => i.toLowerCase().includes(query.toLowerCase()));

  if (query !== prevQuery) {
    setPrevQuery(query);
    setCursor(0);
  }

  useEffect(() => { inputRef.current?.focus(); }, []);

  return (
    <div className="absolute inset-x-0 top-0 z-10 mx-4 mt-2 overflow-hidden rounded-lg border border-border bg-background shadow-xl">
      <input
        ref={inputRef}
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'ArrowDown') { e.preventDefault(); setCursor((c) => Math.min(c + 1, filtered.length - 1)); }
          else if (e.key === 'ArrowUp') { e.preventDefault(); setCursor((c) => Math.max(c - 1, 0)); }
          else if (e.key === 'Enter') { if (filtered[cursor]) onSelect(filtered[cursor]); }
          // Stopped here so Escape closes the palette, not the compose window.
          else if (e.key === 'Escape') { e.stopPropagation(); onClose(); }
        }}
        placeholder="Cerca comando..."
        className="w-full border-b border-border bg-transparent px-4 py-3 text-sm text-foreground outline-none placeholder:text-muted"
      />
      <ul className="max-h-52 overflow-y-auto py-1">
        {filtered.length === 0 && <li className="px-4 py-2 text-xs text-muted">Nessun risultato</li>}
        {filtered.map((item, idx) => (
          <li key={item} onClick={() => onSelect(item)}
            className={cn('cursor-pointer px-4 py-2 text-sm transition-colors',
              idx === cursor ? 'bg-accent/10 text-accent' : 'text-foreground hover:bg-surface-hover')}>
            {item}
          </li>
        ))}
      </ul>
    </div>
  );
}
