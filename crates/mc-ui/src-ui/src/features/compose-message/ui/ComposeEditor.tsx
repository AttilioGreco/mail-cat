import { useCallback, useEffect, useRef, useState } from 'react';
import DOMPurify from 'dompurify';
import CodeMirror, { type ReactCodeMirrorRef } from '@uiw/react-codemirror';
import { markdown } from '@codemirror/lang-markdown';
import { EditorView } from '@codemirror/view';
import {
  Bold, Italic, Code, Link, List, Minus,
  Paperclip, CommandIcon, Eye, FileCode,
  ChevronRight, ChevronDown,
} from 'lucide-react';
import { cn } from '@/shared/lib/utils';
import { markdownToPreviewHtml } from '@/shared/lib/markdownPreview';
import { extractImageFiles, processImageFile } from './milkdown/inlineImagePlugin';
import type { InlineImage } from '../model/composeTypes';
import { v4 as uuidv4 } from 'uuid';

interface ComposeEditorProps {
  initialMd?: string;
  onMdChange?: (md: string) => void;
  onImagesChange?: (images: InlineImage[]) => void;
  onSend?: () => void;
  onSaveDraft?: () => void;
  onAttach?: () => void;
  onError?: (msg: string) => void;
  signatureHtml?: string | null;
  quotedHtml?: string | null;
  autoFocus?: boolean;
}

// Quote and signature come from other people's mail and account settings:
// never trusted. blob: is allowed so inline images still show in the preview.
const sanitize = (html: string) =>
  DOMPurify.sanitize(html, { ALLOWED_URI_REGEXP: /^(?:https?:|mailto:|cid:|blob:|data:image\/|#)/i });

type ViewMode = 'write' | 'preview';

// ── CodeMirror theme ─────────────────────────────────────────────────────────
// Uses the app's CSS variables (defined in globals.css as space-separated HSL
// components, so they must be wrapped in hsl()).

const mailCatTheme = EditorView.theme({
  '&': {
    color: 'hsl(var(--foreground))',
    backgroundColor: 'transparent',
    fontSize: '0.875rem',
    fontFamily: 'inherit',
    height: '100%',
  },
  '&.cm-focused': { outline: 'none' },
  '.cm-scroller': {
    fontFamily: 'inherit',
    lineHeight: '1.625',
    overflow: 'auto',
  },
  '.cm-content': {
    padding: '1rem 1.5rem',
    caretColor: 'hsl(var(--foreground))',
    minHeight: '100%',
  },
  '.cm-cursor, .cm-dropCursor': {
    borderLeftColor: 'hsl(var(--foreground))',
  },
  '&.cm-focused .cm-selectionBackground, .cm-selectionBackground, ::selection': {
    backgroundColor: 'rgba(59,130,246,0.18)',
  },
  '.cm-activeLine': { backgroundColor: 'transparent' },
  '.cm-gutters': { display: 'none' },
  // Markdown syntax colours
  '.tok-heading': { color: 'hsl(var(--accent))', fontWeight: '600' },
  '.tok-strong':  { fontWeight: '700' },
  '.tok-emphasis': { fontStyle: 'italic' },
  '.tok-monospace, .tok-code': { color: '#fbbf24', fontFamily: 'monospace' },
  '.tok-link': { color: 'hsl(var(--accent))', textDecoration: 'underline' },
  '.tok-url':  { color: 'hsl(var(--muted-foreground))' },
  '.tok-quote': { color: 'hsl(var(--muted-foreground))', fontStyle: 'italic' },
  '.cm-placeholder': { color: 'hsl(var(--muted))' },
}, { dark: true });

// CodeMirror's default drop handler pastes dropped files in as text. Claim
// file drops so they reach React instead: images become inline images here,
// anything else bubbles up to ComposeWindow as an attachment.
const fileDrops = EditorView.domEventHandlers({
  drop: (e) => !!e.dataTransfer?.files.length,
});

const EXTENSIONS = [markdown(), mailCatTheme, fileDrops];

// ── Toolbar ──────────────────────────────────────────────────────────────────

function Toolbar({
  viewMode, onViewMode,
  onAttach, onOpenPalette,
  onBold, onItalic, onCode, onLink, onList, onHr,
}: {
  viewMode: ViewMode;
  onViewMode: (m: ViewMode) => void;
  onAttach: () => void;
  onOpenPalette: () => void;
  onBold: () => void; onItalic: () => void; onCode: () => void;
  onLink: () => void; onList: () => void;  onHr: () => void;
}) {
  return (
    <div className="flex items-center gap-0.5 border-b border-border px-3 py-1.5 shrink-0">
      <div className="flex items-center rounded-md border border-border mr-2">
        <ToolBtn icon={<FileCode size={13} />} label="Modifica (MD)"
          active={viewMode === 'write'} onClick={() => onViewMode('write')}
          className="rounded-l-md rounded-r-none border-0" />
        <div className="h-4 w-px bg-border" />
        <ToolBtn icon={<Eye size={13} />} label="Anteprima"
          active={viewMode === 'preview'} onClick={() => onViewMode('preview')}
          className="rounded-r-md rounded-l-none border-0" />
      </div>

      {viewMode === 'write' && (
        <>
          <ToolBtn icon={<Bold size={14} />}   label="Grassetto (Ctrl+B)" onClick={onBold} />
          <ToolBtn icon={<Italic size={14} />} label="Corsivo (Ctrl+I)"   onClick={onItalic} />
          <ToolBtn icon={<Code size={14} />}   label="Codice (Ctrl+E)"    onClick={onCode} />
          <ToolBtn icon={<Link size={14} />}   label="Link (Ctrl+K)"      onClick={onLink} />
          <ToolBtn icon={<List size={14} />}   label="Lista"               onClick={onList} />
          <ToolBtn icon={<Minus size={14} />}  label="Separatore"          onClick={onHr} />
          <div className="mx-2 h-4 w-px bg-border" />
        </>
      )}

      <ToolBtn icon={<Paperclip size={14} />} label="Allega file" onClick={onAttach} />
      <ToolBtn icon={<CommandIcon size={14} />} label="Command palette (Ctrl+Shift+K)"
        onClick={onOpenPalette} />
    </div>
  );
}

function ToolBtn({ icon, label, active, onClick, className }: {
  icon: React.ReactNode; label: string; active?: boolean;
  onClick?: () => void; className?: string;
}) {
  return (
    <button
      type="button"
      title={label}
      onMouseDown={(e) => { e.preventDefault(); onClick?.(); }}
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

// ── Quoted block panel ────────────────────────────────────────────────────────

function QuotedBlockPanel({ html }: { html: string }) {
  const [collapsed, setCollapsed] = useState(true);
  return (
    <div className="mx-6 mb-3 rounded-md border border-border bg-surface/30">
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
          className="border-t border-border px-3 py-2 text-xs text-muted-foreground [&_*]:pointer-events-none overflow-hidden"
          dangerouslySetInnerHTML={{ __html: sanitize(html) }}
        />
      )}
    </div>
  );
}

// ── Signature block panel ─────────────────────────────────────────────────────

function SignatureBlockPanel({ html }: { html: string }) {
  return (
    <div className="mx-6 mb-3 rounded-md border border-border border-dashed bg-surface/20 px-3 py-2">
      <div className="mb-1 text-xs text-muted">Firma</div>
      <div
        className="text-sm text-muted-foreground [&_*]:pointer-events-none overflow-hidden"
        dangerouslySetInnerHTML={{ __html: sanitize(html) }}
      />
    </div>
  );
}

// ── Command Palette ───────────────────────────────────────────────────────────

const PALETTE_ITEMS = [
  'Invia', 'Salva bozza', 'Allega file',
  'Inserisci tabella', 'Inserisci blocco codice',
] as const;

function CommandPalette({ onClose, onSelect }: {
  onClose: () => void;
  onSelect: (item: string) => void;
}) {
  const [query, setQuery] = useState('');
  const [cursor, setCursor] = useState(0);
  const [prevQuery, setPrevQuery] = useState(query);
  const inputRef = useRef<HTMLInputElement>(null);
  const filtered = PALETTE_ITEMS.filter((i) => i.toLowerCase().includes(query.toLowerCase()));

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

// ── Preview ───────────────────────────────────────────────────────────────────

function PreviewPane({ markdown: md, quotedHtml, signatureHtml }: {
  markdown: string;
  quotedHtml?: string | null;
  signatureHtml?: string | null;
}) {
  const [html, setHtml] = useState('');
  useEffect(() => {
    let fullMd = md;
    // btoa only takes Latin-1: go through UTF-8 bytes first.
    const b64 = (h: string) => btoa(String.fromCharCode(...new TextEncoder().encode(h)));
    if (signatureHtml) fullMd += `\n<!-- signature-block: ${b64(signatureHtml)} -->`;
    if (quotedHtml) fullMd += `\n<!-- quoted-block: ${b64(quotedHtml)} -->`;
    markdownToPreviewHtml(fullMd).then((out) => setHtml(sanitize(out)));
  }, [md, quotedHtml, signatureHtml]);
  return (
    <div
      className="flex-1 overflow-y-auto px-6 py-5 prose prose-sm max-w-none text-foreground [&_a]:text-accent [&_a]:underline"
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}

// ── ComposeEditor (root) ──────────────────────────────────────────────────────

export function ComposeEditor({
  initialMd = '',
  onMdChange,
  onImagesChange,
  onSend,
  onSaveDraft,
  onAttach = () => {},
  onError,
  signatureHtml,
  quotedHtml,
  autoFocus,
}: ComposeEditorProps) {
  const [viewMode, setViewMode] = useState<ViewMode>('write');
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [currentMd, setCurrentMd] = useState(initialMd);
  const cmRef = useRef<ReactCodeMirrorRef>(null);
  const imagesRef = useRef<Map<string, InlineImage>>(new Map());

  // ── Helper: insert/wrap text at cursor via CodeMirror API ─────────────────

  const insertMarkdown = useCallback((
    prefix: string, suffix = '', defaultText = 'testo',
  ) => {
    const view = cmRef.current?.view;
    if (!view) return;
    const { from, to } = view.state.selection.main;
    const selected = from === to
      ? defaultText
      : view.state.sliceDoc(from, to);
    view.dispatch({
      changes: { from, to, insert: prefix + selected + suffix },
      selection: { anchor: from + prefix.length + selected.length + suffix.length },
    });
    view.focus();
  }, []);

  // Send and save shortcuts live in ComposeWindow: they must work before this
  // lazy chunk has loaded and outside the editor too.
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.shiftKey && e.key.toLowerCase() === 'k') {
        e.preventDefault(); setPaletteOpen((p) => !p);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, []);

  // ── Inline images ─────────────────────────────────────────────────────────

  const handleImageFiles = useCallback(async (files: File[]) => {
    for (const file of files) {
      const cid = `${uuidv4()}@compose`;
      const result = await processImageFile(file, () => cid);
      if (result.error !== undefined) { onError?.(result.error); continue; }
      imagesRef.current.set(result.image.cid, result.image);
      onImagesChange?.(Array.from(imagesRef.current.values()));
      setCurrentMd((prev) => { const n = prev + '\n' + result.markdown + '\n'; onMdChange?.(n); return n; });
    }
  }, [onMdChange, onImagesChange, onError]);

  const handleDrop = useCallback((e: React.DragEvent) => {
    const files = extractImageFiles(e.dataTransfer);
    if (!files.length) return;
    e.preventDefault();
    handleImageFiles(files);
  }, [handleImageFiles]);

  // ── Palette ───────────────────────────────────────────────────────────────

  const handlePaletteSelect = useCallback((item: string) => {
    setPaletteOpen(false);
    if (item === 'Invia') onSend?.();
    else if (item === 'Salva bozza') onSaveDraft?.();
    else if (item === 'Allega file') onAttach();
    else if (item === 'Inserisci blocco codice') insertMarkdown('```\n', '\n```', 'codice');
    else if (item === 'Inserisci tabella') {
      insertMarkdown('\n| Colonna 1 | Colonna 2 |\n|-----------|----------|\n| cella     | cella     |\n', '', '');
    }
    requestAnimationFrame(() => cmRef.current?.view?.focus());
  }, [onSend, onSaveDraft, onAttach, insertMarkdown]);

  return (
    <div
      className="relative flex flex-1 flex-col overflow-hidden"
      onDrop={handleDrop}
      onDragOver={(e) => e.preventDefault()}
    >
      <Toolbar
        viewMode={viewMode} onViewMode={setViewMode}
        onAttach={onAttach} onOpenPalette={() => setPaletteOpen((p) => !p)}
        onBold={() => insertMarkdown('**', '**', 'grassetto')}
        onItalic={() => insertMarkdown('*', '*', 'corsivo')}
        onCode={() => insertMarkdown('`', '`', 'codice')}
        onLink={() => insertMarkdown('[', '](url)', 'testo')}
        onList={() => insertMarkdown('- ', '', 'elemento')}
        onHr={() => insertMarkdown('\n---\n', '', '')}
      />

      {paletteOpen && (
        <CommandPalette
          onClose={() => { setPaletteOpen(false); cmRef.current?.view?.focus(); }}
          onSelect={handlePaletteSelect}
        />
      )}

      {viewMode === 'write' ? (
        <div className="flex-1 flex flex-col overflow-hidden">
          {/* Editor — fills all remaining space */}
          <div className="flex-1 min-h-0 overflow-hidden">
            <CodeMirror
              ref={cmRef}
              value={currentMd}
              onChange={(val) => { setCurrentMd(val); onMdChange?.(val); }}
              extensions={EXTENSIONS}
              placeholder="Scrivi il tuo messaggio..."
              autoFocus={autoFocus}
              height="100%"
              basicSetup={{
                lineNumbers: false,
                foldGutter: false,
                dropCursor: true,
                allowMultipleSelections: false,
                indentOnInput: false,
                bracketMatching: false,
                closeBrackets: false,
                autocompletion: false,
                rectangularSelection: false,
                highlightActiveLine: false,
                highlightSelectionMatches: false,
                closeBracketsKeymap: false,
                searchKeymap: false,
              }}
              style={{ height: '100%' }}
            />
          </div>

          {/* Signature block — above the quote, as Apple Mail places it */}
          {signatureHtml && (
            <div className="shrink-0 max-h-24 overflow-y-auto border-t border-border">
              <SignatureBlockPanel html={signatureHtml} />
            </div>
          )}

          {/* Quoted block — pinned below editor */}
          {quotedHtml && (
            <div className="shrink-0 max-h-32 overflow-y-auto border-t border-border">
              <QuotedBlockPanel html={quotedHtml} />
            </div>
          )}
        </div>
      ) : (
        <PreviewPane
          markdown={currentMd}
          quotedHtml={quotedHtml}
          signatureHtml={signatureHtml}
        />
      )}

      {/* Drop zone overlay */}
      <div
        className="pointer-events-none absolute inset-0 hidden items-center justify-center rounded-lg border-2 border-dashed border-accent bg-accent/5 text-sm text-accent [.dragging_&]:flex"
        aria-hidden
      >
        Rilascia le immagini qui
      </div>
    </div>
  );
}
