import { useCallback, useEffect, useRef, useState } from 'react';
import CodeMirror, { type ReactCodeMirrorRef } from '@uiw/react-codemirror';
import { markdown } from '@codemirror/lang-markdown';
import { Prec } from '@codemirror/state';
import { EditorView, keymap } from '@codemirror/view';
import { Bold, Italic, Code, Link, List, Minus, Paperclip, CommandIcon, Eye, FileCode } from 'lucide-react';
import { markdownToPreviewHtml } from '@/shared/lib/markdownPreview';
import { extractImageFiles, toInlineImages } from '../lib/inlineImages';
import {
  CommandPalette, EditorFooter, ToolBtn, ToolbarDivider, sanitize, usePaletteShortcut,
  type EditorProps,
} from './editorParts';

type ViewMode = 'write' | 'preview';

const PALETTE_ITEMS = [
  'Invia', 'Salva bozza', 'Allega file',
  'Inserisci tabella', 'Inserisci blocco codice',
] as const;

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

type Wrap = [prefix: string, suffix: string, placeholder: string];
const WRAPS = {
  bold: ['**', '**', 'grassetto'],
  italic: ['*', '*', 'corsivo'],
  code: ['`', '`', 'codice'],
  link: ['[', '](url)', 'testo'],
  list: ['- ', '', 'elemento'],
  hr: ['\n---\n', '', ''],
} satisfies Record<string, Wrap>;

function wrapSelection(view: EditorView, prefix: string, suffix = '', placeholder = 'testo') {
  const { from, to } = view.state.selection.main;
  const selected = from === to ? placeholder : view.state.sliceDoc(from, to);
  view.dispatch({
    changes: { from, to, insert: prefix + selected + suffix },
    selection: { anchor: from + prefix.length + selected.length + suffix.length },
  });
  view.focus();
}

// The shortcuts the toolbar advertises. Highest precedence so they beat the
// default keymap; Mod-Shift-K (palette) is a different chord and not matched.
const shortcut = (key: string, wrap: Wrap) => ({
  key,
  run: (view: EditorView) => (wrapSelection(view, ...wrap), true),
});

const EXTENSIONS = [
  Prec.highest(keymap.of([
    shortcut('Mod-b', WRAPS.bold),
    shortcut('Mod-i', WRAPS.italic),
    shortcut('Mod-e', WRAPS.code),
    shortcut('Mod-k', WRAPS.link),
  ])),
  markdown(),
  mailCatTheme,
  fileDrops,
];

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
          <ToolbarDivider />
        </>
      )}

      <ToolBtn icon={<Paperclip size={14} />} label="Allega file" onClick={onAttach} />
      <ToolBtn icon={<CommandIcon size={14} />} label="Command palette (Ctrl+Shift+K)"
        onClick={onOpenPalette} />
    </div>
  );
}

// ── Preview ───────────────────────────────────────────────────────────────────

function PreviewPane({ markdown: md, quotedHtml, signatureHtml }: {
  markdown: string;
  quotedHtml: string | null;
  signatureHtml: string | null;
}) {
  const [html, setHtml] = useState('');
  useEffect(() => {
    let stale = false;
    let fullMd = md;
    // btoa only takes Latin-1: go through UTF-8 bytes first.
    const b64 = (h: string) => btoa(String.fromCharCode(...new TextEncoder().encode(h)));
    if (signatureHtml) fullMd += `\n<!-- signature-block: ${b64(signatureHtml)} -->`;
    if (quotedHtml) fullMd += `\n<!-- quoted-block: ${b64(quotedHtml)} -->`;
    void markdownToPreviewHtml(fullMd).then((out) => {
      if (!stale) setHtml(sanitize(out));
    });
    return () => { stale = true; };
  }, [md, quotedHtml, signatureHtml]);
  return (
    <div
      className="mc-prose flex-1 overflow-y-auto px-6 py-5 text-sm text-foreground"
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}

// ── MarkdownEditor (root) ─────────────────────────────────────────────────────

export function MarkdownEditor({
  initialBody, onBodyChange, onImageAdd, onSend, onSaveDraft, onAttach, onError,
  signatureHtml, quotedHtml, autoFocus,
}: EditorProps) {
  const [viewMode, setViewMode] = useState<ViewMode>('write');
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [currentMd, setCurrentMd] = useState(initialBody);
  // The latest text, readable between renders: images are appended in a loop.
  const mdRef = useRef(initialBody);
  const cmRef = useRef<ReactCodeMirrorRef>(null);

  const setMd = useCallback((md: string) => {
    mdRef.current = md;
    setCurrentMd(md);
    onBodyChange(md);
  }, [onBodyChange]);

  const insert = (...args: Wrap) => {
    const view = cmRef.current?.view;
    if (view) wrapSelection(view, ...args);
  };
  const actions = {
    bold: () => insert(...WRAPS.bold),
    italic: () => insert(...WRAPS.italic),
    code: () => insert(...WRAPS.code),
    link: () => insert(...WRAPS.link),
    list: () => insert(...WRAPS.list),
    hr: () => insert(...WRAPS.hr),
  };

  // Send and save shortcuts live in ComposeWindow: they must work before this
  // lazy chunk has loaded and outside the editor too.
  usePaletteShortcut(() => setPaletteOpen((p) => !p));

  const addImages = (files: File[]) => {
    for (const image of toInlineImages(files, onError)) {
      onImageAdd(image);
      setMd(`${mdRef.current}\n![${image.blob instanceof File ? image.blob.name : ''}](${image.url})\n`);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    const files = extractImageFiles(e.dataTransfer);
    if (!files.length) return;
    e.preventDefault();
    addImages(files);
  };

  const handlePaletteSelect = (item: string) => {
    setPaletteOpen(false);
    if (item === 'Invia') onSend();
    else if (item === 'Salva bozza') onSaveDraft();
    else if (item === 'Allega file') onAttach();
    else if (item === 'Inserisci blocco codice') insert('```\n', '\n```', 'codice');
    else if (item === 'Inserisci tabella') {
      insert('\n| Colonna 1 | Colonna 2 |\n|-----------|----------|\n| cella     | cella     |\n', '', '');
    }
    requestAnimationFrame(() => cmRef.current?.view?.focus());
  };

  return (
    <div
      className="relative flex flex-1 flex-col overflow-hidden"
      onDrop={handleDrop}
      onDragOver={(e) => e.preventDefault()}
    >
      <Toolbar
        viewMode={viewMode} onViewMode={setViewMode}
        onAttach={onAttach} onOpenPalette={() => setPaletteOpen((p) => !p)}
        onBold={actions.bold} onItalic={actions.italic} onCode={actions.code}
        onLink={actions.link} onList={actions.list} onHr={actions.hr}
      />

      {paletteOpen && (
        <CommandPalette
          items={PALETTE_ITEMS}
          onClose={() => { setPaletteOpen(false); cmRef.current?.view?.focus(); }}
          onSelect={handlePaletteSelect}
        />
      )}

      {viewMode === 'write' ? (
        <div className="flex flex-1 flex-col overflow-hidden">
          <div className="min-h-0 flex-1 overflow-hidden">
            <CodeMirror
              ref={cmRef}
              value={currentMd}
              onChange={setMd}
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
          <EditorFooter signatureHtml={signatureHtml} quotedHtml={quotedHtml} />
        </div>
      ) : (
        <PreviewPane markdown={currentMd} quotedHtml={quotedHtml} signatureHtml={signatureHtml} />
      )}
    </div>
  );
}
