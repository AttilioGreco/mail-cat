import { useRef, useState } from 'react';
import { EditorContent, useEditor, useEditorState, type Editor } from '@tiptap/react';
import type { EditorView } from '@tiptap/pm/view';
import StarterKit from '@tiptap/starter-kit';
import Image from '@tiptap/extension-image';
import { Table, TableRow, TableHeader, TableCell } from '@tiptap/extension-table';
import { TextStyle, Color } from '@tiptap/extension-text-style';
import { Placeholder } from '@tiptap/extensions';
import {
  Bold, Italic, Underline, List, ListOrdered, Quote, Link, ImagePlus, Table2,
  Paperclip, CommandIcon,
} from 'lucide-react';
import { extractImageFiles, toInlineImages } from '../lib/inlineImages';
import {
  CommandPalette, EditorFooter, ToolBtn, ToolbarDivider, sanitize, usePaletteShortcut,
  type EditorProps,
} from './editorParts';

const PALETTE_ITEMS = ['Invia', 'Salva bozza', 'Allega file', 'Inserisci tabella', 'Inserisci immagine'] as const;

// Tiptap's schema already drops anything it has no node for (scripts, forms,
// iframes, unknown attributes); DOMPurify on the way in is the second belt.
const EXTENSIONS = [
  StarterKit.configure({ link: { openOnClick: false, autolink: true } }),
  Image.configure({ inline: true }),
  Table,
  TableRow,
  TableHeader,
  TableCell,
  TextStyle,
  Color,
  Placeholder.configure({ placeholder: 'Scrivi il tuo messaggio...' }),
];

function LinkBar({ editor, onClose }: { editor: Editor; onClose: () => void }) {
  const [url, setUrl] = useState(() => (editor.getAttributes('link').href as string | undefined) ?? '');
  const apply = () => {
    const href = url.trim();
    const chain = editor.chain().focus().extendMarkRange('link');
    (href ? chain.setLink({ href }) : chain.unsetLink()).run();
    onClose();
  };
  return (
    <div className="flex shrink-0 items-center gap-2 border-b border-border px-3 py-1.5">
      <input
        autoFocus
        value={url}
        onChange={(e) => setUrl(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter') { e.preventDefault(); apply(); }
          // Stopped here so Escape closes the bar, not the compose window.
          else if (e.key === 'Escape') { e.stopPropagation(); onClose(); editor.commands.focus(); }
        }}
        placeholder="https://… (vuoto per rimuovere il link)"
        className="flex-1 bg-transparent text-sm text-foreground outline-none placeholder:text-muted"
      />
      <button type="button" onClick={apply} className="text-xs text-accent hover:underline">Applica</button>
    </div>
  );
}

export function SimpleEditor({
  initialBody, onBodyChange, onImageAdd, onSend, onSaveDraft, onAttach, onError,
  signatureHtml, quotedHtml, autoFocus,
}: EditorProps) {
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [linkOpen, setLinkOpen] = useState(false);
  const imageInput = useRef<HTMLInputElement>(null);

  const insertImages = (view: EditorView, files: File[]) => {
    for (const image of toInlineImages(files, onError)) {
      onImageAdd(image);
      const node = view.state.schema.nodes.image.create({
        src: image.url,
        alt: image.blob instanceof File ? image.blob.name : '',
      });
      view.dispatch(view.state.tr.replaceSelectionWith(node));
    }
    view.focus();
  };

  const editor = useEditor({
    extensions: EXTENSIONS,
    content: sanitize(initialBody),
    autofocus: autoFocus ? 'end' : false,
    onUpdate: ({ editor }) => onBodyChange(editor.isEmpty ? '' : editor.getHTML()),
    editorProps: {
      attributes: { class: 'mc-prose min-h-full px-6 py-4 text-sm text-foreground outline-none' },
      transformPastedHTML: sanitize,
      handleKeyDown: (_view, e) => {
        if (!(e.metaKey || e.ctrlKey) || e.shiftKey || e.key.toLowerCase() !== 'k') return false;
        e.preventDefault();
        setLinkOpen(true);
        return true;
      },
      handlePaste: (view, e) => {
        const files = e.clipboardData ? extractImageFiles(e.clipboardData) : [];
        if (!files.length) return false;
        insertImages(view, files);
        return true;
      },
      handleDrop: (view, e) => {
        const files = e.dataTransfer ? extractImageFiles(e.dataTransfer) : [];
        if (!files.length) return false;
        // ComposeWindow treats a handled drop as "not an attachment".
        e.preventDefault();
        insertImages(view, files);
        return true;
      },
    },
  });

  const active = useEditorState({
    editor,
    selector: ({ editor: e }) => ({
      bold: e?.isActive('bold') ?? false,
      italic: e?.isActive('italic') ?? false,
      underline: e?.isActive('underline') ?? false,
      bulletList: e?.isActive('bulletList') ?? false,
      orderedList: e?.isActive('orderedList') ?? false,
      blockquote: e?.isActive('blockquote') ?? false,
      link: e?.isActive('link') ?? false,
    }),
  });

  usePaletteShortcut(() => setPaletteOpen((p) => !p));

  const insertTable = () => editor?.chain().focus().insertTable({ rows: 3, cols: 3, withHeaderRow: true }).run();

  const handlePaletteSelect = (item: string) => {
    setPaletteOpen(false);
    if (item === 'Invia') onSend();
    else if (item === 'Salva bozza') onSaveDraft();
    else if (item === 'Allega file') onAttach();
    else if (item === 'Inserisci tabella') insertTable();
    else if (item === 'Inserisci immagine') imageInput.current?.click();
    requestAnimationFrame(() => editor?.commands.focus());
  };

  if (!editor) return null;
  const chain = () => editor.chain().focus();

  return (
    <div className="relative flex flex-1 flex-col overflow-hidden">
      <div className="flex shrink-0 items-center gap-0.5 border-b border-border px-3 py-1.5">
        <ToolBtn icon={<Bold size={14} />} label="Grassetto (Ctrl+B)" active={active.bold}
          onClick={() => chain().toggleBold().run()} />
        <ToolBtn icon={<Italic size={14} />} label="Corsivo (Ctrl+I)" active={active.italic}
          onClick={() => chain().toggleItalic().run()} />
        <ToolBtn icon={<Underline size={14} />} label="Sottolineato (Ctrl+U)" active={active.underline}
          onClick={() => chain().toggleUnderline().run()} />
        <ToolbarDivider />
        <ToolBtn icon={<List size={14} />} label="Elenco puntato" active={active.bulletList}
          onClick={() => chain().toggleBulletList().run()} />
        <ToolBtn icon={<ListOrdered size={14} />} label="Elenco numerato" active={active.orderedList}
          onClick={() => chain().toggleOrderedList().run()} />
        <ToolBtn icon={<Quote size={14} />} label="Citazione" active={active.blockquote}
          onClick={() => chain().toggleBlockquote().run()} />
        <ToolbarDivider />
        <ToolBtn icon={<Link size={14} />} label="Link (Ctrl+K)" active={active.link || linkOpen}
          onClick={() => setLinkOpen((o) => !o)} />
        <ToolBtn icon={<ImagePlus size={14} />} label="Immagine" onClick={() => imageInput.current?.click()} />
        <ToolBtn icon={<Table2 size={14} />} label="Tabella" onClick={insertTable} />
        <ToolbarDivider />
        <ToolBtn icon={<Paperclip size={14} />} label="Allega file" onClick={onAttach} />
        <ToolBtn icon={<CommandIcon size={14} />} label="Command palette (Ctrl+Shift+K)"
          onClick={() => setPaletteOpen((p) => !p)} />
      </div>

      <input
        ref={imageInput}
        type="file"
        accept="image/*"
        multiple
        className="hidden"
        onChange={(e) => {
          insertImages(editor.view, Array.from(e.target.files ?? []));
          e.target.value = '';
        }}
      />

      {linkOpen && <LinkBar editor={editor} onClose={() => setLinkOpen(false)} />}

      {paletteOpen && (
        <CommandPalette
          items={PALETTE_ITEMS}
          onClose={() => { setPaletteOpen(false); editor.commands.focus(); }}
          onSelect={handlePaletteSelect}
        />
      )}

      <div className="min-h-0 flex-1 overflow-y-auto">
        <EditorContent editor={editor} className="min-h-full" />
      </div>
      <EditorFooter signatureHtml={signatureHtml} quotedHtml={quotedHtml} />
    </div>
  );
}
