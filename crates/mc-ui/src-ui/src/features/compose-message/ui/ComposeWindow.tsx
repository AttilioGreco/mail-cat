import { lazy, Suspense, useEffect, useEffectEvent, useRef, useState, type RefObject } from 'react';
import { X, Send, Loader2, Paperclip, Forward } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/shared/ui/button';
import { Dialog, DialogContent, DialogTitle } from '@/shared/ui/dialog';
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent,
  AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from '@/shared/ui/alert-dialog';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/shared/ui/tooltip';
import { useComposeStore, type ComposeRequest } from '../model/compose-store';
import { usePrepareCompose, useSendCompose, useSaveDraft, useDiscardDraft } from '../model/compose-api';
import type { ComposeDto, ComposeInput, ComposeMode, InlineImage } from '../model/composeTypes';
import { ComposeHeader, type PendingRecipients, type Recipients } from './ComposeHeader';
import { isValidAddress, mergeAddresses, splitAddresses } from './RecipientField';

// ponytail: split out, pulls in CodeMirror + remark (~5.7MB source) that's
// dead weight until the user actually opens compose.
const ComposeEditor = lazy(() =>
  import('./ComposeEditor').then((m) => ({ default: m.ComposeEditor })),
);

const TITLES: Record<ComposeMode, string> = {
  new: 'Nuovo messaggio',
  reply: 'Rispondi',
  replyAll: 'Rispondi a tutti',
  forward: 'Inoltra',
  draft: 'Bozza',
};

const AUTOSAVE_MS = 3000;
const NO_PENDING: PendingRecipients = { to: '', cc: '', bcc: '' };
const SIZE_WARNING_BYTES = 25 * 1024 * 1024;

export function ComposeWindow() {
  const request = useComposeStore((s) => s.request);
  const closeCompose = useComposeStore((s) => s.closeCompose);
  // The form owns the dirty state, so it decides what closing means; until it
  // has loaded, closing is unconditional.
  const requestCloseRef = useRef(closeCompose);

  return (
    <Dialog
      open={request !== null}
      onOpenChange={(open) => !open && requestCloseRef.current()}
      disablePointerDismissal
    >
      <DialogContent
        showCloseButton={false}
        className="flex h-[90vh] w-[720px] max-w-[95vw] flex-col gap-0 overflow-hidden bg-background p-0 shadow-2xl sm:max-w-[95vw]"
      >
        {request && <ComposeSession request={request} requestCloseRef={requestCloseRef} />}
      </DialogContent>
    </Dialog>
  );
}

function ComposeSession({ request, requestCloseRef }: {
  request: ComposeRequest;
  requestCloseRef: RefObject<() => void>;
}) {
  const { data, error, isLoading } = usePrepareCompose(request);
  const closeCompose = useComposeStore((s) => s.closeCompose);

  if (data) return <ComposeForm dto={data} requestCloseRef={requestCloseRef} />;
  return (
    <>
      <TitleBar title={TITLES[request.mode]} onClose={closeCompose} />
      <div className="flex flex-1 items-center justify-center gap-2 text-sm text-muted-foreground">
        {isLoading ? <><Loader2 size={14} className="animate-spin" /> Preparazione…</> : (
          <span className="text-destructive">Impossibile preparare il messaggio: {String(error)}</span>
        )}
      </div>
    </>
  );
}

function TitleBar({ title, onClose }: { title: string; onClose: () => void }) {
  return (
    <div className="flex shrink-0 items-center justify-between border-b border-border px-4 py-3">
      <DialogTitle className="text-sm font-semibold text-foreground">{title}</DialogTitle>
      <button
        type="button"
        onClick={onClose}
        title="Chiudi"
        className="rounded-md p-1 text-muted-foreground transition-colors hover:bg-surface-hover hover:text-foreground"
      >
        <X size={16} />
      </button>
    </div>
  );
}

function ComposeForm({ dto, requestCloseRef }: { dto: ComposeDto; requestCloseRef: RefObject<() => void> }) {
  const closeCompose = useComposeStore((s) => s.closeCompose);
  const sendCompose = useSendCompose();
  const saveDraft = useSaveDraft();
  const discardDraft = useDiscardDraft();

  const [accountId, setAccountId] = useState(dto.accountId);
  const [recipients, setRecipients] = useState<Recipients>({ to: dto.to, cc: dto.cc, bcc: dto.bcc });
  const [pending, setPending] = useState(NO_PENDING);
  const [subject, setSubject] = useState(dto.subject);
  // A reopened draft's images: the editor shows them via blob URLs, swapped
  // back to the same cid: on save/send.
  const [initial] = useState(() => restoreInlineImages(dto));
  const [bodyMd, setBodyMd] = useState(initial.md);
  const [images, setImages] = useState(initial.images);
  const [files, setFiles] = useState<File[]>([]);
  const [forwarded, setForwarded] = useState(dto.forwardedAttachments);
  const [confirm, setConfirm] = useState<'close' | 'subject' | null>(null);
  const [saved, setSaved] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);

  // Read when a queued job runs, not when it was queued: a save that started
  // before the previous one returned must still reuse its draft id.
  const draftId = useRef(dto.draftId);
  const reopened = dto.draftId !== null;
  // Set once the message is sent or thrown away, so an autosave timer that
  // fires meanwhile can't resurrect it as a draft.
  const finished = useRef(false);
  // Saves, sends and discards run one at a time, so two autosaves never race
  // into two drafts and a send always carries the latest draft id.
  const chain = useRef<Promise<unknown>>(Promise.resolve());
  const queue = <T,>(job: () => Promise<T>): Promise<T> => {
    const run = chain.current.then(job);
    chain.current = run.catch(() => {});
    return run;
  };

  const { to, cc, bcc } = recipients;
  const hasInvalid = [...to, ...cc, ...bcc].some((a) => !isValidAddress(a));
  const hasPending = Object.values(pending).some((text) => text.trim() !== '');
  const changed =
    accountId !== dto.accountId ||
    !sameList(to, dto.to) || !sameList(cc, dto.cc) || !sameList(bcc, dto.bcc) ||
    subject !== dto.subject || bodyMd !== initial.md ||
    files.length > 0 || forwarded.length !== dto.forwardedAttachments.length;
  const dirty = changed || hasPending;

  // Shortcuts fire before a recipient field sees the key, so text still being
  // typed there is turned into chips here, exactly as Enter would.
  const commitPending = (): Recipients => {
    if (!hasPending) return recipients;
    const next = {
      to: mergeAddresses(to, splitAddresses(pending.to)),
      cc: mergeAddresses(cc, splitAddresses(pending.cc)),
      bcc: mergeAddresses(bcc, splitAddresses(pending.bcc)),
    };
    setRecipients(next);
    setPending(NO_PENDING);
    return next;
  };

  const identity = dto.identities.find((i) => i.accountId === accountId);
  const signatureHtml = accountId === dto.accountId ? dto.signatureHtml : (identity?.signatureHtml ?? null);

  const buildInput = async (r: Recipients): Promise<ComposeInput> => {
    const used = images.filter((i) => bodyMd.includes(i.url));
    const md = used.reduce((acc, i) => acc.split(i.url).join(`cid:${i.cid}`), bodyMd);
    const attachments = await Promise.all([
      ...files.map(async (f) => ({
        filename: f.name,
        mimeType: f.type || 'application/octet-stream',
        contentId: null,
        dataBase64: await toBase64(f),
      })),
      ...used.map(async (i) => ({
        filename: i.blob instanceof File ? i.blob.name : i.cid,
        mimeType: i.mimeType,
        contentId: i.cid,
        dataBase64: await toBase64(i.blob),
      })),
    ]);
    return {
      draftId: draftId.current,
      accountId, ...r, subject,
      bodyMd: md,
      mode: dto.mode,
      sourceMessageId: dto.sourceMessageId,
      attachments,
      forwardedAttachmentIds: forwarded.map((a) => a.id),
    };
  };

  // Autosave leaves text still being typed in a recipient field alone: it is
  // usually half an address, and committing it would turn it into a chip
  // under the user's fingers.
  const save = (r = recipients) => queue(async () => {
    if (finished.current) return;
    draftId.current = await saveDraft.mutateAsync(await buildInput(r));
    setSaved(true);
  });

  // The reopened draft exactly as it was stored when this window opened.
  const originalInput = (): ComposeInput => ({
    draftId: draftId.current,
    accountId: dto.accountId,
    to: dto.to, cc: dto.cc, bcc: dto.bcc,
    subject: dto.subject,
    bodyMd: dto.bodyMd,
    mode: dto.mode,
    sourceMessageId: dto.sourceMessageId,
    attachments: dto.inlineImages
      .filter((i) => dto.bodyMd.includes(`cid:${i.contentId}`))
      .map((i) => ({ filename: i.contentId, mimeType: i.mimeType, contentId: i.contentId, dataBase64: i.dataBase64 })),
    forwardedAttachmentIds: dto.forwardedAttachments.map((a) => a.id),
  });

  // Throwing the changes away: a draft this window created goes; a reopened
  // one is never lost, so if an autosave already replaced it, it is saved
  // back as it was opened.
  const discard = () => queue(async () => {
    if (!draftId.current || draftId.current === dto.draftId) return;
    if (reopened) draftId.current = await saveDraft.mutateAsync(originalInput());
    else await discardDraft.mutateAsync(draftId.current);
  });

  const discardAndClose = () => {
    setConfirm(null);
    finished.current = true;
    void discard().catch((err) => toast.error(
      `${reopened ? 'Ripristino bozza non riuscito' : 'Eliminazione bozza non riuscita'}: ${String(err)}`,
    ));
    closeCompose();
  };

  // Not dirty means what is on screen is what the window opened with, so
  // undoing back to it is the same as "Non salvare".
  const close = () => (dirty ? setConfirm('close') : discardAndClose());

  useEffect(() => {
    requestCloseRef.current = close;
  });
  useEffect(() => () => {
    requestCloseRef.current = closeCompose;
  }, [requestCloseRef, closeCompose]);

  // Pending text counts as a recipient here; it is validated when it is
  // committed, on send.
  const canSend = (to.length + cc.length + bcc.length > 0 || hasPending) && !hasInvalid && !sendCompose.isPending;

  const send = async (skipSubjectCheck = false) => {
    if (sendCompose.isPending) return;
    const r = commitPending();
    const all = [...r.to, ...r.cc, ...r.bcc];
    if (all.some((a) => !isValidAddress(a))) {
      toast.error('Correggi gli indirizzi non validi prima di inviare.');
      return;
    }
    if (all.length === 0) {
      toast.error('Aggiungi almeno un destinatario.');
      return;
    }
    if (!skipSubjectCheck && !subject.trim()) return setConfirm('subject');
    // Also guards a double Cmd+Enter: isPending only flips once encoding is done.
    if (finished.current) return;
    // Saves still queued are skipped, which is fine: the send carries the
    // latest content and the id of the last draft that did get saved.
    finished.current = true;
    try {
      await queue(async () => sendCompose.mutateAsync(await buildInput(r)));
      toast.success('Messaggio inviato');
      closeCompose();
    } catch (err) {
      finished.current = false;
      toast.error(`Invio non riuscito: ${String(err)}`);
    }
  };

  const saveNow = async () => {
    try {
      await save(commitPending());
      toast.success('Bozza salvata');
    } catch (err) {
      toast.error(`Salvataggio bozza non riuscito: ${String(err)}`);
    }
  };

  const saveAndClose = async () => {
    setConfirm(null);
    try {
      await save(commitPending());
      closeCompose();
    } catch (err) {
      toast.error(`Salvataggio bozza non riuscito: ${String(err)}`);
    }
  };

  const autosave = useEffectEvent(() => {
    save().catch((err) => toast.error(`Salvataggio automatico non riuscito: ${String(err)}`));
  });
  useEffect(() => {
    if (!changed) return;
    const timer = setTimeout(autosave, AUTOSAVE_MS);
    return () => clearTimeout(timer);
  }, [changed, accountId, recipients, subject, bodyMd, images, files, forwarded]);

  const onShortcut = useEffectEvent((e: KeyboardEvent) => {
    if (!(e.metaKey || e.ctrlKey) || confirm) return;
    const key = e.key.toLowerCase();
    const isSend = key === 'enter' || (e.shiftKey && key === 'd');
    const isSave = key === 's' && !e.shiftKey;
    if (!isSend && !isSave) return;
    // Stopped in the capture phase so global hotkeys bound to the same chord
    // (Outlook's Mod+Shift+D = go to Trash) don't fire behind the window.
    e.preventDefault();
    e.stopPropagation();
    if (isSend) void send();
    else void saveNow();
  });
  useEffect(() => {
    const handler = (e: KeyboardEvent) => onShortcut(e);
    window.addEventListener('keydown', handler, true);
    return () => window.removeEventListener('keydown', handler, true);
  }, []);

  const addFiles = (added: File[]) => {
    if (added.length === 0) return;
    const total = [...files, ...added, ...images.map((i) => i.blob)].reduce((n, f) => n + f.size, 0)
      + forwarded.reduce((n, a) => n + a.size, 0);
    if (total > SIZE_WARNING_BYTES) {
      toast.warning(`Allegati per ${formatSize(total)}: molti server rifiutano messaggi oltre 25 MB.`);
    }
    setFiles((prev) => [...prev, ...added]);
  };

  const replying = dto.mode === 'reply' || dto.mode === 'replyAll';

  return (
    <div
      className="flex min-h-0 flex-1 flex-col"
      onDragOver={(e) => e.preventDefault()}
      onDrop={(e) => {
        // The editor already took images dropped on it as inline images.
        if (e.defaultPrevented) return;
        e.preventDefault();
        addFiles(Array.from(e.dataTransfer.files));
      }}
    >
      <TitleBar title={TITLES[dto.mode]} onClose={close} />

      <input
        ref={fileInput}
        type="file"
        multiple
        className="hidden"
        onChange={(e) => {
          addFiles(Array.from(e.target.files ?? []));
          e.target.value = ''; // so the same file can be picked again
        }}
      />

      <ComposeHeader
        identities={dto.identities}
        accountId={accountId}
        onAccountChange={setAccountId}
        recipients={recipients}
        onRecipientsChange={setRecipients}
        pending={pending}
        onPendingChange={setPending}
        subject={subject}
        onSubjectChange={setSubject}
        focusRecipients={!replying}
      />

      <Suspense fallback={<div className="flex flex-1 items-center justify-center text-sm text-muted">Caricamento editor…</div>}>
        <ComposeEditor
          initialMd={initial.md}
          onMdChange={setBodyMd}
          onImageAdd={(image) => setImages((prev) => [...prev, image])}
          onSend={() => void send()}
          onSaveDraft={() => void saveNow()}
          onAttach={() => fileInput.current?.click()}
          onError={(msg) => toast.error(msg)}
          signatureHtml={signatureHtml}
          quotedHtml={dto.quotedHtml}
          autoFocus={replying}
        />
      </Suspense>

      {(files.length > 0 || forwarded.length > 0) && (
        <div className="flex shrink-0 flex-wrap gap-1.5 border-t border-border px-4 py-2">
          {forwarded.map((a) => (
            <AttachmentChip
              key={a.id}
              icon={<Forward size={11} />}
              name={a.filename}
              size={a.size}
              onRemove={() => setForwarded((prev) => prev.filter((x) => x.id !== a.id))}
            />
          ))}
          {files.map((f, idx) => (
            <AttachmentChip
              key={`${f.name}-${idx}`}
              icon={<Paperclip size={11} />}
              name={f.name}
              size={f.size}
              onRemove={() => setFiles((prev) => prev.filter((_, i) => i !== idx))}
            />
          ))}
        </div>
      )}

      <div className="flex shrink-0 items-center justify-between border-t border-border px-4 py-2.5">
        <span className="text-xs text-muted">
          {saveDraft.isPending ? 'Salvataggio bozza…' : saved ? 'Bozza salvata' : ''}
        </span>
        <Tooltip disabled={!hasInvalid}>
          <TooltipTrigger render={<span />}>
            <Button onClick={() => void send()} disabled={!canSend} className="gap-2 px-4">
              {sendCompose.isPending ? <Loader2 size={14} className="animate-spin" /> : <Send size={14} />}
              Invia
            </Button>
          </TooltipTrigger>
          <TooltipContent>Alcuni indirizzi non sono validi</TooltipContent>
        </Tooltip>
      </div>

      <AlertDialog open={confirm === 'close'} onOpenChange={(open) => !open && setConfirm(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Salvare la bozza?</AlertDialogTitle>
            <AlertDialogDescription>
              Se non la salvi, il messaggio andrà perso.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Annulla</AlertDialogCancel>
            <AlertDialogAction variant="destructive" onClick={discardAndClose}>Non salvare</AlertDialogAction>
            <AlertDialogAction onClick={() => void saveAndClose()}>Salva</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={confirm === 'subject'} onOpenChange={(open) => !open && setConfirm(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Inviare senza oggetto?</AlertDialogTitle>
            <AlertDialogDescription>Il messaggio non ha un oggetto.</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Annulla</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                setConfirm(null);
                void send(true);
              }}
            >
              Invia comunque
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

function AttachmentChip({ icon, name, size, onRemove }: {
  icon: React.ReactNode;
  name: string;
  size: number;
  onRemove: () => void;
}) {
  return (
    <div className="flex items-center gap-1.5 rounded border border-border bg-surface px-2 py-1 text-xs text-foreground">
      <span className="shrink-0 text-muted">{icon}</span>
      <span className="max-w-[160px] truncate" title={name}>{name}</span>
      <span className="text-muted">{formatSize(size)}</span>
      <button type="button" onClick={onRemove} title="Rimuovi" className="text-muted hover:text-foreground">
        <X size={11} />
      </button>
    </div>
  );
}

function restoreInlineImages(dto: ComposeDto): { md: string; images: InlineImage[] } {
  const images = dto.inlineImages.map(({ contentId, mimeType, dataBase64 }) => {
    const bytes = Uint8Array.from(atob(dataBase64), (c) => c.charCodeAt(0));
    const blob = new Blob([bytes], { type: mimeType });
    return { cid: contentId, blob, mimeType, url: URL.createObjectURL(blob) };
  });
  const md = images.reduce((acc, i) => acc.split(`cid:${i.cid}`).join(i.url), dto.bodyMd);
  return { md, images };
}

const sameList = (a: string[], b: string[]) => a.length === b.length && a.every((v, i) => v === b[i]);

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

async function toBase64(blob: Blob): Promise<string> {
  const dataUrl = await new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result as string);
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(blob);
  });
  return dataUrl.slice(dataUrl.indexOf(',') + 1);
}
