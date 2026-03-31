import { useEffect, useRef, useState } from 'react';
import { X, Send, Loader2 } from 'lucide-react';
import { cn } from '@/lib/utils';
import { ComposeHeader } from './ComposeHeader';
import { ComposeEditor } from './ComposeEditor';
import { ConfirmIdentityDialog } from './ConfirmIdentityDialog';
import { useIdentities, bestIdentityForReply } from '@/hooks/useIdentities';
import { useSendMessage } from '@/hooks/useMutations';
import { markdownToPreviewHtml } from '@/lib/markdownPreview';
import type { ComposeInitialData, HeaderState, Identity } from '@/lib/composeTypes';

interface ComposeWindowProps {
  open: boolean;
  onClose: () => void;
  initialData: ComposeInitialData | null;
}

export function ComposeWindow({ open, onClose, initialData }: ComposeWindowProps) {
  const { identities } = useIdentities();
  const sendMessage = useSendMessage();

  const [header, setHeader] = useState<HeaderState | null>(null);
  const [bodyMd, setBodyMd] = useState('');
  const [attachments, setAttachments] = useState<File[]>([]);
  const [toast, setToast] = useState<{ msg: string; type: 'error' | 'info' } | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // TASK-08: dialog conferma identità per reply
  const [identityDialogOpen, setIdentityDialogOpen] = useState(false);
  const [resolvedIdentity, setResolvedIdentity] = useState<Identity | null>(null);

  const isDirty = bodyMd.trim().length > 0;

  useEffect(() => {
    if (!open || !initialData || identities.length === 0) return;

    const enriched: ComposeInitialData = { ...initialData, identities };
    const best = initialData.from
      ? (identities.find((i) => i.email === initialData.from) ?? bestIdentityForReply(identities, initialData.from))
      : identities[0];

    setResolvedIdentity(best ?? null);

    const exactMatch = initialData.from
      ? identities.some((i) => i.email === initialData.from)
      : true;

    if (initialData.mode === 'reply' && !exactMatch && identities.length > 1) {
      setIdentityDialogOpen(true);
    }

    void enriched;
  }, [open, initialData, identities]);

  const resetState = () => {
    setBodyMd('');
    setHeader(null);

    setAttachments([]);
    setResolvedIdentity(null);
    setIdentityDialogOpen(false);
    setToast(null);
  };

  const handleClose = () => {
    if (isDirty && !window.confirm('Chiudere? Le modifiche non salvate andranno perse.')) return;
    resetState();
    onClose();
  };

  // ── TASK-10: pipeline invio ───────────────────────────────────────────────

  const handleSend = async () => {
    if (!header) return;

    // Validazione pre-invio
    if (header.to.length === 0) {
      setToast({ msg: 'Aggiungi almeno un destinatario.', type: 'error' });
      return;
    }
    if (!header.subject.trim()) {
      if (!window.confirm('Inviare senza oggetto?')) return;
    }
    if (!bodyMd.trim()) {
      if (!window.confirm('Inviare messaggio vuoto?')) return;
    }

    // Assemblaggio HTML
    const bodyHtml = await markdownToPreviewHtml(bodyMd);
    let fullHtml = `<div class="body">${bodyHtml}</div>`;
    if (initialData?.quotedHtml) {
      fullHtml += `<blockquote style="margin:8px 0;padding:4px 12px;border-left:3px solid #ccc;color:#666;font-size:0.9em">${initialData.quotedHtml}</blockquote>`;
    }
    if (resolvedIdentity?.signatureHtml) {
      fullHtml += `<div class="signature" style="margin-top:16px;border-top:1px solid #e0e0e0;padding-top:12px;color:#555;font-size:0.9em">${resolvedIdentity.signatureHtml}</div>`;
    }

    // Plain text: markdown raw + citazione come testo semplice
    let bodyText = bodyMd;
    if (initialData?.quotedHtml) bodyText += '\n\n---\n(citazione originale)';

    try {
      await sendMessage.mutateAsync({
        accountId: header.from.id,
        fromEmail: header.from.email,
        to: header.to,
        cc: header.cc,
        bcc: header.bcc,
        subject: header.subject,
        bodyText,
        bodyHtml: fullHtml,
        inReplyTo: initialData?.inReplyTo,
        references: initialData?.references,
      });
      resetState();
      onClose();
    } catch (err) {
      setToast({ msg: `Errore invio: ${String(err)}`, type: 'error' });
    }
  };

  // ── File picker ───────────────────────────────────────────────────────────

  const handleAttach = () => fileInputRef.current?.click();

  const handleFileSelected = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files ?? []);
    if (files.length === 0) return;
    setAttachments((prev) => [...prev, ...files]);
    e.target.value = ''; // reset so same file can be re-selected
  };

  // ── Shortcut Escape ───────────────────────────────────────────────────────

  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !identityDialogOpen) handleClose();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [open, isDirty, identityDialogOpen]);

  if (!open || !initialData) return null;

  const enrichedData: ComposeInitialData = {
    ...initialData,
    identities,
    from: resolvedIdentity?.email ?? initialData.from,
    accountId: initialData.accountId,
  };

  const signatureHtml = resolvedIdentity?.signatureHtml;
  const isSending = sendMessage.isPending;

  return (
    <>
      {/* Overlay */}
      <div
        className="fixed inset-0 z-40 bg-black/40 backdrop-blur-sm"
        onClick={handleClose}
        aria-hidden
      />

      {/* Hidden file input */}
      <input
        ref={fileInputRef}
        type="file"
        multiple
        className="hidden"
        onChange={handleFileSelected}
      />

      {/* Modale */}
      <div
        role="dialog"
        aria-modal
        aria-label="Nuovo messaggio"
        className={cn(
          'fixed z-50 flex flex-col overflow-hidden rounded-xl border border-border bg-background shadow-2xl',
          'top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2',
          'w-[720px] max-w-[95vw]',
        )}
        style={{ height: '90vh' }}
      >
        {/* Titlebar */}
        <div className="flex items-center justify-between border-b border-border px-4 py-3 shrink-0">
          <span className="text-sm font-semibold text-foreground">
            {initialData.mode === 'reply'
              ? 'Rispondi'
              : initialData.mode === 'forward'
                ? 'Inoltra'
                : 'Nuovo messaggio'}
          </span>
          <button
            onClick={handleClose}
            title="Chiudi"
            className="rounded-md p-1 text-muted-foreground transition-colors hover:bg-surface-hover hover:text-foreground"
          >
            <X size={16} />
          </button>
        </div>

        {/* Corpo */}
        <div className="flex flex-1 flex-col overflow-hidden">
          <ComposeHeader
            initialData={enrichedData}
            onChange={setHeader}
          />
          <ComposeEditor
            initialMd=""
            onMdChange={setBodyMd}
            onImagesChange={() => {/* TASK-10: CID images for MIME */}}
            onSend={handleSend}
            onSaveDraft={() => {/* TASK-09 */}}
            onAttach={handleAttach}
            signatureHtml={signatureHtml}
            quotedHtml={initialData.quotedHtml}
            attachments={attachments}
            onAttachmentRemove={(idx) => setAttachments((prev) => prev.filter((_, i) => i !== idx))}
            onToast={(msg, type) => setToast({ msg, type: type ?? 'info' })}
          />
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between border-t border-border px-4 py-2.5 shrink-0">
          <span className={cn('text-xs', toast
            ? (toast.type === 'error' ? 'text-destructive' : 'text-muted-foreground')
            : 'text-muted')}>
            {toast?.msg ?? (isDirty ? 'Modifiche non salvate' : '')}
          </span>
          <button
            onClick={handleSend}
            disabled={!header || header.to.length === 0 || isSending}
            className="flex items-center gap-2 rounded-lg bg-accent px-4 py-1.5 text-sm font-medium text-accent-foreground transition-colors hover:bg-accent/90 disabled:opacity-50"
          >
            {isSending ? <Loader2 size={14} className="animate-spin" /> : <Send size={14} />}
            Invia
          </button>
        </div>
      </div>

      {/* TASK-08: Dialog conferma identità */}
      {identityDialogOpen && identities.length > 0 && (
        <ConfirmIdentityDialog
          identities={identities}
          suggestedEmail={initialData.from ?? ''}
          onConfirm={(identity) => {
            setResolvedIdentity(identity);
            setIdentityDialogOpen(false);
          }}
          onCancel={() => {
            setIdentityDialogOpen(false);
          }}
        />
      )}
    </>
  );
}
