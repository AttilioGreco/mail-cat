import { useState } from 'react';
import { X, ExternalLink, Loader2, AlertCircle, CheckCircle2 } from 'lucide-react';
import {
  useAddImapAccount,
  useOAuthLogin,
  type OAuthProviderId,
} from '../model/useAccountMutations';

interface AddAccountDialogProps {
  open: boolean;
  onClose: () => void;
  /** Called once an account was added, so the caller can prompt for a restart. */
  onAdded: () => void;
}

type Mode = 'oauth' | 'imap';

export function AddAccountDialog({ open, onClose, onAdded }: AddAccountDialogProps) {
  const [mode, setMode] = useState<Mode>('oauth');

  if (!open) return null;

  return (
    <>
      <div className="fixed inset-0 z-40 bg-black/35" onClick={onClose} aria-hidden />

      <section
        role="dialog"
        aria-modal
        aria-label="Aggiungi account"
        className="fixed left-1/2 top-1/2 z-50 max-h-[90vh] w-[560px] max-w-[95vw] -translate-x-1/2 -translate-y-1/2 overflow-y-auto rounded-xl border border-border bg-background shadow-2xl"
      >
        <header className="flex items-center justify-between border-b border-border px-4 py-3">
          <h2 className="text-sm font-semibold text-foreground">Aggiungi account</h2>
          <button
            type="button"
            onClick={onClose}
            className="rounded-md p-1 text-muted hover:bg-surface-hover hover:text-foreground"
            title="Chiudi"
          >
            <X size={16} />
          </button>
        </header>

        <div className="flex gap-2 border-b border-border px-4 py-3">
          <ModeButton active={mode === 'oauth'} onClick={() => setMode('oauth')}>
            Gmail / Outlook
          </ModeButton>
          <ModeButton active={mode === 'imap'} onClick={() => setMode('imap')}>
            IMAP manuale
          </ModeButton>
        </div>

        <div className="p-4">
          {mode === 'oauth' ? (
            <OAuthForm onAdded={onAdded} />
          ) : (
            <ImapForm onAdded={onAdded} onClose={onClose} />
          )}
        </div>
      </section>
    </>
  );
}

function ModeButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        'rounded border px-3 py-1.5 text-xs transition-colors',
        active
          ? 'border-accent bg-accent/10 text-foreground'
          : 'border-border bg-background text-muted hover:bg-surface-hover hover:text-foreground',
      ].join(' ')}
    >
      {children}
    </button>
  );
}

function OAuthForm({ onAdded }: { onAdded: () => void }) {
  const [email, setEmail] = useState('');
  const [name, setName] = useState('');
  const [provider, setProvider] = useState<OAuthProviderId>('google');
  const { start, event, pending } = useOAuthLogin();

  const succeeded = event?.kind === 'succeeded';
  const failed = event?.kind === 'failed';

  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    start(name.trim() || email.trim(), email.trim(), provider);
  };

  return (
    <form onSubmit={submit} className="space-y-4">
      <p className="text-xs text-muted">
        Il login avviene nel browser. Al termine l'account viene salvato sul server e i token
        finiscono nel keychain di sistema.
      </p>

      <Field label="Provider">
        <select
          value={provider}
          onChange={(e) => setProvider(e.target.value as OAuthProviderId)}
          className="w-full rounded border border-border bg-background px-2.5 py-2 text-sm text-foreground"
        >
          <option value="google">Google (Gmail)</option>
          <option value="microsoft">Microsoft (Outlook)</option>
        </select>
      </Field>

      <Field label="Email">
        <Input type="email" required value={email} onChange={setEmail} placeholder="me@gmail.com" />
      </Field>

      <Field label="Nome visualizzato" hint="opzionale — default: l'email">
        <Input value={name} onChange={setName} placeholder="Lavoro" />
      </Field>

      {event?.kind === 'awaiting_browser' && (
        <Notice icon={<Loader2 size={14} className="animate-spin" />}>
          In attesa del login nel browser…
          {event.authorize_url && (
            <a
              href={event.authorize_url}
              target="_blank"
              rel="noreferrer"
              className="ml-1 inline-flex items-center gap-1 text-accent underline"
            >
              apri il link <ExternalLink size={11} />
            </a>
          )}
        </Notice>
      )}

      {succeeded && (
        <Notice icon={<CheckCircle2 size={14} className="text-accent" />}>
          Account {event?.email} aggiunto. Riavvia il server per attivarlo.
        </Notice>
      )}

      {failed && (
        <Notice icon={<AlertCircle size={14} className="text-destructive" />}>
          {event?.message ?? 'Login fallito'}
        </Notice>
      )}

      {succeeded ? (
        <SubmitButton type="button" onClick={onAdded}>
          Continua
        </SubmitButton>
      ) : (
        <SubmitButton type="submit" disabled={pending || !email.trim()}>
          {pending ? 'Login in corso…' : 'Accedi con il browser'}
        </SubmitButton>
      )}
    </form>
  );
}

function ImapForm({ onAdded, onClose }: { onAdded: () => void; onClose: () => void }) {
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [imapHost, setImapHost] = useState('');
  const [imapPort, setImapPort] = useState('993');
  const [useTls, setUseTls] = useState(true);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [smtpHost, setSmtpHost] = useState('');
  const [smtpPort, setSmtpPort] = useState('587');

  const addAccount = useAddImapAccount();

  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    addAccount.mutate(
      {
        name: name.trim() || email.trim(),
        email: email.trim(),
        imapHost: imapHost.trim(),
        imapPort: Number(imapPort),
        useTls,
        // The username is usually the address itself.
        username: username.trim() || email.trim(),
        password,
        smtpHost: smtpHost.trim() || null,
        smtpPort: smtpHost.trim() ? Number(smtpPort) : null,
      },
      {
        onSuccess: () => {
          onClose();
          onAdded();
        },
      },
    );
  };

  return (
    <form onSubmit={submit} className="space-y-4">
      <p className="text-xs text-muted">
        La password viene salvata nel keychain di sistema del server, non nel file di
        configurazione.
      </p>

      <div className="grid grid-cols-2 gap-3">
        <Field label="Email">
          <Input type="email" required value={email} onChange={setEmail} placeholder="me@example.com" />
        </Field>
        <Field label="Nome visualizzato" hint="opzionale">
          <Input value={name} onChange={setName} placeholder="Lavoro" />
        </Field>
      </div>

      <div className="grid grid-cols-[1fr_100px] gap-3">
        <Field label="Server IMAP">
          <Input required value={imapHost} onChange={setImapHost} placeholder="mail.example.com" />
        </Field>
        <Field label="Porta">
          <Input type="number" required min={1} max={65535} value={imapPort} onChange={setImapPort} />
        </Field>
      </div>

      <label className="flex items-center gap-2 text-xs text-foreground">
        <input type="checkbox" checked={useTls} onChange={(e) => setUseTls(e.target.checked)} />
        Usa TLS (consigliato)
      </label>

      <div className="grid grid-cols-2 gap-3">
        <Field label="Username" hint="opzionale — default: l'email">
          <Input value={username} onChange={setUsername} />
        </Field>
        <Field label="Password">
          <Input type="password" required value={password} onChange={setPassword} />
        </Field>
      </div>

      <div className="grid grid-cols-[1fr_100px] gap-3">
        <Field label="Server SMTP" hint="opzionale — senza, l'invio è disattivato">
          <Input value={smtpHost} onChange={setSmtpHost} placeholder="mail.example.com" />
        </Field>
        <Field label="Porta">
          <Input type="number" min={1} max={65535} value={smtpPort} onChange={setSmtpPort} />
        </Field>
      </div>

      {addAccount.isError && (
        <Notice icon={<AlertCircle size={14} className="text-destructive" />}>
          {String(addAccount.error)}
        </Notice>
      )}

      <SubmitButton type="submit" disabled={addAccount.isPending}>
        {addAccount.isPending ? 'Salvataggio…' : 'Aggiungi account'}
      </SubmitButton>
    </form>
  );
}

// ── Small form primitives (local: only this dialog needs them) ───────────────

function Field({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <label className="block">
      <span className="mb-1 block text-xs font-medium text-foreground">
        {label}
        {hint && <span className="ml-1 font-normal text-muted">({hint})</span>}
      </span>
      {children}
    </label>
  );
}

function Input({
  value,
  onChange,
  ...rest
}: {
  value: string;
  onChange: (v: string) => void;
} & Omit<React.InputHTMLAttributes<HTMLInputElement>, 'value' | 'onChange'>) {
  return (
    <input
      {...rest}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="w-full rounded border border-border bg-background px-2.5 py-2 text-sm text-foreground"
    />
  );
}

function Notice({ icon, children }: { icon: React.ReactNode; children: React.ReactNode }) {
  return (
    <div className="flex items-start gap-2 rounded-lg border border-border bg-surface p-3 text-xs text-foreground">
      <span className="mt-0.5 shrink-0">{icon}</span>
      <span>{children}</span>
    </div>
  );
}

function SubmitButton({
  children,
  ...rest
}: React.ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      {...rest}
      className="w-full rounded-lg bg-accent px-3 py-2 text-sm font-medium text-accent-foreground transition-colors hover:bg-accent/90 disabled:opacity-50"
    >
      {children}
    </button>
  );
}
