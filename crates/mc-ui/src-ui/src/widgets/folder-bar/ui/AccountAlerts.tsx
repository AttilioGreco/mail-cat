import { useState } from 'react';
import { RefreshCw, KeyRound } from 'lucide-react';
import { authLabel, authState, useSecondsUntil, type Account } from '@/entities/account';
import { useSettingsStore } from '@/features/manage-settings';
import { useOAuthLogin, useRestartServer } from '@/features/manage-accounts';

/** Everything that stops mail for an account, with its fix. */
export function AccountAlerts({ account }: { account: Account }) {
  return (
    <>
      <AuthBanner account={account} />
      {account.auth?.taskError && (
        <p className="mx-2 mb-1 truncate text-[10px] text-destructive" title={account.auth.taskError}>
          Un'attività dell'account si è fermata: riavvialo da Impostazioni
        </p>
      )}
      {account.auth?.smtpError && (
        <p className="mx-2 mb-1 truncate text-[10px] text-amber-500" title={account.auth.smtpError}>
          Invio bloccato: il server SMTP rifiuta l'accesso
        </p>
      )}
    </>
  );
}

function AuthBanner({ account }: { account: Account }) {
  const state = authState(account);
  const secondsLeft = useSecondsUntil(state === 'offline' ? account.auth?.nextRetry : null);
  const openSettings = useSettingsStore((s) => s.openSettings);
  const restartServer = useRestartServer();
  const label = authLabel(account, secondsLeft);
  const detail = account.auth?.lastError ?? undefined;

  switch (state) {
    case 'offline':
      return (
        <p className="mx-2 mb-1 truncate text-[10px] text-muted" title={detail}>
          {label}
        </p>
      );
    case 'needs_reauth':
      // Only an OAuth account can be fixed by logging in again. Without the
      // live status the auth type is unknown, so say nothing about it.
      if (!account.auth) {
        return <AuthAlert label="Accesso richiesto" action="Apri impostazioni" onAction={openSettings} />;
      }
      return account.auth.oauthProvider ? (
        <ReauthBanner accountId={account.id} label={label} />
      ) : (
        <AuthAlert label="Password rifiutata" detail={detail} action="Aggiorna password" onAction={openSettings} />
      );
    case 'credentials_rejected':
      return <AuthAlert label={label} detail={detail} action="Aggiorna password" onAction={openSettings} />;
    case 'oauth_client_invalid':
      return (
        <AuthAlert
          label={label}
          detail={detail}
          action="Riavvia il server"
          busy={restartServer.isPending}
          onAction={() => restartServer.mutate()}
        />
      );
    default:
      return null;
  }
}

function AuthAlert({
  label,
  detail,
  action,
  busy = false,
  onAction,
}: {
  label: string;
  detail?: string;
  action: string;
  busy?: boolean;
  onAction: () => void;
}) {
  return (
    <div className="mx-0.5 mb-1 rounded-md border border-amber-500/40 bg-amber-500/5 px-2 py-2">
      <div className="flex items-center gap-2 text-[11px] text-amber-500" title={detail}>
        <KeyRound size={11} className="shrink-0" />
        <span>{label}</span>
      </div>
      <button
        type="button"
        disabled={busy}
        onClick={onAction}
        className="mt-2 flex w-full items-center justify-center gap-2 rounded border border-amber-500/40 px-2 py-1 text-[11px] font-medium text-amber-500 transition-colors hover:bg-amber-500/10 disabled:opacity-60"
      >
        {busy && <RefreshCw size={10} className="animate-spin" />}
        {action}
      </button>
    </div>
  );
}

function ReauthBanner({ accountId, label }: { accountId: string; label: string }) {
  const { reauth, event, pending } = useOAuthLogin();
  // The oauth:event stream is global; this keeps a second stale account's banner
  // from mirroring the progress of the one actually being re-authenticated.
  const [started, setStarted] = useState(false);
  const busy = started && pending;

  return (
    <div className="mx-0.5 mb-1 rounded-md border border-amber-500/40 bg-amber-500/5 px-2 py-2">
      <div className="flex items-center gap-2 text-[11px] text-amber-500">
        <KeyRound size={11} className="shrink-0" />
        <span>{label}</span>
      </div>
      <button
        type="button"
        disabled={busy}
        onClick={() => {
          setStarted(true);
          reauth(accountId);
        }}
        className="mt-2 flex w-full items-center justify-center gap-2 rounded border border-amber-500/40 px-2 py-1 text-[11px] font-medium text-amber-500 transition-colors hover:bg-amber-500/10 disabled:opacity-60"
      >
        {busy && <RefreshCw size={10} className="animate-spin" />}
        {busy ? 'Apri il browser…' : 'Ri-autentica'}
      </button>
      {started && event?.kind === 'failed' && (
        <p className="mt-1 text-[10px] text-destructive">{event.message}</p>
      )}
    </div>
  );
}
