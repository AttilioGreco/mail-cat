import { useState } from 'react';
import type { UseMutationResult } from '@tanstack/react-query';
import { RefreshCw, SearchX, Trash2 } from 'lucide-react';
import { useSearchMessages } from '../model/useSearch';
import { useSearchStore } from '../model/search-store';
import { useSessionStore } from '@/entities/session';
import { cn, formatDate, sourceMailbox } from '@/shared/lib/utils';
import type { SearchHit } from '../model/types';

/** `deleteMany` is message-actions' bulk delete, handed in by the widget:
 *  features may not import each other. */
export function SearchResults({ deleteMany }: {
  deleteMany: UseMutationResult<[number, number], Error, { messages: { messageId: string; mailboxId: string }[] }>;
}) {
  const searchQuery = useSearchStore((s) => s.searchQuery);
  const { selectedAccountId, selectedMailboxId, selectedMessageId, selectMessage } = useSessionStore();
  const { data: hits = [], isLoading } = useSearchMessages(searchQuery, selectedAccountId);
  const [outcome, setOutcome] = useState<string | null>(null);

  // Group action over the whole result set: the query *is* the selection.
  // Trash only — a permanent bulk expunge driven by a search query is one
  // mistyped filter away from shredding a mailbox, so it isn't offered here.
  const trashAll = () => {
    const messages = hits.map((h) => ({
      messageId: h.messageId,
      mailboxId: sourceMailbox(h.mailboxIds, selectedMailboxId),
    }));
    if (messages.length === 0) return;
    if (!window.confirm(`Sposti nel cestino ${messages.length} messaggi?`)) return;

    setOutcome(null);
    deleteMany.mutate(
      { messages },
      {
        onSuccess: ([succeeded, failed]) => {
          selectMessage(null);
          // The index can be behind the mailbox, so "all of them" is a claim
          // worth checking rather than assuming.
          setOutcome(
            failed === 0
              ? `${succeeded} messaggi spostati nel cestino`
              : `${succeeded} spostati, ${failed} non trovati (indice non allineato)`,
          );
        },
        onError: (e) => setOutcome(`Errore: ${String(e)}`),
      },
    );
  };

  if (isLoading) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-3 text-muted-foreground">
        <RefreshCw size={20} className="animate-spin text-muted" />
        <span className="text-sm">Searching...</span>
      </div>
    );
  }

  if (hits.length === 0) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-3 text-muted-foreground">
        <SearchX size={32} className="text-muted" />
        <span className="text-sm">No results for &ldquo;{searchQuery}&rdquo;</span>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col overflow-y-auto">
      <div className="flex items-center justify-between border-b border-border px-4 py-2">
        <span className="text-sm font-semibold text-foreground">Search results</span>
        <div className="flex items-center gap-2">
          <button
            onClick={trashAll}
            disabled={deleteMany.isPending}
            title={`Sposta nel cestino tutti i ${hits.length} risultati`}
            className="flex items-center gap-1 rounded px-2 py-0.5 text-xs text-muted transition-colors hover:bg-accent hover:text-destructive disabled:opacity-50"
          >
            <Trash2 size={12} />
            {deleteMany.isPending ? 'Sposto…' : 'Cestina tutti'}
          </button>
          <span className="text-xs text-muted">{hits.length}</span>
        </div>
      </div>
      {outcome && (
        <div className="border-b border-border bg-surface-hover px-4 py-1.5 text-xs text-muted">
          {outcome}
        </div>
      )}
      {hits.map((hit) => (
        <SearchHitRow
          key={hit.messageId}
          hit={hit}
          isSelected={selectedMessageId === hit.messageId}
          onSelect={() => selectMessage(hit.messageId)}
        />
      ))}
    </div>
  );
}

function SearchHitRow({
  hit,
  isSelected,
  onSelect,
}: {
  hit: SearchHit;
  isSelected: boolean;
  onSelect: () => void;
}) {
  const from = hit.fromName ?? hit.fromEmail;
  const dateStr = hit.date ? formatDate(new Date(hit.date * 1000).toISOString()) : '';

  return (
    <button
      onClick={onSelect}
      className={cn(
        'w-full border-b border-border px-4 py-2.5 text-left transition-colors',
        isSelected
          ? 'bg-accent/10 border-l-2 border-l-accent'
          : 'hover:bg-surface-hover',
      )}
    >
      {/* From + Date row */}
      <div className="flex items-baseline justify-between gap-2">
        <span className="truncate text-xs font-medium text-foreground">
          {from}
        </span>
        <span className="shrink-0 text-[10px] text-muted">
          {dateStr}
        </span>
      </div>
      {/* Subject */}
      <div className="mt-0.5 truncate text-xs text-foreground">
        {hit.subject || '(no subject)'}
      </div>
      {/* Snippet from body */}
      {hit.snippet && (
        <p
          className="mt-0.5 text-[11px] leading-relaxed text-muted line-clamp-1 [&_b]:font-bold [&_b]:text-accent"
          dangerouslySetInnerHTML={{ __html: hit.snippet }}
        />
      )}
    </button>
  );
}
