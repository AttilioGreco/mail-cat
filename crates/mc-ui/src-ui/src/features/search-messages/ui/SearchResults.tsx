import { RefreshCw, SearchX } from 'lucide-react';
import { useSearchMessages } from '../model/useSearch';
import { useSearchStore } from '../model/search-store';
import { useSessionStore } from '@/entities/session';
import { cn, formatDate } from '@/shared/lib/utils';
import type { SearchHit } from '../model/types';

export function SearchResults() {
  const searchQuery = useSearchStore((s) => s.searchQuery);
  const { selectedAccountId, selectedMessageId, selectMessage } = useSessionStore();
  const { data: hits = [], isLoading } = useSearchMessages(searchQuery, selectedAccountId);

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
        <span className="text-xs text-muted">{hits.length}</span>
      </div>
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
