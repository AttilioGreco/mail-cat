import { useQuery } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { SearchHit, Suggestion } from './types';

interface SearchHitDto {
  message_id: string;
  score: number;
  snippet: string | null;
  subject: string;
  from_name: string | null;
  from_email: string;
  date: number;
}

async function fetchSearchResults(
  query: string,
  accountId?: string | null,
  mailboxId?: string | null,
): Promise<SearchHit[]> {
  const dtos = await invoke<SearchHitDto[]>('search_messages', {
    query,
    accountId: accountId ?? null,
    mailboxId: mailboxId ?? null,
  });
  return dtos.map((d) => ({
    messageId: d.message_id,
    score: d.score,
    snippet: d.snippet,
    subject: d.subject,
    fromName: d.from_name,
    fromEmail: d.from_email,
    date: d.date,
  }));
}

export function useSearchMessages(query: string | null, accountId?: string | null) {
  return useQuery({
    queryKey: ['search', query, accountId],
    queryFn: () => fetchSearchResults(query!, accountId),
    enabled: query !== null && query.trim().length > 0,
    staleTime: 30_000,
  });
}

// ── Suggest (autocomplete) ───────────────────────────────────────────────────

interface SuggestionDto {
  field: string;
  value: string;
  doc_count: number;
}

async function fetchSuggestions(
  prefix: string,
  scope?: string | null,
): Promise<Suggestion[]> {
  const dtos = await invoke<SuggestionDto[]>('suggest_messages', {
    prefix,
    scope: scope ?? null,
    maxPerField: 5,
  });
  return dtos.map((d) => ({
    field: d.field,
    value: d.value,
    docCount: d.doc_count,
  }));
}

export function useSuggestions(prefix: string | null, scope?: string | null) {
  return useQuery({
    queryKey: ['suggest', prefix, scope],
    queryFn: () => fetchSuggestions(prefix!, scope),
    enabled: prefix !== null && prefix.trim().length >= 2,
    staleTime: 60_000,
  });
}
