import { useQuery } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { MessageEnvelope, MessageBody, SearchHit, Suggestion } from '@/lib/types';

// Raw DTO shape returned by the Rust command
interface MessageSummaryDto {
  id: string;
  mailbox_id: string;
  subject: string;
  from_name: string | null;
  from_email: string;
  to: string[];
  date: string;
  is_read: boolean;
  has_attachments: boolean;
}

interface MessageBodyDto {
  id: string;
  html_body: string | null;
  text_body: string | null;
}

async function fetchEnvelopes(accountId: string, mailboxId: string): Promise<MessageEnvelope[]> {
  const dtos = await invoke<MessageSummaryDto[]>('list_messages', { accountId, mailboxId });
  return dtos.map((d) => ({
    uid: d.id,
    subject: d.subject,
    fromName: d.from_name ?? d.from_email,
    fromEmail: d.from_email,
    toAddresses: d.to,
    date: d.date,
    isRead: d.is_read,
    hasAttachment: d.has_attachments,
    preview: '',
  }));
}

async function fetchBody(messageId: string): Promise<MessageBody> {
  const dto = await invoke<MessageBodyDto>('get_message', { messageId });
  return {
    uid: dto.id,
    htmlBody: dto.html_body,
    textBody: dto.text_body,
  };
}

/**
 * mailboxKey format: "accountId::mailboxId"
 * mailboxId is the server-assigned UUID, not the path.
 */
export function useMessages(mailboxKey: string | null) {
  const parts = mailboxKey?.split('::') ?? [];
  const [accountId, mailboxId] = parts;

  return useQuery({
    queryKey: ['messages', mailboxKey],
    queryFn: () => fetchEnvelopes(accountId, mailboxId),
    enabled: parts.length === 2,
    staleTime: 15_000,
  });
}

export function useMessageBody(uid: string | null) {
  return useQuery({
    queryKey: ['body', uid],
    queryFn: () => fetchBody(uid!),
    enabled: uid !== null,
    staleTime: Infinity,
  });
}

// ── Search ──────────────────────────────────────────────────────────────────

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
