import { useQuery } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { MessageEnvelope, MessageBody } from './types';

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
  is_flagged: boolean;
  has_attachments: boolean;
}

interface MessageBodyDto {
  id: string;
  html_body: string | null;
  text_body: string | null;
  rfc_message_id: string | null;
  in_reply_to: string | null;
  references: string[];
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
    isFlagged: d.is_flagged,
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
    rfcMessageId: dto.rfc_message_id,
    inReplyTo: dto.in_reply_to,
    references: dto.references,
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

const BODY_POLL_MS = 3_000;
/** Fetches of a still-empty body, the first included, before giving up:
 *  about a minute, bounded for a message with no text part at all. */
const BODY_FETCHES = 20;

interface BodyQueryState {
  data?: MessageBody;
  dataUpdateCount: number;
  errorUpdateCount: number;
}

/** Re-ask while the body is missing — empty, or the fetch failed — counting
 *  failed fetches too, so a server that keeps erroring is not polled forever. */
export function bodyPollInterval({ data, dataUpdateCount, errorUpdateCount }: BodyQueryState): number | false {
  const missing = data?.htmlBody == null && data?.textBody == null;
  return missing && dataUpdateCount + errorUpdateCount < BODY_FETCHES ? BODY_POLL_MS : false;
}

export function useMessageBody(uid: string | null) {
  return useQuery({
    queryKey: ['body', uid],
    queryFn: () => fetchBody(uid!),
    enabled: uid !== null,
    staleTime: Infinity,
    // A body not yet downloaded comes back empty when the server's on-open
    // fetch fails; with an infinite staleTime that empty answer would stay
    // cached for good. Re-asking retries the fetch and picks up a body the
    // backfill stored in the meantime.
    refetchInterval: ({ state }) => bodyPollInterval(state),
  });
}
