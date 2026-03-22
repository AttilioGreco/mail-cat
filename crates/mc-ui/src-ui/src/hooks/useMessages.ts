import { useQuery } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { MessageEnvelope, MessageBody } from '@/lib/types';

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
