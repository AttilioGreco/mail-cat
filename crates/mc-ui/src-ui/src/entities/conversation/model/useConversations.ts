import { invoke } from '@tauri-apps/api/core';
import { useQuery } from '@tanstack/react-query';
import type { ConversationSummary, ThreadMessage } from './types';

function toConversation(raw: Record<string, unknown>): ConversationSummary {
  return {
    threadId: raw.thread_id as string,
    accountId: raw.account_id as string,
    subject: raw.subject as string,
    messageCount: raw.message_count as number,
    unreadCount: raw.unread_count as number,
    lastMessageAt: raw.last_message_at as string,
    latestSender: raw.latest_sender as string,
    sampleMessageId: raw.sample_message_id as string,
  };
}

function toThreadMessage(raw: Record<string, unknown>): ThreadMessage {
  return {
    id: raw.id as string,
    rfcMessageId: (raw.rfc_message_id as string | null) ?? null,
    fromName: (raw.from_name as string | null) ?? null,
    fromEmail: raw.from_email as string,
    to: raw.to as string[],
    cc: (raw.cc as string[] | null) ?? [],
    subject: raw.subject as string,
    date: raw.date as string,
    isRead: raw.is_read as boolean,
    mailboxIds: (raw.mailbox_ids as string[] | null) ?? [],
    htmlBody: (raw.html_body as string | null) ?? null,
    textBody: (raw.text_body as string | null) ?? null,
    inReplyTo: (raw.in_reply_to as string | null) ?? null,
    references: (raw.references as string[] | null) ?? [],
  };
}

export function useConversations(accountId: string | null, mailboxId: string | null = null) {
  return useQuery({
    queryKey: ['conversations', accountId, mailboxId],
    queryFn: async () => {
      const raw = await invoke<Record<string, unknown>[]>('list_conversations', {
        accountId,
        mailboxId,
        pageSize: 200,
      });
      return raw.map(toConversation);
    },
    enabled: !!accountId,
    staleTime: 30_000,
  });
}

/** Shared with `fetchQuery` callers that need a thread outside a component. */
export function threadQuery(messageId: string) {
  return {
    queryKey: ['thread', messageId],
    queryFn: async () => {
      const raw = await invoke<Record<string, unknown>[]>('get_thread', {
        messageId,
      });
      return raw.map(toThreadMessage);
    },
    staleTime: 60_000,
  };
}

export function useThread(messageId: string | null) {
  return useQuery({
    ...threadQuery(messageId ?? ''),
    enabled: !!messageId,
  });
}
