import { useMutation, useQueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';

interface SendParams {
  accountId: string;
  fromEmail: string;
  to: string[];
  cc?: string[];
  bcc?: string[];
  subject: string;
  bodyText?: string;
  bodyHtml?: string;
  inReplyTo?: string;
  references?: string[];
}

export function useSendMessage() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: SendParams) =>
      invoke<string>('send_message', {
        accountId: params.accountId,
        fromEmail: params.fromEmail,
        to: params.to,
        cc: params.cc ?? [],
        bcc: params.bcc ?? [],
        subject: params.subject,
        bodyText: params.bodyText ?? null,
        bodyHtml: params.bodyHtml ?? null,
        inReplyTo: params.inReplyTo ?? null,
        references: params.references ?? [],
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['thread'] });
      qc.invalidateQueries({ queryKey: ['conversations'] });
      qc.invalidateQueries({ queryKey: ['messages'] });
    },
  });
}
