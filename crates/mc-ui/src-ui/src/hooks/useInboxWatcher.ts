import { useEffect } from 'react'
import { listen } from '@tauri-apps/api/event'
import { invoke } from '@tauri-apps/api/core'
import { useQueryClient } from '@tanstack/react-query'
import type { MailboxEventPayload } from './useMailboxWatcher'
import type { Mailbox } from '@/lib/types'

/**
 * Permanently watches the INBOX of an account so that unread badges in the
 * sidebar stay up to date even when the user is viewing another folder.
 *
 * Works alongside `useMailboxWatcher` in MessageList: the WatchState on the
 * Rust side is ref-counted, so both hooks can call start/stop_watching for the
 * same mailbox_id independently without interfering.
 *
 * On any event for the INBOX, the mailboxes list for the account is invalidated
 * so FolderItem re-renders with the latest unread count.
 */
export function useInboxWatcher(accountId: string | null, inbox: Mailbox | null) {
  const queryClient = useQueryClient()

  useEffect(() => {
    if (!accountId || !inbox) return

    const mailboxId = inbox.id

    invoke('start_watching', { mailboxId }).catch((e) =>
      console.error('[inbox-watcher] start_watching failed:', e),
    )

    let unlisten: (() => void) | null = null
    listen<MailboxEventPayload>('mailbox:event', (event) => {
      const payload = event.payload
      if (payload.mailbox_id !== mailboxId) return
      queryClient.invalidateQueries({ queryKey: ['mailboxes', accountId] })
    }).then((fn) => {
      unlisten = fn
    })

    return () => {
      invoke('stop_watching', { mailboxId }).catch(() => {})
      unlisten?.()
    }
  }, [accountId, inbox?.id, queryClient])
}
