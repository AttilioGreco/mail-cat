import { useEffect } from 'react'
import { listen, UnlistenFn } from '@tauri-apps/api/event'
import { invoke } from '@tauri-apps/api/core'
import { useQueryClient } from '@tanstack/react-query'

export interface MailboxEventPayload {
  mailbox_id: string
  kind: 'counts_changed' | 'new_message' | 'message_expunged' | 'flags_changed'
  total?: number
  unseen?: number
  remote_id?: string
}

/**
 * Registers a real-time watch on a mailbox via IMAP IDLE.
 *
 * @param mailboxId  The UUID of the mailbox (used for the backend start_watching command)
 * @param mailboxKey The composite "accountId::mailboxId" key (used for query invalidation)
 * @param accountId  The account UUID (used to invalidate the mailboxes list)
 */
export function useMailboxWatcher(
  mailboxId: string | null,
  mailboxKey: string | null,
  accountId: string | null,
) {
  const queryClient = useQueryClient()

  useEffect(() => {
    if (!mailboxId || !mailboxKey || !accountId) return

    let unlisten: UnlistenFn | null = null

    // Start watching on the backend (idempotent)
    console.log('[watcher] start_watching mailboxId=', mailboxId, 'mailboxKey=', mailboxKey)
    invoke('start_watching', { mailboxId })
      .then(() => console.log('[watcher] start_watching OK'))
      .catch((e) => console.error('[watcher] start_watching failed:', e))

    // Listen for push events from the backend
    listen<MailboxEventPayload>('mailbox:event', (event) => {
      const payload = event.payload
      console.log('[watcher] raw event received', payload)
      if (payload.mailbox_id !== mailboxId) {
        console.log('[watcher] ignoring event for different mailbox', payload.mailbox_id, '!==', mailboxId)
        return
      }
      console.log('[watcher] processing event kind=', payload.kind)

      // Refresh both list shapes: the flat message list and the conversation
      // list are two views of the same mailbox, and only one of them is mounted
      // at a time. Invalidating just ['messages'] left conversation mode frozen
      // until the client was restarted.
      queryClient.invalidateQueries({ queryKey: ['messages', mailboxKey] })
      queryClient.invalidateQueries({ queryKey: ['conversations', accountId] })
      if (payload.kind === 'counts_changed') {
        queryClient.invalidateQueries({ queryKey: ['mailboxes', accountId] })
      }
    }).then((fn) => {
      console.log('[watcher] listener registered for mailbox', mailboxId)
      unlisten = fn
    })

    return () => {
      invoke('stop_watching', { mailboxId }).catch(() => {})
      unlisten?.()
    }
  }, [mailboxId, mailboxKey, accountId, queryClient])
}
