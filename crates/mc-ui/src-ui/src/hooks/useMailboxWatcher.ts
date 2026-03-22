import { useEffect } from 'react'
import { listen, UnlistenFn } from '@tauri-apps/api/event'
import { invoke } from '@tauri-apps/api/core'
import { useQueryClient } from '@tanstack/react-query'

export interface MailboxEventPayload {
  mailbox_id: string
  kind: 'counts_changed' | 'new_message' | 'message_expunged' | 'flags_changed'
  total?: number
  unseen?: number
  provider_uid?: number
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

      switch (payload.kind) {
        case 'new_message':
        case 'message_expunged':
        case 'flags_changed':
          console.log('[watcher] invalidating messages key=', mailboxKey)
          queryClient.invalidateQueries({ queryKey: ['messages', mailboxKey] })
          break
        case 'counts_changed':
          console.log('[watcher] invalidating messages + mailboxes accountId=', accountId)
          queryClient.invalidateQueries({ queryKey: ['messages', mailboxKey] })
          queryClient.invalidateQueries({ queryKey: ['mailboxes', accountId] })
          break
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
