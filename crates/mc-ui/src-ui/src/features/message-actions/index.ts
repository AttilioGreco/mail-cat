export {
  useSetFlags,
  useMarkAsRead,
  useMarkAsUnread,
  useToggleStar,
  useMarkMailboxRead,
  useMoveMessage,
  useDeleteMessage,
  useSelectNeighbour,
  useDeleteMessages,
  useMoveMessages,
  useTrashThreads,
} from './model/useMessageMutations';
export type { BulkResult, MessageRef } from './model/useMessageMutations';
