import { Video } from 'lucide-react';
import { openUrl } from '@tauri-apps/plugin-opener';
import { Button } from '@/shared/ui/button';

/** Opens the event's video call in the browser; `compact` is the icon-only form for dense rows. */
export function JoinButton({ url, compact }: { url: string; compact?: boolean }) {
  return (
    <Button
      type="button"
      size={compact ? 'icon-xs' : 'sm'}
      variant={compact ? 'ghost' : 'default'}
      title={compact ? 'Partecipa alla chiamata' : undefined}
      aria-label="Partecipa"
      onClick={() => void openUrl(url)}
    >
      <Video />
      {!compact && 'Partecipa'}
    </Button>
  );
}
