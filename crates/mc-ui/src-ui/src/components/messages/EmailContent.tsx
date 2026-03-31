import DOMPurify from 'dompurify';
import { useThemeStore } from '@/store/themeStore';

interface EmailContentProps {
  htmlBody?: string | null;
  textBody?: string | null;
  className?: string;
}

function buildSrcDoc(cleanHtml: string, mode: 'auto' | 'light' | 'invert'): string {
  const baseCss = `
    html, body {
      margin: 0;
      padding: 0;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
      line-height: 1.5;
      word-break: break-word;
    }
    img, video, table { max-width: 100%; }
    pre { white-space: pre-wrap; }
    a { color: #1d4ed8; }
  `;

  const modeCss =
    mode === 'light'
      ? `body { background: #ffffff; color: #111827; }`
      : mode === 'invert'
        ? `
          body { background: #ffffff; color: #111827; }
          html { filter: invert(1) hue-rotate(180deg); }
          img, video { filter: invert(1) hue-rotate(180deg); }
        `
        : `body { background: #ffffff; color: inherit; }`;

  return `<!doctype html><html><head><meta charset="utf-8"><style>${baseCss}${modeCss}</style></head><body>${cleanHtml}</body></html>`;
}

export function EmailContent({ htmlBody, textBody, className }: EmailContentProps) {
  const mailContentMode = useThemeStore((s) => s.mailContentMode);

  if (htmlBody) {
    const clean = DOMPurify.sanitize(htmlBody, {
      ALLOWED_TAGS: [
        'p', 'br', 'b', 'i', 'em', 'strong', 'a', 'ul', 'ol', 'li', 'h1', 'h2', 'h3', 'h4',
        'blockquote', 'pre', 'code', 'div', 'span', 'table', 'tr', 'td', 'th', 'thead', 'tbody',
        'img', 'hr'
      ],
      ALLOWED_ATTR: ['href', 'target', 'rel', 'style', 'class', 'src', 'alt', 'width', 'height'],
      FORCE_BODY: true,
    });

    return (
      <iframe
        srcDoc={buildSrcDoc(clean, mailContentMode)}
        sandbox="allow-same-origin"
        className={className ?? 'w-full min-h-[280px] border border-border rounded-md bg-white'}
        title="email content"
      />
    );
  }

  return (
    <pre className="whitespace-pre-wrap font-sans text-sm leading-relaxed text-foreground">
      {textBody ?? '(No content)'}
    </pre>
  );
}
