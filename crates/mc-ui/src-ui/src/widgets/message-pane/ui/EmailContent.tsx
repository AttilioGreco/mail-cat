import { useCallback, useEffect, useRef, useState } from 'react';
import DOMPurify from 'dompurify';
import { useThemeStore } from '@/entities/theme';

interface EmailContentProps {
  htmlBody?: string | null;
  textBody?: string | null;
  className?: string;
}

const BASE_CSS = `
  *{box-sizing:border-box;margin:0;padding:0}
  html,body{overflow:hidden}
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;
       font-size:14px;line-height:1.6;word-break:break-word;padding:12px 16px}
  img,video{max-width:100%;height:auto}
  img{display:block}
  a{color:#1a73e8}
  blockquote{border-left:3px solid #dadce0;margin:8px 0;padding-left:12px;color:#555}
  pre,code{font-family:monospace;font-size:13px;background:#f5f5f5;border-radius:3px;padding:2px 4px}
  pre{padding:10px;overflow-x:auto;white-space:pre-wrap}
  table{border-collapse:collapse;max-width:100%}
  td,th{padding:4px 8px}
`;

function buildSrcDoc(cleanHtml: string, mode: 'auto' | 'light' | 'invert'): string {
  const modeCss =
    mode === 'invert'
      ? `
        body { background: #ffffff; color: #111827; }
        html { filter: invert(1) hue-rotate(180deg); }
        img, video { filter: invert(1) hue-rotate(180deg); }
      `
      : `body { background: #ffffff; color: #111827; }`;

  return `<!doctype html><html><head><meta charset="utf-8"><style>${BASE_CSS}${modeCss}</style></head><body>${cleanHtml}</body></html>`;
}

export function EmailContent({ htmlBody, textBody, className }: EmailContentProps) {
  const mailContentMode = useThemeStore((s) => s.mailContentMode);
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const observerRef = useRef<ResizeObserver | null>(null);
  const [height, setHeight] = useState(120);

  // The iframe has no intrinsic height, so it must be measured from the inside
  // and grown to fit — otherwise the body is clipped at whatever the CSS says.
  const measure = useCallback(() => {
    const h = iframeRef.current?.contentDocument?.documentElement.scrollHeight;
    if (h && h > 0) setHeight(h);
  }, []);

  const onLoad = useCallback(() => {
    measure();
    const doc = iframeRef.current?.contentDocument;
    if (!doc) return;
    // Re-measure as images and late layout land.
    observerRef.current?.disconnect();
    const ro = new ResizeObserver(measure);
    ro.observe(doc.documentElement);
    observerRef.current = ro;
    doc.querySelectorAll('img').forEach((img) => img.addEventListener('load', measure));
  }, [measure]);

  useEffect(() => () => observerRef.current?.disconnect(), []);

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
        ref={iframeRef}
        srcDoc={buildSrcDoc(clean, mailContentMode)}
        sandbox="allow-same-origin"
        onLoad={onLoad}
        style={{ height: `${height}px` }}
        className={className ?? 'w-full border-0 bg-white'}
        title="email content"
      />
    );
  }

  return (
    <pre className="whitespace-pre-wrap break-words font-sans text-sm leading-relaxed text-foreground">
      {textBody ?? '(No content)'}
    </pre>
  );
}
