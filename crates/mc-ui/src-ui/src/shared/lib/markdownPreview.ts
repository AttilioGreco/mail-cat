import { remark } from 'remark';
import remarkBreaks from 'remark-breaks';
import remarkGfm from 'remark-gfm';
import remarkHtml from 'remark-html';

// Single newlines are line breaks, exactly as the send path renders them.
const processor = remark().use(remarkGfm).use(remarkBreaks).use(remarkHtml, { sanitize: false });

const fromBase64 = (b64: string) => new TextDecoder().decode(Uint8Array.from(atob(b64), (c) => c.charCodeAt(0)));

/**
 * Converte markdown in HTML per il pannello anteprima.
 * I commenti quoted-block/signature-block vengono trasformati in blocchi styled.
 */
export async function markdownToPreviewHtml(md: string): Promise<string> {
  const file = await processor.process(md);
  let html = String(file);

  // Sostituisce i commenti quoted-block con blockquote collassabili
  html = html.replace(
    /<!-- quoted-block: ([A-Za-z0-9+/=]+) -->/g,
    (_match, b64) => {
      const inner = fromBase64(b64);
      return `<blockquote class="mc-quoted-block" style="border-left:3px solid #ccc;margin:8px 0;padding:4px 12px;color:#666;font-size:0.9em">${inner}</blockquote>`;
    },
  );

  // Sostituisce i commenti signature-block con un div firma
  html = html.replace(
    /<!-- signature-block: ([A-Za-z0-9+/=]+) -->/g,
    (_match, b64) => {
      const inner = fromBase64(b64);
      return `<div class="mc-signature-block" style="margin-top:16px;border-top:1px solid #e0e0e0;padding-top:12px;color:#555;font-size:0.9em">${inner}</div>`;
    },
  );

  return html;
}
