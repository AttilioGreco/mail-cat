import type { InlineImage } from '@/lib/composeTypes';

const ACCEPTED_TYPES = new Set(['image/png', 'image/jpeg', 'image/gif', 'image/webp', 'image/svg+xml']);
const MAX_SIZE_BYTES = 10 * 1024 * 1024; // 10 MB

export interface InlineImageMap {
  get(cid: string): InlineImage | undefined;
  set(cid: string, img: InlineImage): void;
  entries(): IterableIterator<[string, InlineImage]>;
}

/** Estrae i File immagine da un DataTransfer (drop o paste). */
export function extractImageFiles(dataTransfer: DataTransfer): File[] {
  const files: File[] = [];
  for (const file of Array.from(dataTransfer.files)) {
    if (ACCEPTED_TYPES.has(file.type)) files.push(file);
  }
  // Fallback per paste senza files (es. screenshot da clipboard)
  if (files.length === 0) {
    for (const item of Array.from(dataTransfer.items)) {
      if (item.kind === 'file' && ACCEPTED_TYPES.has(item.type)) {
        const f = item.getAsFile();
        if (f) files.push(f);
      }
    }
  }
  return files;
}

export interface ProcessResult {
  markdown: string;
  image: InlineImage;
  error?: never;
}

export interface ProcessError {
  error: string;
  markdown: undefined;
  image: undefined;
}

/**
 * Processa un file immagine per l'inserimento inline.
 * Restituisce la sintassi markdown e l'oggetto InlineImage.
 */
export async function processImageFile(
  file: File,
  generateCid: () => string,
): Promise<ProcessResult | ProcessError> {
  if (!ACCEPTED_TYPES.has(file.type)) {
    return { error: `Formato non supportato: ${file.type}`, markdown: undefined, image: undefined };
  }
  if (file.size > MAX_SIZE_BYTES) {
    return { error: `Immagine troppo grande (max 10 MB): ${(file.size / 1024 / 1024).toFixed(1)} MB`, markdown: undefined, image: undefined };
  }

  const cid = generateCid();
  const objectUrl = URL.createObjectURL(file);

  return {
    markdown: `![${file.name}](${objectUrl})`,
    image: { cid, blob: file, mimeType: file.type },
  };
}
