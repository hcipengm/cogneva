import {
  prepareWithSegments,
  layoutWithLines,
  walkLineRanges,
  type PreparedTextWithSegments,
} from '@chenglou/pretext';

const FONT_STACK =
  '16px Inter, "PingFang SC", "Microsoft YaHei", sans-serif';
const CODE_FONT_STACK =
  '14px "JetBrains Mono", "Fira Code", monospace';

let fontsLoaded = false;

export async function ensureFonts(): Promise<void> {
  if (fontsLoaded) return;
  if (typeof document === 'undefined') return;

  await Promise.all([
    document.fonts.load(FONT_STACK),
    document.fonts.load(CODE_FONT_STACK),
  ]);

  fontsLoaded = true;
}

export interface ParagraphLayout {
  lines: Array<{ text: string; width: number }>;
  height: number;
  lineCount: number;
}

export function measureParagraph(
  text: string,
  options: { isCode?: boolean } = {}
): PreparedTextWithSegments {
  return prepareWithSegments(
    text,
    options.isCode ? CODE_FONT_STACK : FONT_STACK,
    {
      whiteSpace: 'pre-wrap',
      wordBreak: 'normal',
    }
  );
}

export function layoutParagraph(
  prepared: PreparedTextWithSegments,
  maxWidth: number,
  lineHeight: number
): ParagraphLayout {
  const result = layoutWithLines(prepared, maxWidth, lineHeight);
  return {
    lines: result.lines.map((line) => ({
      text: line.text,
      width: line.width,
    })),
    height: result.height,
    lineCount: result.lineCount,
  };
}

export function computeTightWidth(
  prepared: PreparedTextWithSegments,
  maxWidth: number
): number {
  let maxLineWidth = 0;
  walkLineRanges(prepared, maxWidth, (line) => {
    if (line.width > maxLineWidth) {
      maxLineWidth = line.width;
    }
  });
  return Math.ceil(maxLineWidth);
}

export type { PreparedTextWithSegments as Prepared };
