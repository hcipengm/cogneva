import {
  layoutParagraph,
  measureParagraph,
  computeTightWidth,
  type Prepared,
} from '@/layout/textEngine';
import type { MessageRole } from '@/store/streamStore';

export interface RenderMessage {
  id: string;
  role: MessageRole;
  text: string;
  prepared?: Prepared;
}

const PADDING_X = 18;
const PADDING_Y = 14;
const MAX_BUBBLE_WIDTH = 520;
const MIN_BUBBLE_WIDTH = 80;
const BUBBLE_RADIUS = 18;
const GAP_BETWEEN_MESSAGES = 20;
const LINE_HEIGHT = 1.55;
const FONT_SIZE = 15;

const COLORS = {
  userBg: ['#3b82f6', '#2563eb'] as const,
  assistantBg: ['#1e293b', '#0f172a'] as const,
  userText: '#ffffff',
  assistantText: '#e2e8f0',
  shadow: 'rgba(0, 0, 0, 0.35)',
  indicator: '#38bdf8',
};

export function prepareMessage(message: RenderMessage): RenderMessage {
  return {
    ...message,
    prepared: measureParagraph(message.text),
  };
}

export function measureMessageBubble(
  message: RenderMessage,
  availableWidth: number
): { width: number; height: number } {
  if (!message.prepared) {
    return { width: 0, height: 0 };
  }

  const maxTextWidth = Math.min(MAX_BUBBLE_WIDTH, availableWidth - 96) - PADDING_X * 2;
  const tightWidth = computeTightWidth(message.prepared, maxTextWidth);
  const bubbleWidth = Math.max(
    MIN_BUBBLE_WIDTH,
    Math.min(tightWidth + PADDING_X * 2, maxTextWidth + PADDING_X * 2)
  );

  const textWidth = bubbleWidth - PADDING_X * 2;
  const { height: textHeight } = layoutParagraph(
    message.prepared,
    textWidth,
    LINE_HEIGHT
  );
  const bubbleHeight = textHeight + PADDING_Y * 2;

  return { width: bubbleWidth, height: bubbleHeight };
}

export function drawMessageBubble(
  ctx: CanvasRenderingContext2D,
  message: RenderMessage,
  x: number,
  y: number,
  availableWidth: number
): number {
  if (!message.prepared) {
    return 0;
  }

  const maxTextWidth = Math.min(MAX_BUBBLE_WIDTH, availableWidth - 96) - PADDING_X * 2;
  const tightWidth = computeTightWidth(message.prepared, maxTextWidth);
  const bubbleWidth = Math.max(
    MIN_BUBBLE_WIDTH,
    Math.min(tightWidth + PADDING_X * 2, maxTextWidth + PADDING_X * 2)
  );

  const textWidth = bubbleWidth - PADDING_X * 2;
  const { lines, height: textHeight } = layoutParagraph(
    message.prepared,
    textWidth,
    LINE_HEIGHT
  );
  const bubbleHeight = textHeight + PADDING_Y * 2;

  const isUser = message.role === 'user';

  // Shadow
  ctx.save();
  ctx.shadowColor = COLORS.shadow;
  ctx.shadowBlur = 16;
  ctx.shadowOffsetY = 6;

  // Bubble background with gradient
  const gradient = ctx.createLinearGradient(x, y, x, y + bubbleHeight);
  if (isUser) {
    gradient.addColorStop(0, COLORS.userBg[0]);
    gradient.addColorStop(1, COLORS.userBg[1]);
  } else {
    gradient.addColorStop(0, COLORS.assistantBg[0]);
    gradient.addColorStop(1, COLORS.assistantBg[1]);
  }

  ctx.fillStyle = gradient;
  roundRect(ctx, x, y, bubbleWidth, bubbleHeight, BUBBLE_RADIUS);
  ctx.fill();
  ctx.restore();

  // Text
  ctx.fillStyle = isUser ? COLORS.userText : COLORS.assistantText;
  ctx.font = `500 ${FONT_SIZE}px Inter, "PingFang SC", "Microsoft YaHei", sans-serif`;
  ctx.textBaseline = 'alphabetic';

  const lineHeightPx = FONT_SIZE * LINE_HEIGHT;
  const startY = y + PADDING_Y + 13;

  lines.forEach((line, index) => {
    ctx.fillText(line.text, x + PADDING_X, startY + index * lineHeightPx);
  });

  // Streaming indicator dot
  if (message.role === 'assistant' && message.text.length > 0) {
    drawStreamingIndicator(ctx, x + bubbleWidth - 22, y + bubbleHeight - 12);
  }

  return bubbleHeight;
}

function drawStreamingIndicator(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number
): void {
  const time = Date.now() / 1000;
  const alpha = 0.5 + 0.5 * Math.sin(time * 4);
  ctx.save();
  ctx.shadowColor = COLORS.indicator;
  ctx.shadowBlur = 8;
  ctx.fillStyle = `rgba(56, 189, 248, ${alpha})`;
  ctx.beginPath();
  ctx.arc(x, y, 3, 0, Math.PI * 2);
  ctx.fill();
  ctx.restore();
}

function roundRect(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  radius: number
): void {
  const r = Math.min(radius, width / 2, height / 2);
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + width - r, y);
  ctx.quadraticCurveTo(x + width, y, x + width, y + r);
  ctx.lineTo(x + width, y + height - r);
  ctx.quadraticCurveTo(x + width, y + height, x + width - r, y + height);
  ctx.lineTo(x + r, y + height);
  ctx.quadraticCurveTo(x, y + height, x, y + height - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
}

export { GAP_BETWEEN_MESSAGES, MAX_BUBBLE_WIDTH };
