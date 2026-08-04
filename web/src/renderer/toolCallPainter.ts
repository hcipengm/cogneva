export interface ToolCall {
  id: string;
  name: string;
  status: 'pending' | 'running' | 'done' | 'error';
  params?: Record<string, unknown>;
  result?: unknown;
}

const STATUS_COLORS: Record<ToolCall['status'], string> = {
  pending: '#64748b',
  running: '#3b82f6',
  done: '#22c55e',
  error: '#ef4444',
};

const STATUS_ICONS: Record<ToolCall['status'], string> = {
  pending: '⏸',
  running: '⚙',
  done: '✓',
  error: '✕',
};

export function drawToolCallCard(
  ctx: CanvasRenderingContext2D,
  tool: ToolCall,
  x: number,
  y: number,
  width: number
): number {
  const height = 56;
  const radius = 12;

  // Background
  ctx.save();
  ctx.fillStyle = 'rgba(15, 23, 42, 0.8)';
  roundRect(ctx, x, y, width, height, radius);
  ctx.fill();

  // Left accent bar
  ctx.fillStyle = STATUS_COLORS[tool.status];
  ctx.beginPath();
  ctx.roundRect(x, y, 4, height, [radius, 0, 0, radius]);
  ctx.fill();
  ctx.restore();

  // Icon
  ctx.fillStyle = STATUS_COLORS[tool.status];
  ctx.font = '16px Inter, sans-serif';
  ctx.fillText(STATUS_ICONS[tool.status], x + 18, y + 24);

  // Name
  ctx.fillStyle = '#e2e8f0';
  ctx.font = '600 13px Inter, sans-serif';
  ctx.fillText(tool.name, x + 44, y + 24);

  // Status
  ctx.fillStyle = STATUS_COLORS[tool.status];
  ctx.font = '10px Inter, sans-serif';
  ctx.fillText(tool.status.toUpperCase(), x + 44, y + 42);

  // Progress bar for running
  if (tool.status === 'running') {
    const barX = x + width - 80;
    const barY = y + 34;
    const barW = 60;
    const barH = 4;

    ctx.fillStyle = 'rgba(100, 116, 139, 0.4)';
    roundRect(ctx, barX, barY, barW, barH, 2);
    ctx.fill();

    const time = Date.now() / 1000;
    const progress = 0.3 + 0.4 * Math.sin(time * 2);
    ctx.fillStyle = STATUS_COLORS.running;
    roundRect(ctx, barX, barY, barW * progress, barH, 2);
    ctx.fill();
  }

  return height;
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
