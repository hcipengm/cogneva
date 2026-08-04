export interface PipelineStage {
  id: string;
  label: string;
  status: 'pending' | 'running' | 'done' | 'error';
}

const STATUS_COLORS: Record<PipelineStage['status'], string> = {
  pending: '#64748b',
  running: '#3b82f6',
  done: '#22c55e',
  error: '#ef4444',
};

export function drawPipeline(
  ctx: CanvasRenderingContext2D,
  stages: PipelineStage[],
  panelX: number,
  panelY: number,
  panelW: number,
  _panelH: number
): void {
  // Panel background
  ctx.save();
  ctx.fillStyle = 'rgba(15, 23, 42, 0.6)';
  roundRect(ctx, panelX, panelY, panelW, 44 + stages.length * 42, 16);
  ctx.fill();

  ctx.strokeStyle = 'rgba(148, 163, 184, 0.15)';
  ctx.lineWidth = 1;
  ctx.stroke();
  ctx.restore();

  // Title
  ctx.fillStyle = '#94a3b8';
  ctx.font = '12px Inter, sans-serif';
  ctx.fillText('SANDBOX PIPELINE', panelX + 16, panelY + 24);

  const startY = panelY + 48;
  const lineX = panelX + 28;

  stages.forEach((stage, index) => {
    const y = startY + index * 42;

    // Connector line
    if (index < stages.length - 1) {
      ctx.strokeStyle =
        stage.status === 'done' ? STATUS_COLORS.done : 'rgba(100, 116, 139, 0.4)';
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.moveTo(lineX, y + 6);
      ctx.lineTo(lineX, y + 42);
      ctx.stroke();
    }

    // Node dot
    ctx.save();
    ctx.shadowColor = STATUS_COLORS[stage.status];
    ctx.shadowBlur = 10;
    ctx.fillStyle = STATUS_COLORS[stage.status];
    ctx.beginPath();
    ctx.arc(lineX, y, 5, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();

    // Label
    ctx.fillStyle = '#e2e8f0';
    ctx.font = '12px Inter, sans-serif';
    ctx.fillText(stage.label, lineX + 18, y + 4);

    // Status badge
    const statusText = stage.status.toUpperCase();
    ctx.fillStyle = STATUS_COLORS[stage.status];
    ctx.font = '10px Inter, sans-serif';
    ctx.fillText(statusText, panelX + panelW - 52, y + 4);
  });
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
