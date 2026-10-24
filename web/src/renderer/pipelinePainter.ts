import { font, token, tokenAlpha } from '@/renderer/theme';

export interface PipelineStage {
  id: string;
  label: string;
  status: 'pending' | 'running' | 'done' | 'error';
}

/** 每次落笔现读：换主题后下一帧就是新颜色（模块加载时取一次会停在旧主题上）。 */
function statusColor(status: PipelineStage['status']): string {
  switch (status) {
    case 'pending':
      return token('muted');
    case 'running':
      return token('brass');
    case 'done':
      return token('ok');
    case 'error':
      return token('danger');
  }
}

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
  ctx.fillStyle = tokenAlpha('ink-800', 0.6);
  roundRect(ctx, panelX, panelY, panelW, 44 + stages.length * 42, 16);
  ctx.fill();

  ctx.strokeStyle = tokenAlpha('line', 0.8);
  ctx.lineWidth = 1;
  ctx.stroke();
  ctx.restore();

  // Title
  ctx.fillStyle = token('muted');
  ctx.font = `12px ${font('ui')}`;
  ctx.fillText('SANDBOX PIPELINE', panelX + 16, panelY + 24);

  const startY = panelY + 48;
  const lineX = panelX + 28;

  stages.forEach((stage, index) => {
    const y = startY + index * 42;

    // Connector line
    if (index < stages.length - 1) {
      ctx.strokeStyle =
        stage.status === 'done' ? token('ok') : tokenAlpha('muted', 0.4);
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.moveTo(lineX, y + 6);
      ctx.lineTo(lineX, y + 42);
      ctx.stroke();
    }

    // Node dot
    ctx.save();
    ctx.shadowColor = statusColor(stage.status);
    ctx.shadowBlur = 10;
    ctx.fillStyle = statusColor(stage.status);
    ctx.beginPath();
    ctx.arc(lineX, y, 5, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();

    // Label
    ctx.fillStyle = token('paper');
    ctx.font = `12px ${font('ui')}`;
    ctx.fillText(stage.label, lineX + 18, y + 4);

    // Status badge
    const statusText = stage.status.toUpperCase();
    ctx.fillStyle = statusColor(stage.status);
    ctx.font = `10px ${font('mono')}`;
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
