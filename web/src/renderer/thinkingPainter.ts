export function drawThinkingBlock(
  ctx: CanvasRenderingContext2D,
  text: string,
  x: number,
  y: number,
  width: number
): number {
  const height = 42;
  const radius = 12;

  // Background
  ctx.save();
  ctx.fillStyle = 'rgba(30, 41, 59, 0.6)';
  roundRect(ctx, x, y, width, height, radius);
  ctx.fill();
  ctx.restore();

  // Pulsing dot
  const time = Date.now() / 1000;
  const alpha = 0.5 + 0.5 * Math.sin(time * 3);
  ctx.save();
  ctx.shadowColor = '#a855f7';
  ctx.shadowBlur = 8;
  ctx.fillStyle = `rgba(168, 85, 247, ${alpha})`;
  ctx.beginPath();
  ctx.arc(x + 18, y + 21, 4, 0, Math.PI * 2);
  ctx.fill();
  ctx.restore();

  // Text
  ctx.fillStyle = '#c084fc';
  ctx.font = '500 13px Inter, sans-serif';
  ctx.fillText(text || '思考中...', x + 34, y + 25);

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
