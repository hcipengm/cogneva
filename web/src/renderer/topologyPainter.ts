export interface TopologyNode {
  id: string;
  label: string;
  x: number;
  y: number;
  status: 'healthy' | 'running' | 'error' | 'idle';
  type: 'core' | 'agent' | 'sandbox' | 'gateway';
}

export interface TopologyEdge {
  from: string;
  to: string;
  active: boolean;
}

const NODE_COLORS: Record<TopologyNode['status'], string> = {
  healthy: '#22c55e',
  running: '#3b82f6',
  error: '#ef4444',
  idle: '#64748b',
};

const TYPE_COLORS: Record<TopologyNode['type'], string> = {
  core: '#a855f7',
  agent: '#3b82f6',
  sandbox: '#f59e0b',
  gateway: '#14b8a6',
};

export function drawTopology(
  ctx: CanvasRenderingContext2D,
  nodes: TopologyNode[],
  edges: TopologyEdge[],
  panelX: number,
  panelY: number,
  panelW: number,
  panelH: number
): void {
  // Panel background
  ctx.save();
  ctx.fillStyle = 'rgba(15, 23, 42, 0.6)';
  roundRect(ctx, panelX, panelY, panelW, panelH, 16);
  ctx.fill();

  ctx.strokeStyle = 'rgba(148, 163, 184, 0.15)';
  ctx.lineWidth = 1;
  ctx.stroke();
  ctx.restore();

  // Title
  ctx.fillStyle = '#94a3b8';
  ctx.font = '12px Inter, sans-serif';
  ctx.fillText('CLUSTER TOPOLOGY', panelX + 16, panelY + 24);

  // Edges
  const nodeMap = new Map(nodes.map((n) => [n.id, n]));
  const time = Date.now() / 1000;

  edges.forEach((edge) => {
    const from = nodeMap.get(edge.from);
    const to = nodeMap.get(edge.to);
    if (!from || !to) return;

    ctx.beginPath();
    ctx.moveTo(panelX + from.x, panelY + from.y);
    ctx.lineTo(panelX + to.x, panelY + to.y);
    ctx.strokeStyle = edge.active
      ? `rgba(59, 130, 246, ${0.3 + 0.2 * Math.sin(time * 3)})`
      : 'rgba(148, 163, 184, 0.15)';
    ctx.lineWidth = edge.active ? 2 : 1;
    ctx.stroke();
  });

  // Nodes
  nodes.forEach((node) => {
    const x = panelX + node.x;
    const y = panelY + node.y;

    // Glow
    ctx.save();
    ctx.shadowColor = NODE_COLORS[node.status];
    ctx.shadowBlur = 12;
    ctx.fillStyle = NODE_COLORS[node.status];
    ctx.beginPath();
    ctx.arc(x, y, 6, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();

    // Ring
    ctx.strokeStyle = TYPE_COLORS[node.type];
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.arc(x, y, 9, 0, Math.PI * 2);
    ctx.stroke();

    // Label
    ctx.fillStyle = '#e2e8f0';
    ctx.font = '11px Inter, sans-serif';
    ctx.fillText(node.label, x + 16, y + 4);
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
