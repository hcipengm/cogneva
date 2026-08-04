import type { Message } from '@/store/streamStore';
import {
  drawMessageBubble,
  prepareMessage,
  measureMessageBubble,
  GAP_BETWEEN_MESSAGES,
  type RenderMessage,
} from './messagePainter';
import { drawTopology } from './topologyPainter';
import { drawPipeline } from './pipelinePainter';
import { drawToolCallCard } from './toolCallPainter';
import { drawThinkingBlock } from './thinkingPainter';
import type { TopologyNode, TopologyEdge } from './topologyPainter';
import type { PipelineStage } from './pipelinePainter';

export interface Viewport {
  width: number;
  height: number;
  dpr: number;
  scrollY: number;
}

export interface RenderFrame {
  messages: Message[];
  connectionStatus: string;
  viewport: Viewport;
  autoScroll: boolean;
}

interface LayoutItem {
  message: RenderMessage;
  x: number;
  y: number;
  width: number;
  height: number;
}

const PANEL_WIDTH = 280;
const PANEL_MARGIN = 24;

export class CanvasRenderer {
  private ctx: CanvasRenderingContext2D;
  private viewport: Viewport = { width: 0, height: 0, dpr: 1, scrollY: 0 };
  private cache = new Map<string, RenderMessage>();
  private contentHeight = 0;

  constructor(canvas: HTMLCanvasElement) {
    const ctx = canvas.getContext('2d', { alpha: false });
    if (!ctx) {
      throw new Error('Failed to get Canvas 2D context');
    }
    this.ctx = ctx;
  }

  resize(width: number, height: number, dpr: number): void {
    this.viewport = {
      ...this.viewport,
      width,
      height,
      dpr,
    };
  }

  getViewport(): Viewport {
    return { ...this.viewport };
  }

  setScrollY(scrollY: number): void {
    const maxScroll = Math.max(
      0,
      this.contentHeight - this.viewport.height + 120
    );
    this.viewport.scrollY = Math.max(0, Math.min(scrollY, maxScroll));
  }

  render(frame: RenderFrame): void {
    const { ctx, viewport } = this;

    // Background
    this.drawBackground();

    // Prepare messages with caching
    const renderMessages = frame.messages.map((message) => {
      const cacheKey = `${message.id}:${message.text.length}:${message.status}`;
      const cached = this.cache.get(cacheKey);
      if (cached && cached.text === message.text) {
        return cached;
      }
      const prepared = prepareMessage({
        id: message.id,
        role: message.role,
        text: message.text,
      });
      this.cache.set(cacheKey, prepared);
      return prepared;
    });

    // Layout messages in the left area
    const contentWidth = viewport.width - PANEL_WIDTH - PANEL_MARGIN * 2;
    const marginX = 28;
    const startY = 28;
    const layoutItems: LayoutItem[] = [];
    let currentY = startY;

    renderMessages.forEach((message) => {
      const { width: bubbleWidth, height: bubbleHeight } = measureMessageBubble(
        message,
        contentWidth
      );

      const bubbleX =
        message.role === 'user'
          ? contentWidth - marginX - bubbleWidth
          : marginX;

      layoutItems.push({
        message,
        x: bubbleX,
        y: currentY,
        width: bubbleWidth,
        height: bubbleHeight,
      });

      currentY += bubbleHeight + GAP_BETWEEN_MESSAGES;
    });

    this.contentHeight = currentY;

    // Auto-scroll to bottom when streaming
    if (frame.autoScroll && this.contentHeight > viewport.height) {
      const targetScrollY = this.contentHeight - viewport.height + 120;
      this.viewport.scrollY = Math.max(0, targetScrollY);
    }

    const offsetY = -this.viewport.scrollY;

    // Draw messages
    let lastAssistantY = 0;
    layoutItems.forEach((item) => {
      drawMessageBubble(ctx, item.message, item.x, item.y + offsetY, contentWidth);
      if (item.message.role === 'assistant') {
        lastAssistantY = item.y + offsetY + item.height + 12;
      }
    });

    // Draw thinking block and tool calls during streaming
    const lastMessage = frame.messages[frame.messages.length - 1];
    if (lastMessage?.role === 'assistant' && lastMessage.status === 'streaming') {
      const cardX = marginX;
      const cardWidth = Math.min(400, contentWidth - marginX * 2);
      let cardY = lastAssistantY;

      if (cardY > 0) {
        cardY += drawThinkingBlock(ctx, '分析意图并规划工具调用...', cardX, cardY, cardWidth);
        cardY += 10;
        cardY += drawToolCallCard(ctx, { id: '1', name: 'health_latency_profiler', status: 'running' }, cardX, cardY, cardWidth);
      }
    }

    // Draw right-side visualization panels
    this.drawVisualizationPanels();

    // Draw connection status indicator
    this.drawConnectionStatus(frame.connectionStatus);
  }

  getContentHeight(): number {
    return this.contentHeight;
  }

  private drawBackground(): void {
    const { ctx, viewport } = this;

    // Base gradient
    const gradient = ctx.createRadialGradient(
      viewport.width / 2,
      viewport.height / 2,
      0,
      viewport.width / 2,
      viewport.height / 2,
      viewport.width * 0.8
    );
    gradient.addColorStop(0, '#0b1221');
    gradient.addColorStop(1, '#020617');
    ctx.fillStyle = gradient;
    ctx.fillRect(0, 0, viewport.width, viewport.height);

    // Subtle grid
    ctx.strokeStyle = 'rgba(148, 163, 184, 0.04)';
    ctx.lineWidth = 1;

    const gridSize = 48;
    const offsetY = -this.viewport.scrollY % gridSize;

    ctx.beginPath();
    for (let x = 0; x < viewport.width; x += gridSize) {
      ctx.moveTo(x, 0);
      ctx.lineTo(x, viewport.height);
    }
    for (let y = offsetY; y < viewport.height; y += gridSize) {
      ctx.moveTo(0, y);
      ctx.lineTo(viewport.width, y);
    }
    ctx.stroke();

    // Top fade for input area
    const topFade = ctx.createLinearGradient(0, 0, 0, 80);
    topFade.addColorStop(0, 'rgba(2, 6, 23, 0.7)');
    topFade.addColorStop(1, 'rgba(2, 6, 23, 0)');
    ctx.fillStyle = topFade;
    ctx.fillRect(0, 0, viewport.width, 80);
  }

  private drawVisualizationPanels(): void {
    const { ctx, viewport } = this;
    const panelX = viewport.width - PANEL_WIDTH - PANEL_MARGIN;
    const panelY = 28;

    // Topology panel
    const topologyNodes: TopologyNode[] = [
      { id: 'gateway', label: 'Gateway', x: 140, y: 70, status: 'healthy', type: 'gateway' },
      { id: 'core', label: 'Cogneva Core', x: 80, y: 130, status: 'running', type: 'core' },
      { id: 'agent1', label: 'Agent Alpha', x: 180, y: 180, status: 'running', type: 'agent' },
      { id: 'agent2', label: 'Agent Beta', x: 60, y: 220, status: 'idle', type: 'agent' },
      { id: 'sandbox', label: 'Sandbox', x: 130, y: 270, status: 'healthy', type: 'sandbox' },
    ];
    const topologyEdges: TopologyEdge[] = [
      { from: 'gateway', to: 'core', active: true },
      { from: 'core', to: 'agent1', active: true },
      { from: 'core', to: 'agent2', active: false },
      { from: 'agent1', to: 'sandbox', active: true },
    ];

    drawTopology(
      ctx,
      topologyNodes,
      topologyEdges,
      panelX,
      panelY,
      PANEL_WIDTH,
      320
    );

    // Pipeline panel
    const pipelineStages: PipelineStage[] = [
      { id: 'bootstrap', label: 'Bootstrap', status: 'done' },
      { id: 'probe', label: 'Environment Probe', status: 'done' },
      { id: 'deploy', label: 'K3s Deploy', status: 'running' },
      { id: 'evolve', label: 'Self Evolution', status: 'pending' },
      { id: 'handoff', label: 'WebUI Handoff', status: 'pending' },
    ];

    drawPipeline(
      ctx,
      pipelineStages,
      panelX,
      panelY + 340,
      PANEL_WIDTH,
      240
    );
  }

  private drawConnectionStatus(status: string): void {
    const { ctx, viewport } = this;
    const x = viewport.width - 28;
    const y = 28;

    const colors: Record<string, string> = {
      open: '#22c55e',
      connecting: '#f59e0b',
      closed: '#64748b',
      error: '#ef4444',
    };

    const color = colors[status] ?? '#64748b';

    ctx.save();
    ctx.shadowColor = color;
    ctx.shadowBlur = 10;
    ctx.fillStyle = color;
    ctx.beginPath();
    ctx.arc(x, y, 5, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();

    ctx.fillStyle = 'rgba(226, 232, 240, 0.7)';
    ctx.font = '11px Inter, sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(status, x - 14, y + 4);
    ctx.textAlign = 'left';
  }

  clearCache(): void {
    this.cache.clear();
  }
}
