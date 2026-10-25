import { token, tokenAlpha } from './theme';

export interface Viewport {
  width: number;
  height: number;
  dpr: number;
  scrollY: number;
}

export interface RenderFrame {
  connectionStatus: string;
  viewport: Viewport;
  autoScroll: boolean;
}

/**
 * 工作台的底。只画底，不画内容。
 *
 * 这个画布以前画的是三样东西：一条栅格、一段气泡对话、两个写死的可视化面板
 * （拓扑与流水线）。三样都在说同一类谎——它们看起来在讲集群里发生的事，实际
 * 上一个像素与集群无关：面板上的节点是字面量，流水线永远停在「K3s Deploy」，
 * 画出来的气泡与服务端真正流回来的消息是两套数据。读者分不出哪一块是读数、
 * 哪一块是装饰，而一块分不出真假的读数比没有更坏。
 *
 * 所以内容搬走了：对话是 DOM 里的回合，拓扑与流水线等真实端点接上之后再画。
 * 这里只剩一层底——主题纯色，外加极低频、极低对比的粒子，让一屏静止的界面不
 * 那么像一张截图。粒子的位移不携带任何信息，因此它不冒充读数；`prefers-reduced-
 * motion` 打开时连它也不动。
 */
export class CanvasRenderer {
  private ctx: CanvasRenderingContext2D;
  private viewport: Viewport = { width: 0, height: 0, dpr: 1, scrollY: 0 };
  private particles: { x: number; y: number; vx: number; vy: number; r: number }[] = [];
  private lastFrameAt = 0;
  private still = false;

  constructor(canvas: HTMLCanvasElement) {
    const ctx = canvas.getContext('2d', { alpha: false });
    if (!ctx) {
      throw new Error('Failed to get Canvas 2D context');
    }
    this.ctx = ctx;
    this.still =
      typeof window !== 'undefined' &&
      typeof window.matchMedia === 'function' &&
      window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  }

  resize(width: number, height: number, dpr: number): void {
    const first = this.viewport.width === 0;
    this.viewport = { ...this.viewport, width, height, dpr };
    if (first || this.particles.length === 0) this.seedParticles();
  }

  getViewport(): Viewport {
    return { ...this.viewport };
  }

  /** 内容不再滚动：对话由 DOM 自己滚。留着这个接口，画布这一侧不必再有特例。 */
  setScrollY(_scrollY: number): void {}

  getContentHeight(): number {
    return 0;
  }

  clearCache(): void {}

  render(frame: RenderFrame): void {
    const now = performance.now();
    const delta = this.lastFrameAt ? Math.min(64, now - this.lastFrameAt) : 0;
    this.lastFrameAt = now;

    this.drawGround();

    if (this.still) {
      this.drawParticles();
      return;
    }

    // 粒子的速度是「每分钟几像素」量级：看得见在呼吸，盯着看不出在动。
    for (const p of this.particles) {
      p.x += (p.vx * delta) / 1000;
      p.y += (p.vy * delta) / 1000;
      const w = this.viewport.width;
      const h = this.viewport.height;
      if (p.x < -20) p.x = w + 20;
      if (p.x > w + 20) p.x = -20;
      if (p.y < -20) p.y = h + 20;
      if (p.y > h + 20) p.y = -20;
    }
    this.drawParticles();
    void frame;
  }

  private seedParticles(): void {
    const count = Math.max(
      18,
      Math.min(64, Math.round((this.viewport.width * this.viewport.height) / 42000))
    );
    this.particles = Array.from({ length: count }, () => ({
      x: Math.random() * this.viewport.width,
      y: Math.random() * this.viewport.height,
      // 每秒 −6 到 +6 像素的漂移。
      vx: (Math.random() - 0.5) * 12,
      vy: (Math.random() - 0.5) * 12,
      r: 0.6 + Math.random() * 1.1,
    }));
  }

  /** 底：主题纯色。不铺栅格——栅格是「这里可以拖东西」的暗示，而这里不能。 */
  private drawGround(): void {
    const { ctx, viewport } = this;
    ctx.fillStyle = token('ink-1000');
    ctx.fillRect(0, 0, viewport.width, viewport.height);

    // 顶部一道极淡的渐变，把标题行与内容区分开，不靠一条实线。
    const gradient = ctx.createLinearGradient(0, 0, 0, 160);
    gradient.addColorStop(0, tokenAlpha('ink-800', 0.55));
    gradient.addColorStop(1, tokenAlpha('ink-800', 0));
    ctx.fillStyle = gradient;
    ctx.fillRect(0, 0, viewport.width, 160);
  }

  private drawParticles(): void {
    const { ctx } = this;
    ctx.save();
    ctx.fillStyle = tokenAlpha('line-strong', 0.35);
    for (const p of this.particles) {
      ctx.beginPath();
      ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.restore();
  }
}
