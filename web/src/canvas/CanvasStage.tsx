import { useEffect, useRef, useCallback } from 'react';
import { CanvasRenderer } from '@/renderer/canvasRenderer';
import { useStreamStore } from '@/store/streamStore';
import { ensureFonts } from '@/layout/textEngine';

export function CanvasStage() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const rendererRef = useRef<CanvasRenderer | null>(null);
  const rafRef = useRef<number>(0);
  const isUserScrollingRef = useRef(false);
  const scrollTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const setupCanvas = useCallback(async () => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    await ensureFonts();

    const renderer = new CanvasRenderer(canvas);
    rendererRef.current = renderer;

    const resize = () => {
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();
      const width = Math.max(1, Math.floor(rect.width));
      const height = Math.max(1, Math.floor(rect.height));

      canvas.width = width * dpr;
      canvas.height = height * dpr;
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;

      const ctx = canvas.getContext('2d');
      if (ctx) {
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      }

      renderer.resize(width, height, dpr);
    };

    const handleWheel = (event: WheelEvent) => {
      event.preventDefault();
      isUserScrollingRef.current = true;
      if (scrollTimeoutRef.current) {
        clearTimeout(scrollTimeoutRef.current);
      }
      scrollTimeoutRef.current = setTimeout(() => {
        isUserScrollingRef.current = false;
      }, 1500);

      const viewport = renderer.getViewport();
      renderer.setScrollY(viewport.scrollY + event.deltaY);
    };

    resize();
    window.addEventListener('resize', resize);
    canvas.addEventListener('wheel', handleWheel, { passive: false });

    const loop = () => {
      const state = useStreamStore.getState();
      const lastMessage = state.messages[state.messages.length - 1];
      const isStreaming = lastMessage?.status === 'streaming';
      const autoScroll = isStreaming && !isUserScrollingRef.current;

      renderer.render({
        messages: state.messages,
        connectionStatus: state.connectionStatus,
        viewport: renderer.getViewport(),
        autoScroll,
      });
      rafRef.current = requestAnimationFrame(loop);
    };

    rafRef.current = requestAnimationFrame(loop);

    return () => {
      window.removeEventListener('resize', resize);
      canvas.removeEventListener('wheel', handleWheel);
      cancelAnimationFrame(rafRef.current);
      if (scrollTimeoutRef.current) {
        clearTimeout(scrollTimeoutRef.current);
      }
    };
  }, []);

  useEffect(() => {
    let cleanup: (() => void) | undefined;

    setupCanvas().then((cleanupFn) => {
      cleanup = cleanupFn;
    });

    return () => {
      cleanup?.();
    };
  }, [setupCanvas]);

  return (
    <canvas
      ref={canvasRef}
      className="absolute inset-0 h-full w-full"
      aria-label="Cogneva event stream"
      role="img"
    />
  );
}
