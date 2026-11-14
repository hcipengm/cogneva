import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  getChatCommands,
  getChatQuota,
  getWorkspaceTree,
  type ChatCommand,
  type ChatQuota,
  type WorkspaceEntry,
} from '@/api/client';
import { callsForGoal, shortGoal, type GoalLive } from '@/conversation/dispatch';
import { Markdown } from '@/conversation/Markdown';
import { TodoPanel, todoListsIn } from '@/conversation/TodoPanel';
import { toolDiff } from '@/conversation/toolDiff';
import type { StreamClient } from '@/hooks/useEventStream';
import { sessionKind, useChatStore, type ChatMessage, type ChatSession } from '@/store/chatStore';
import { useStreamStore, type ToolCallRecord } from '@/store/streamStore';
import {
  DEFAULT_PERMISSION_MODE,
  usePermissionStore,
  type PermissionAsk,
} from '@/store/permissionStore';
import { useMemoryStore } from '@/store/memoryStore';
import { useShell } from '@/shell/store';
import type { ContextCompaction, SessionSpend, TurnUsage } from '@/types/events';
import { Note } from '@/ui/Note';

/**
 * 一段对话的窗口。
 *
 * 画成回合而不是气泡：气泡把每一句圈成一张卡片，一段会话于是变成一串互不相干
 * 的胶囊，读者要在里面找「上一句是谁说的、这一轮到哪儿了」。这里用左侧一条细线
 * 贯穿一轮、角色用等宽体标在行首，句子就是句子本身。正在流的那一轮线是亮的，
 * 流完就暗下去——一轮的边界因此是看得见的，而不是靠气泡的边缘去猜。
 */

function stamp(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  return d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
}

/**
 * 账上一条变更的叫法。身份是 `kind:scope:literal` 三段，正文里那段才是人认得的；
 * 整串放在悬停提示里——点进去靠的是整串，读名字靠的是这一段。
 */
function shortChange(id: string): string {
  const parts = id.split(':');
  const literal = parts.length >= 3 ? parts.slice(2).join(':') : id;
  return literal.length > 14 ? `${literal.slice(0, 14)}…` : literal;
}

/** 一万以上按「3.2 万」读，以下的整数带千分位。 */
function compactCount(n: number): string {
  if (n >= 10000) return `${(n / 10000).toFixed(1)} 万`;
  return n.toLocaleString('zh-CN');
}

/**
 * 一轮花销读成一行：轮次、进出 token、时长、模型、费用。
 *
 * 读不到的那几项不写：费用只有上游声明了价目才有数，模型名只有上游报了才有，
 * 把它们写成 0 或「未知」就是拿一个没读到的值冒充一个读到的值。
 */
function usageLine(usage: TurnUsage): string {
  const parts: string[] = [];
  if (usage.rounds !== null) parts.push(`${usage.rounds} 轮`);
  if (usage.inputTokens !== null || usage.outputTokens !== null) {
    const input = usage.inputTokens === null ? '—' : compactCount(usage.inputTokens);
    const output = usage.outputTokens === null ? '—' : compactCount(usage.outputTokens);
    parts.push(`输入 ${input} · 输出 ${output}`);
  }
  if (usage.durationMs !== null) parts.push(`${(usage.durationMs / 1000).toFixed(1)} 秒`);
  if (usage.model) parts.push(usage.model);
  if (usage.costUsd !== null) parts.push(`$${usage.costUsd.toFixed(4)}`);
  return parts.join(' · ');
}

/**
 * 这一段会话的合计读成一行：调用次数、进出 token、各币种的钱。
 *
 * 读不到那一格时写「花销读不到」：留白会被读成「这段没花」，而事实是这一格
 * 没有读数。没定价的调用次数单列，不让它沉默地消失在金额后面。
 */
function sessionSpendLine(spend: SessionSpend): string {
  if (!spend.readable) return '会话花销读不到';
  const parts: string[] = [];
  if (spend.calls !== null) parts.push(spend.calls + ' 次调用');
  if (spend.inputTokens !== null || spend.outputTokens !== null) {
    const input = spend.inputTokens === null ? '—' : compactCount(spend.inputTokens);
    const output = spend.outputTokens === null ? '—' : compactCount(spend.outputTokens);
    parts.push('输入 ' + input + ' · 输出 ' + output);
  }
  for (const line of spend.money) {
    const times = line.pricedCalls > 1 ? '（' + line.pricedCalls + ' 次）' : '';
    parts.push(moneyText(line.currency, line.costMicros) + times);
  }
  if (spend.unpricedCalls !== null && spend.unpricedCalls > 0) {
    parts.push(spend.unpricedCalls + ' 次未定价');
  }
  if (parts.length === 0) return '会话 0 次调用';
  return '会话 ' + parts.join(' · ');
}

/**
 * 钱按币种写：价目本可能换过，把两种币折成一个和就是编一个汇率。
 *
 * 微单位是价钱的本体（账本里存的就是它），这里只做一次显示层的除法。
 */
function moneyText(currency: string, micros: number): string {
  const amount = micros / 1_000_000;
  if (currency === 'USD') return '$' + amount.toFixed(4);
  if (currency === 'CNY') return '¥' + amount.toFixed(4);
  return amount.toFixed(4) + ' ' + currency;
}

/**
 * 配额那一格读成一行：已用、上限、还剩。
 *
 * 「未配置」与「读不到」是两句不同的话，也都不是「无限」：前者是没人设过界
 * （执法面因此不拒），后者是这次没读到（连已用都没有）。
 */
function quotaLine(quota: ChatQuota | null, unreadable: boolean): string {
  if (unreadable || !quota || !quota.readable) return '配额读不到';
  const used = quota.used_today ?? null;
  const effective = quota.effective ?? null;
  if (effective === null) {
    return used === null ? '配额未配置' : '今日 ' + compactCount(used) + '（未配置上限）';
  }
  const parts: string[] = [];
  if (used !== null) parts.push('今日 ' + compactCount(used));
  parts.push('上限 ' + compactCount(effective.hard_limit));
  parts.push('剩 ' + compactCount(effective.remaining));
  if (effective.blocking) parts.push('已用完');
  else if (effective.warning) parts.push('接近上限');
  return '配额 ' + parts.join(' · ');
}

/**
 * 一次上下文裁剪读成一行：挤掉多少、留下多少、尾巴从哪一句开始。
 *
 * 写的是服务端量出来的数，不在这里估算。「从「…」起」那一段是原文的开头，
 * 不是模型写的摘要——读者据此认出被挤掉的是哪一轮，而这正是这一行存在的理由：
 * 让「模型这一轮没看见那段」在屏幕上说得出口，而不是等它答得奇怪时去猜。
 */
function compactionLine(compaction: ContextCompaction): string {
  const parts: string[] = ['上下文压缩'];
  if (compaction.droppedMessages !== null) {
    const freed =
      compaction.freedTokens === null ? '' : `（约 ${compactCount(compaction.freedTokens)} tokens）`;
    parts.push(`挤掉 ${compaction.droppedMessages} 条${freed}`);
  }
  if (compaction.keptMessages !== null) parts.push(`保留最近 ${compaction.keptMessages} 条`);
  if (compaction.keptFrom) parts.push(`从「${compaction.keptFrom}」起`);
  return parts.join(' · ');
}

/**
 * 一条附件最多多大。
 *
 * override — 一帧的上限是 64 MiB（传输层），而附件在这条路上要以 base64 走，
 * 体积先涨三分之一。留在这个量级是因为它下面装得下手机原图与常规 PDF；真需要
 * 更大的东西，那一份该用 `read_file` 去读，而不是塞进一句话里。
 */
const MAX_ATTACHMENT_BYTES = 16 * 1024 * 1024;

/** 还没发出去、正随草稿走的附件。数据（base64）只活到这一句发出去为止。 */
interface PendingAttachment {
  name: string;
  mimeType: string;
  bytes: number;
  data: string;
}

/** 字节数按人的读法写：一屏几十条里，`1.4 MB` 比 `1468006` 有用。 */
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

/**
 * 读一个文件成一个附件。类型取浏览器认出来的那个（`file.type`）；它认不出时退到
 * 数据 URL 自己的 `data:` 前缀，最后才落到 `application/octet-stream`——猜成
 * `image/png` 会让一张不是图的东西被当成图交给模型。
 */
function readAsAttachment(file: File): Promise<PendingAttachment> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error(file.name));
    reader.onload = () => {
      const result = String(reader.result ?? '');
      const comma = result.indexOf(',');
      const fromUrl = result.startsWith('data:')
        ? result.slice(5, result.indexOf(';'))
        : '';
      resolve({
        name: file.name || '附件',
        mimeType: file.type || fromUrl || 'application/octet-stream',
        bytes: file.size,
        data: comma >= 0 ? result.slice(comma + 1) : '',
      });
    };
    reader.readAsDataURL(file);
  });
}

/**
 * 一条长消息画成附件时，正文先折起来的那条线（字符）。
 *
 * override — 屏幕上的折叠线，人按「一眼就知道这不是一句话」的量级挑的，不随窗口
 * 变；它只决定「折起来还是铺开」，不决定「留下还是丢掉」——原文一个字节都不少，
 * 服务端已经按它自己量出来的阈值把原文落成了文件。
 */
const LONG_MESSAGE_CHARS = 2000;

/**
 * `@` 之后一份文件列表一次画几行。
 *
 * override — 一个下拉的高度。人是在一屏可见的名字里挑，不是在翻页；多出来的行
 * 只会把输入框挤出屏幕。缩窄成「挑不到就继续打字」，下一层目录再按同一个数列。
 */
const MENTION_ROWS = 8;

/**
 * 光标前面那个还没说完的 `@` 名字。
 *
 * 只有「词的起头」才算：邮箱里的那个 `@`（前面紧贴一个字）不该弹出文件列表；
 * 名字里带空白就是这句话已经说到下一词了，也不该再补。返回 `start` 是为了替换
 * 时能把 `@` 连同打了一半的路径一起换掉，而不是只接在光标后面。
 */
export function mentionAt(
  text: string,
  caret: number
): { start: number; dir: string; leaf: string } | null {
  const head = text.slice(0, caret);
  const at = head.lastIndexOf('@');
  if (at < 0) return null;
  if (at > 0 && !/\s/.test(head[at - 1])) return null;
  const partial = head.slice(at + 1);
  if (/\s/.test(partial)) return null;
  const slash = partial.lastIndexOf('/');
  return {
    start: at,
    dir: slash >= 0 ? partial.slice(0, slash + 1) : '',
    leaf: slash >= 0 ? partial.slice(slash + 1) : partial,
  };
}

/**
 * 把一条路径接进草稿：目录补到 `/` 上继续往下打，文件补一个空格收尾。
 *
 * 收尾的空格不是好看——没有它，下一句会连在这条路径后面，成了一条不存在的路径。
 */
export function mentionedPath(entry: { path: string; dir: boolean }): string {
  return `@${entry.path}${entry.dir ? '/' : ' '}`;
}

/**
 * 这一句是不是一条命令：`/名字 要求`。
 *
 * 名字后面必须紧跟空白或行尾——`/usr/local/bin 不见了` 是一句话，`/web-research
 * 查一下` 是一条命令。两者的分别不在开头的那个 `/`，而在名字边界；按「以 `/`
 * 开头就算命令」去分，一句报错会被当成一条命令去执行。
 */
export function commandLine(text: string): { name: string; arguments: string } | null {
  const match = /^\/([A-Za-z0-9_-]+)(?:[ \t]+([\s\S]*))?$/.exec(text.trim());
  if (!match) return null;
  return { name: match[1], arguments: match[2] ?? '' };
}

/**
 * 光标还在第一行第一个词里时，那半截名字；`null` 是「没在打命令」。
 *
 * 只认整句开头的那一个 `/`：一句话中间出现的斜杠（路径、日期、分数）不是在点
 * 命令。名字里出现空白或别的字符就是这一句已经往下写了，列表该收。
 */
export function slashQuery(text: string, caret: number): string | null {
  if (!text.startsWith('/') || caret < 1) return null;
  const word = text.slice(1, caret);
  if (!/^[A-Za-z0-9_-]*$/.test(word)) return null;
  return word;
}

/**
 * 一条长消息：长度、原文落在哪，正文折起来。
 *
 * 服务端对超过窗口的消息只把开头交给循环，原文逐字落在执行器的文件里；这一条
 * 因此不能按普通正文画——那样读到的「全文」其实是开头，而读者不会知道少了什么。
 * 附件那一行写的是**全文**的字符数，正文只是屏幕上的折叠。
 */
function LongMessage({ message }: { message: ChatMessage }) {
  const [open, setOpen] = useState(false);
  const chars = message.attachment?.chars ?? message.text.length;
  return (
    <div className="long-message border-l border-[var(--line)] pl-2">
      <div className="mono flex flex-wrap items-baseline gap-2 text-[10px] text-[var(--muted)]">
        <span style={{ color: 'var(--paper-dim)' }}>长消息 · {compactCount(chars)} 字符</span>
        {message.attachment && (
          <span className="break-all">原文已存成文件 {message.attachment.path}</span>
        )}
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          className="transition-colors hover:text-[var(--paper-dim)]"
        >
          {open ? '收起' : '展开全文'}
        </button>
      </div>
      {/* 正文仍然走同一个渲染器：折叠的是高度，不是这一条话的读法。 */}
      <div className={`mt-1 ${open ? '' : 'max-h-40 overflow-hidden'}`}>
        <Markdown text={message.text} />
      </div>
    </div>
  );
}

function Turn({ message }: { message: ChatMessage }) {
  const [showThinking, setShowThinking] = useState(false);
  const streaming = message.status === 'streaming' || message.status === 'sending';
  const who = message.role === 'user' ? '你' : '执行者';

  return (
    <article className="group relative pl-4">
      <span
        aria-hidden
        className="chat-turn-rule absolute left-0 top-1 bottom-1 w-px"
        data-streaming={streaming}
      />
      <header className="mb-1 flex items-baseline gap-2">
        <span
          className="mono text-[11px]"
          style={{ color: message.role === 'user' ? 'var(--brass)' : 'var(--live)' }}
        >
          {who}
        </span>
        <span className="mono text-[10px] text-[var(--muted)]">{stamp(message.at)}</span>
        {message.status === 'sending' && (
          <span className="text-[10px] text-[var(--muted)]">已发出，等待回声</span>
        )}
        {message.failure && (
          <span className="text-[10px]" style={{ color: 'var(--warn)' }}>
            {message.failure}
          </span>
        )}
      </header>

      {message.thinking && (
        <div className="mb-1">
          <button
            type="button"
            onClick={() => setShowThinking((v) => !v)}
            className="text-[11px] text-[var(--muted)] transition-colors hover:text-[var(--paper-dim)]"
          >
            {showThinking ? '收起思考' : '思考过程'}
          </button>
          {showThinking && (
            <p className="mt-1 whitespace-pre-wrap border-l border-[var(--line)] pl-2 text-[11px] leading-[1.6] text-[var(--muted)]">
              {message.thinking}
            </p>
          )}
        </div>
      )}

      {/*
        一条长得不像一句话的消息画成附件：服务端把原文落成了文件，这一条就不该
        铺成正文——铺开来是一整屏，读的人还会以为屏幕上这段就是全部。界面按正文
        长度认出读回历史里的那几条（记录里没有附件那一格），按附件那一格认出刚
        发出去的这几条。
      */}
      {message.attachment || (message.role === 'user' && message.text.length > LONG_MESSAGE_CHARS) ? (
        <LongMessage message={message} />
      ) : (
        /*
          正文按 Markdown 画：回复里带的是列表、代码块和 diff，按纯文本铺出来，
          读者要自己对齐正负号、自己认哪一段是代码。两边的颜色仍然分开——「你」说
          的是纸色，回复略暗一档。
        */
        <div style={{ color: message.role === 'user' ? 'var(--paper)' : 'var(--paper-dim)' }}>
          <Markdown text={message.text} streaming={streaming} />
        </div>
      )}

      {/* 随这一句发上去的附件。它们不是正文的一部分，但少了这一行，事后没人
          分得清「问了这么一句」与「问了这么一句、还带了一张图」。 */}
      {message.files.length > 0 && (
        <div className="mono mt-1 flex flex-wrap gap-2 text-[10px] text-[var(--muted)]">
          {message.files.map((file, index) => (
            <span key={`${file.name}-${index}`}>
              〔{file.mimeType.split('/')[0]}〕{file.name}
              {/* 读回的历史里没有字节数：这一条只写名字，不写一个零。 */}
              {file.bytes !== null && <> · {formatBytes(file.bytes)}</>}
            </span>
          ))}
        </div>
      )}

      {/*
        这一轮把一段早期上下文挤出了提示词。它排在花销上面：先读「模型这一轮
        少看到了什么」，再读「这一轮花了多少」——两件事都在这一条消息下面。
      */}
      {message.role === 'assistant' && message.compaction && (
        <p className="mono mt-1 text-[10px] text-[var(--brass)]">
          {compactionLine(message.compaction)}
        </p>
      )}

      {/*
        这一轮开头读进了哪几句属主约定。排在花销上面：约定的原文是这条回答的
        前提，读的人先看到它，才看得懂回答为什么长这样。一句也没读到时写
        「一句约定都没读到」而不是不画——「没写过」与「平台不读」要分得开，
        后者根本不会有这一帧。
      */}
      {message.role === 'assistant' && message.memory && (
        <p className="mono mt-1 text-[10px] text-[var(--muted)]">
          {message.memory.length === 0
            ? '这一轮没有读进属主约定'
            : `读进 ${message.memory.length} 句属主约定：${message.memory
                .map((entry) => entry.text)
                .join('；')}`}
        </p>
      )}

      {/*
        这一轮的外部 MCP 服务。连不上时把原因写出来：那一步的动作是去查那台
        服务，而屏幕上只留「没有这一帧」的话，读的人分不出「没挂」与「断了」。
      */}
      {message.role === 'assistant' && message.mcp && (
        <p className="mono mt-1 text-[10px] text-[var(--muted)]">
          {message.mcp.reachable
            ? `外部 MCP 提供 ${message.mcp.tools.length} 只工具${
                message.mcp.tools.length > 0 ? `：${message.mcp.tools.join('、')}` : ''
              }`
            : `外部 MCP 连不上${message.mcp.error ? `：${message.mcp.error}` : ''}`}
        </p>
      )}

      {/*
        这一轮花了多少。只有服务端真的量到了才有一行：读回的历史与被叫停的那
        一轮没有这一格，那时屏幕上什么也不写，而不是写一排零。
      */}
      {message.role === 'assistant' && message.usage && (
        <p className="mono mt-1 text-[10px] text-[var(--muted)]">{usageLine(message.usage)}</p>
      )}
    </article>
  );
}

/**
 * 一次工具调用在对话流里的样子。
 *
 * 不是一行日志：它是这一段对话里真的发生过的一件事，要能展开看它传了什么、回了
 * 什么。收起来时只留一行——工具名、哪个执行者、成没成；展开时给原文。默认收起是
 * 因为一次运行能调几十次，全展开就把回复挤到屏幕外面去了。
 */
function ToolTurn({ call }: { call: ToolCallRecord }) {
  const [open, setOpen] = useState(false);
  /** 改动类工具折成的那份 diff；别的调用是 `null`（见 `toolDiff`）。 */
  const diff = useMemo(() => toolDiff(call), [call]);
  const tone =
    call.status === 'error' ? 'var(--danger)' : call.status === 'running' ? 'var(--brass)' : 'var(--ok)';
  const label = call.status === 'error' ? '失败' : call.status === 'running' ? '进行中' : '返回';
  return (
    <article className="group relative pl-4">
      <span aria-hidden className="absolute left-0 top-1 bottom-1 w-px" style={{ background: 'var(--line)' }} />
      <Note
        as="button"
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-baseline gap-2 text-left"
        text="展开这次调用的入参与返回"
      >
        <span className="mono text-[11px]" style={{ color: 'var(--paper-dim)' }}>
          工具
        </span>
        <span className="mono text-[11px]" style={{ color: 'var(--paper)' }}>
          {call.name || '未命名'}
        </span>
        <span className="mono text-[10px] text-[var(--muted)]">
          {call.agent ? call.agent.slice(0, 8) : '未知执行者'}
        </span>
        <span className="mono text-[10px]" style={{ color: tone }}>
          {label}
        </span>
        <span className="mono ml-auto text-[10px] text-[var(--muted)]">{stamp(call.startedAt)}</span>
      </Note>
      {/*
        改动类工具直接在流里画那一份 diff，不用先展开：这一行最要紧的信息是「改了
        哪几行」，而它在入参与返回那两份 JSON 里只有自己找。展开仍然是原文——diff
        是给眼睛的，原文是给核对的。
      */}
      {diff && (
        <div className="tool-diff mt-1 border-l border-[var(--line)] pl-2">
          <div className="mono flex flex-wrap items-baseline gap-2 text-[10px] text-[var(--muted)]">
            {diff.path && (
              <span className="break-all" style={{ color: 'var(--paper-dim)' }}>
                {diff.path}
              </span>
            )}
            <span>{diff.label}</span>
            <span style={{ color: 'var(--ok)' }}>+{diff.added}</span>
            <span style={{ color: 'var(--danger)' }}>−{diff.removed}</span>
            {call.status === 'running' && <span>调用还没回来</span>}
          </div>
          <pre className="mono mt-1 max-h-64 overflow-auto text-[11px] leading-[1.5]">
            {diff.lines.map((line, index) => (
              <span key={index} className={`diff-${line.kind}`}>
                {line.kind === 'add' ? '+' : line.kind === 'del' ? '−' : ' '}
                {line.text || ' '}
                {'\n'}
              </span>
            ))}
          </pre>
        </div>
      )}
      {open && (
        <div className="mt-1 flex flex-col gap-1 border-l border-[var(--line)] pl-2">
          <div>
            <div className="text-[10px] uppercase tracking-[0.12em] text-[var(--muted)]">入参</div>
            <pre className="mono max-h-48 overflow-auto whitespace-pre-wrap break-all text-[11px] text-[var(--paper-dim)]">
              {call.args || '（没有入参）'}
            </pre>
          </div>
          {call.progress && (
            <div>
              <div className="text-[10px] uppercase tracking-[0.12em] text-[var(--muted)]">中途输出</div>
              <pre className="mono max-h-48 overflow-auto whitespace-pre-wrap break-all text-[11px] text-[var(--muted)]">
                {call.progress}
              </pre>
            </div>
          )}
          <div>
            <div className="text-[10px] uppercase tracking-[0.12em] text-[var(--muted)]">返回</div>
            <pre
              className="mono max-h-64 overflow-auto whitespace-pre-wrap break-all text-[11px]"
              style={{ color: call.status === 'error' ? 'var(--danger)' : 'var(--paper-dim)' }}
            >
              {call.result || (call.status === 'running' ? '还在跑' : '（空）')}
            </pre>
          </div>
        </div>
      )}
    </article>
  );
}

/**
 * 一个执行者说的话。
 *
 * 与「你」和「这段对话的回复」分开画：它是目标下面的某一个人在汇报，不是平台在
 * 回答你。混成一个样子，读者会以为这些字是说给自己听的，于是把「第 3 轮用了
 * read_file」当成一次正式答复。
 */
function AgentTurn({ turn }: { turn: { agent: string; text: string; at: number } }) {
  const [open, setOpen] = useState(false);
  const long = turn.text.length > 400;
  const shown = long && !open ? `${turn.text.slice(0, 400)}…` : turn.text;
  return (
    <article className="group relative pl-4">
      <span
        aria-hidden
        className="absolute left-0 top-1 bottom-1 w-px"
        style={{ background: 'var(--line)' }}
      />
      <header className="mb-1 flex items-baseline gap-2">
        <span className="mono text-[11px]" style={{ color: 'var(--live)' }}>
          执行者 {turn.agent.slice(0, 8)}
        </span>
        <span className="mono text-[10px] text-[var(--muted)]">
          {new Date(turn.at).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })}
        </span>
        {long && (
          <button
            type="button"
            onClick={() => setOpen((v) => !v)}
            className="text-[10px] text-[var(--muted)] transition-colors hover:text-[var(--paper-dim)]"
          >
            {open ? '收起' : `全文（${turn.text.length} 字）`}
          </button>
        )}
      </header>
      <p className="whitespace-pre-wrap break-words text-[12px] leading-[1.7] text-[var(--paper-dim)]">
        {shown}
      </p>
    </article>
  );
}

/**
 * 这段对话是哪种：关掉就断的一问一答，还是已经交给守护接着做的活。
 *
 * 判的是它有没有把目标交出去，不是用户勾了什么：勾选是意愿，屏幕上要说的是这件事
 * 现在有没有人接着做。
 */
function KindBadge({ session, live }: { session: ChatSession; live: GoalLive[] }) {
  const kind = sessionKind(session);
  if (kind === 'chat') {
    return (
      <Note
        className="shrink-0 border border-[var(--line)] px-1.5 py-0.5 text-[10px] text-[var(--muted)]"
        text="一问一答：回复只走当前这条连接。关掉这一段，这一轮就没有下文了——说过的话仍然记在历史里。"
      >
        可下线
      </Note>
    );
  }
  const running = live.reduce((sum, goal) => sum + goal.running, 0);
  const total = live.reduce((sum, goal) => sum + goal.total, 0);
  const done = live.reduce((sum, goal) => sum + goal.completed, 0);
  const failed = live.reduce((sum, goal) => sum + goal.failed, 0);
  return (
    <Note
      className="flex shrink-0 items-baseline gap-1.5 border px-1.5 py-0.5 text-[10px]"
      style={{ borderColor: 'var(--ok)', color: 'var(--ok)' }}
      text={
        '已交给编排器：任务由分解、调度与监督接管，关掉这一段它继续跑，回来接得上。' +
        (total > 0 ? `这道目标下 ${total} 个任务，${done} 个收口、${running} 个在跑。` : '还在分解，子任务尚未落地。')
      }
    >
      <span>常驻 · 守护</span>
      {total > 0 && (
        <span className="mono text-[var(--muted)]">
          {done}/{total}
          {running > 0 ? ` · 跑 ${running}` : ''}
          {failed > 0 ? ` · 失败 ${failed}` : ''}
        </span>
      )}
    </Note>
  );
}

export interface ConversationPaneProps {
  sessionId: string;
  /**
   * 这一段对话的叫法。由工作台统一分配——同时开着哪几段决定谁叫什么，
   * 单独一格算不出「和邻居不重名」这件事。
   */
  name: string;
  active: boolean;
  onFocus: () => void;
  onClose: () => void;
  clientRef: React.RefObject<StreamClient | null>;
  /** 只剩一段时不给关：关掉最后一段对话，面上就没有落点了。 */
  closable: boolean;
  /**
   * 去不去读回历史。演示模式没有后端，读一个不存在的接口只会让每一段对话的
   * 顶上挂一条「历史没读到」——那是把「没有这个功能」和「这个功能坏了」画成
   * 同一个样子。
   */
  historyEnabled?: boolean;
  /**
   * 平台这一拍的流程总览折成的目标表（按 `goal_id`）。
   *
   * 这不是给对话面凑一列数字：它决定这一段对话是不是「常驻」，也决定工具调用该
   * 归到谁名下。没有它的时候两件事都只能猜，而猜出来的「守护中」比不显示更坏。
   */
  goalLive?: Record<string, GoalLive>;
}

export function ConversationPane({
  sessionId,
  name,
  active,
  onFocus,
  onClose,
  clientRef,
  closable,
  historyEnabled = true,
  goalLive,
}: ConversationPaneProps) {
  const session = useChatStore((s) => s.sessions[sessionId]);
  const appendUserTurn = useChatStore((s) => s.appendUserTurn);
  const hydrate = useChatStore((s) => s.hydrate);
  /** 把账上的某一行打开。这一面只知道身份，落到哪一行由账那一面决定。 */
  const focusRecord = useShell((s) => s.focusRecord);
  const [draft, setDraft] = useState('');
  /** 草稿那一格。它要按内容长高，而长高只能问它自己滚出来的高度。 */
  const draftRef = useRef<HTMLTextAreaElement | null>(null);
  // 一行高固定住的话，第二行会藏进框里、光标跑到看不见的地方。每次改动先清成
  // 自动再收到实际高度，删字时框要跟着缩回去，不能只长不缩。
  useEffect(() => {
    const el = draftRef.current;
    if (!el) return;
    el.style.height = 'auto';
    el.style.height = `${el.scrollHeight}px`;
  }, [draft]);
  /** 正随草稿走的附件，以及认不出来的那一个为什么不进来。 */
  const [pendingFiles, setPendingFiles] = useState<PendingAttachment[]>([]);
  const [attachError, setAttachError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  /**
   * 正在补的那个 `@`：替换从哪儿开始、列的是哪一层的文件、挑到第几行。
   *
   * `null` 是「没有在补」。列表本身跟着目录走：同一层里继续打字只筛已有的那几
   * 行，换一层才再问一次服务端——一个字一个请求，是把文件树当成输入法在用。
   */
  const [mention, setMention] = useState<{
    query: { start: number; dir: string; leaf: string };
    entries: WorkspaceEntry[];
    index: number;
    error: string | null;
  } | null>(null);
  const mentionDir = mention?.query.dir;

  /**
   * 正在打的那条命令（`/` 开头、光标还在名字里）。
   *
   * 名单每次都向服务端要：技能目录是热加载的，人刚放进去的一份说明应当在下一
   * 次打 `/` 时就看得见。`error` 与「候选是空的」分开——前者是没读到，后者是读到
   * 了但没有这个名字。
   */
  const [slash, setSlash] = useState<{
    query: string;
    index: number;
    list: ChatCommand[];
    note?: string;
    error: string | null;
    /** 名单到手了没有。「还没读到」与「一条都没有」在屏幕上要分得开。 */
    loaded: boolean;
  } | null>(null);
  const slashOpen = slash !== null;

  /**
   * 把选来 / 贴进来的文件收成附件。
   *
   * 太大的那一个不进队列，也不再静默丢掉：一句「带不动」当场写在输入框上方，
   * 而不是让用户以为它跟着消息走了。
   */
  const attach = useCallback(async (files: FileList | File[] | null) => {
    if (!files || files.length === 0) return;
    const accepted: PendingAttachment[] = [];
    let error: string | null = null;
    for (const file of Array.from(files)) {
      if (file.size > MAX_ATTACHMENT_BYTES) {
        error = `「${file.name}」有 ${formatBytes(file.size)}，一帧带不动：单个附件上限是 ${formatBytes(MAX_ATTACHMENT_BYTES)}`;
        continue;
      }
      try {
        accepted.push(await readAsAttachment(file));
      } catch {
        error = `「${file.name}」读不出来`;
      }
    }
    if (accepted.length > 0) {
      setPendingFiles((prev) => [...prev, ...accepted]);
      setAttachError(null);
    }
    if (error) setAttachError(error);
  }, []);

  /**
   * 列一层目录给 `@` 用。走的是项目源码面那棵树：和员工自己读的是同一份仓库。
   *
   * 读不到就把原因写在列表里，不画成「没有匹配的文件」——那个部署可能只是没挂
   * 项目仓库，这时「没有文件」是一句假话，而用户会以为自己的路径打错了。
   */
  useEffect(() => {
    if (mentionDir === undefined) return;
    let live = true;
    getWorkspaceTree(mentionDir.replace(/\/$/, ''), 'project')
      .then((tree) => {
        if (!live) return;
        setMention((prev) =>
          prev && prev.query.dir === mentionDir
            ? { ...prev, entries: tree.entries, index: 0, error: null }
            : prev
        );
      })
      .catch((e: unknown) => {
        if (!live) return;
        setMention((prev) =>
          prev && prev.query.dir === mentionDir
            ? {
                ...prev,
                entries: [],
                index: 0,
                error: (e as { message?: string })?.message ?? '读不到文件列表',
              }
            : prev
        );
      });
    return () => {
      live = false;
    };
  }, [mentionDir]);

  /** 把挑中的那一条接进草稿，光标放到它后面。 */
  const acceptMention = useCallback(
    (entry: WorkspaceEntry) => {
      if (!mention) return;
      const caret = draftRef.current?.selectionStart ?? draft.length;
      const insert = mentionedPath(entry);
      setDraft(
        draft.slice(0, mention.query.start) + insert + draft.slice(caret)
      );
      setMention(null);
      // 光标落在这条路径之后：接着往下打时，补全还能从这一层继续。
      const at = mention.query.start + insert.length;
      requestAnimationFrame(() => {
        const el = draftRef.current;
        if (!el) return;
        el.focus();
        el.setSelectionRange(at, at);
      });
    },
    [draft, mention]
  );

  useEffect(() => {
    if (!slashOpen) return;
    let live = true;
    getChatCommands()
      .then((answer) => {
        if (!live) return;
        setSlash((prev) =>
          prev
            ? { ...prev, list: answer.commands, note: answer.note, error: null, loaded: true }
            : prev
        );
      })
      .catch((e) => {
        if (!live) return;
        setSlash((prev) =>
          prev
            ? {
                ...prev,
                list: [],
                error: (e as { message?: string })?.message ?? '读不到命令名单',
                loaded: true,
              }
            : prev
        );
      });
    return () => {
      live = false;
    };
  }, [slashOpen]);

  /** 把挑中的那一条接进草稿：只补名字与一个空格，要求由人接着打。 */
  const acceptSlash = useCallback(
    (command: ChatCommand) => {
      const head = '/' + command.name + ' ';
      setDraft(head);
      setSlash(null);
      requestAnimationFrame(() => {
        const el = draftRef.current;
        if (!el) return;
        el.focus();
        el.setSelectionRange(head.length, head.length);
      });
    },
    []
  );

  /** 名字以打了一半的那几个字符起头的命令。 */
  const slashCandidates = useMemo(() => {
    if (!slash) return [];
    const query = slash.query.toLowerCase();
    return slash.list
      .filter((command) => command.name.toLowerCase().startsWith(query))
      .slice(0, MENTION_ROWS);
  }, [slash]);

  /** 这一层里以打了一半的名字起头的那几行。目录与文件一起列：先打得进去，再挑内容。 */
  const mentionCandidates = useMemo(() => {
    if (!mention) return [];
    const leaf = mention.query.leaf.toLowerCase();
    return mention.entries
      .filter((entry) => entry.name.toLowerCase().startsWith(leaf))
      .slice(0, MENTION_ROWS);
  }, [mention]);

  /**
   * 等着这一段答的「要不要动手」。
   *
   * 按会话筛，不按连接筛：同一段对话可以有几问同时在等（用户连发两句），而它们
   * 各自按请求号回去，抢不得。这一段已经停掉的那一轮不会有问留下——服务端在连接
   * 与那一轮一起消失时自己拒绝。
   */
  const allPermissionAsks = usePermissionStore((s) => s.asks);
  const permissionAsks = useMemo(
    () => allPermissionAsks.filter((ask) => ask.sessionId === sessionId),
    [allPermissionAsks, sessionId]
  );
  const permissionMode = usePermissionStore((s) => s.modes[sessionId] ?? DEFAULT_PERMISSION_MODE);
  /**
   * 这个人留下来的规则。`null` 是「还没读到」，不是「一条也没有」——两句话在
   * 这一面写出来就是两句不同的字，见下面那块规则面板。
   */
  const permissionRules = usePermissionStore((s) => s.rules);
  const [rulesOpen, setRulesOpen] = useState(false);
  /**
   * 这个人写下的约定：下一轮开头自动进提示词的那几句。
   *
   * `null` 是「还没读到」，空数组是「读到了，一句也没写」，与规则那份名单同一个
   * 规矩。草稿另存一格：写到一半的句子不该进任何一份账。
   */
  const memoryEntries = useMemoryStore((s) => s.entries);
  const [memoryOpen, setMemoryOpen] = useState(false);
  const [memoryDraft, setMemoryDraft] = useState('');
  const scrollerRef = useRef<HTMLDivElement>(null);
  const stickRef = useRef(true);
  /** 这一段交出去的目标，按总览折出来的现状。总览里还没有的那几道如实缺席。 */
  const live = useMemo(
    () => (session?.goals ?? []).map((goal) => goalLive?.[goal.id]).filter(Boolean) as GoalLive[],
    [goalLive, session?.goals]
  );
  const toolCalls = useStreamStore((s) => s.toolCalls);
  const agentMessages = useStreamStore((s) => s.messages);
  /**
   * 这一段自己的执行者调过的工具，按时间接回对话里。
   *
   * 一道目标一个人接：先交出去的那道从自己的时刻起算，后交出去的各自从自己的时刻
   * 起算——同一段对话交过两次活时，第二次不该把第一次的调用也算成自己的。
   */
  const tools = useMemo(() => {
    const goals = session?.goals ?? [];
    const collected: ToolCallRecord[] = [];
    // 这一段自己的员工做的事，先接回来。
    //
    // 下面那一支认的是「这道活派给过谁」，只覆盖交出去以后由执行者接着干的部分。
    // 而一段对话在自己手里的时候（还没交出任何目标，或者这一问本来就是一句话的
    // 活）它是自己在调工具的——那些帧带着它的 `agent_id` 落在同一条总线上，却
    // 一条都没有进过这一面：屏幕上只有一问一答，看起来像它一个工具都没用。
    // 身份由服务端的 `agent.identity` 帧带进来，前端算不出这个 id（名字里带进程
    // 指纹），所以没有那一帧时这里什么也接不回来，而不是接错；重开页面时那一帧
    // 不会重发，回答者改由记录里的 `agent_id` 补回来（见 `hydrate`）。
    //
    // 另一条路是会话号：从记录里重放回来的调用带着自己在哪一段，回答者那一格
    // 空着的老轮也能靠它放回这一段下面——否则重开之后待办面板就空了。
    const own = session?.agentId;
    for (const call of toolCalls) {
      const mine = (own && call.agent === own) || (call.session && call.session === sessionId);
      if (!mine) continue;
      if (!collected.some((seen) => seen.id === call.id)) collected.push(call);
    }
    for (const goal of goals) {
      for (const call of callsForGoal(toolCalls, goalLive?.[goal.id], goal.at)) {
        if (!collected.some((seen) => seen.id === call.id)) collected.push(call);
      }
    }
    return collected.sort((a, b) => Date.parse(a.startedAt) - Date.parse(b.startedAt));
  }, [goalLive, session?.agentId, session?.goals, sessionId, toolCalls]);
  /**
   * 这道目标名下的执行者自己说的话。
   *
   * 与上面的工具调用同一条归属规则：按「这道活派给过谁」加「什么时候交出去的」
   * 一起判。它们是执行者的正文，不是这段对话的回复——混成一个样子，读者会以为
   * 平台在直接回答他。所以单独一支、标着谁说的。
   */
  const agentTurns = useMemo(() => {
    const goals = session?.goals ?? [];
    const out: { id: string; agent: string; text: string; at: number }[] = [];
    for (const goal of goals) {
      const goalState = goalLive?.[goal.id];
      if (!goalState || goalState.agents.length === 0) continue;
      const agents = new Set(goalState.agents);
      const from = Date.parse(goal.at);
      for (const message of agentMessages) {
        if (!message.agent || !agents.has(message.agent)) continue;
        // 消息 id 形如 `<agent>:<iso 时刻>`：那一刻是这条消息开始流的时候。
        const at = Date.parse(message.id.split(':').slice(1).join(':'));
        if (Number.isFinite(from) && Number.isFinite(at) && at < from) continue;
        if (out.some((seen) => seen.id === message.id)) continue;
        out.push({ id: message.id, agent: message.agent, text: message.text, at });
      }
    }
    return out;
  }, [agentMessages, goalLive, session?.goals]);
  /**
   * 这一段手上那张待办单，按写者分开，一位一张。
   *
   * 模型每一步都把整张单子重写一遍，所以多次 `todo` 调用是同一张单子的几个版本。
   * 折成一块面板之后，下面时间线里那些调用本身不再各占一行——画五遍同一份清单，
   * 读者还得自己找哪份最新。
   */
  const todoLists = useMemo(() => todoListsIn(tools), [tools]);
  /** 说与做并成一条时间线。时刻读不出来的排在最前——宁可前也不能藏。 */
  const timeline = useMemo(() => {
    type Item =
      | { kind: 'tool'; at: number; call: ToolCallRecord }
      | { kind: 'agent'; at: number; agent: { id: string; agent: string; text: string; at: number } }
      | { kind: 'message'; at: number; message: ChatMessage };
    const items: Item[] = (session?.messages ?? []).map((message) => ({
      kind: 'message' as const,
      at: Date.parse(message.at),
      message,
    }));
    for (const call of tools) {
      // 待办不进时间线：它的现况画在下面那块面板里，一次调用一行原文只会重复。
      // 认不出来的（没有结果、也没有可读入参）那一次仍然照常出现，见 TodoPanel。
      if (call.name === 'todo') continue;
      items.push({ kind: 'tool' as const, at: Date.parse(call.startedAt), call });
    }
    for (const turn of agentTurns) {
      items.push({ kind: 'agent' as const, at: turn.at, agent: turn });
    }
    const rank = (at: number) => (Number.isFinite(at) ? at : 0);
    return items.sort((a, b) => rank(a.at) - rank(b.at));
  }, [session?.messages, tools, agentTurns]);

  // 打开就先把历史读回来。读过的不再读：重读会把刚流出来的这一段冲掉。
  useEffect(() => {
    if (historyEnabled) void hydrate(sessionId);
  }, [hydrate, historyEnabled, sessionId]);

  // 只在读者本来就贴着底的时候跟着滚。正在往回翻的时候被拽回底部，是对话界面
  // 最容易被骂的一个动作。
  useEffect(() => {
    const el = scrollerRef.current;
    if (!el || !stickRef.current) return;
    el.scrollTop = el.scrollHeight;
  }, [session?.messages, tools.length]);

  const onScroll = useCallback(() => {
    const el = scrollerRef.current;
    if (!el) return;
    stickRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 48;
  }, []);

  const send = useCallback(() => {
    const text = draft.trim();
    // 带附件的一句可以没有正文：一张截图本身就是那半句话。两样都空才是没说话。
    if (!text && pendingFiles.length === 0) return;
    setDraft('');
    const files = pendingFiles;
    setPendingFiles([]);
    setAttachError(null);
    // 先入账，再出门。顺序反过来时，网络慢一拍、或者这个部署的回声路径断了，
    // 用户看到的就是「我说话了，但这里什么都没发生」。
    appendUserTurn(
      sessionId,
      text,
      files.map((file) => ({ name: file.name, mimeType: file.mimeType, bytes: file.bytes }))
    );
    stickRef.current = true;

    const client = clientRef.current;
    if (!client) return;
    // 线上形状按服务端那一边的名字写：`mime_type` 是那条协议的语言，不是这里的。
    const attachments = files.map((file) => ({
      name: file.name,
      mime_type: file.mimeType,
      data: file.data,
    }));
    // 一条命令与一句话走两条帧：服务端不必按开头的 `/` 去猜，一段以 `/usr/...`
    // 开头的报错也就不会被当成一条命令去执行。
    const command = commandLine(text);
    if ('simulateUserMessage' in client) {
      client.setSession?.(sessionId);
      client.simulateUserMessage(text);
    } else if (command) {
      client.send({
        type: 'command',
        session_id: sessionId,
        name: command.name,
        arguments: command.arguments,
        attachments,
      });
    } else {
      client.send({
        type: 'chat_message',
        session_id: sessionId,
        content: text,
        attachments,
      });
    }
  }, [appendUserTurn, clientRef, draft, pendingFiles, sessionId]);

  /**
   * 停下这一段上正在跑的那一轮。
   *
   * 界面上不做本地拦截：停了没有，由服务端那一帧说了算（`message.cancelled`
   * 之后紧跟收尾帧）。本地先把光标收掉的话，一次没送到的停止会看起来像停下了，
   * 而模型那边还在说——屏幕和账从此不是一件事。
   */
  const stop = useCallback(() => {
    clientRef.current?.send({ type: 'chat_cancel', session_id: sessionId });
  }, [clientRef, sessionId]);

  const streaming = Boolean(session?.streaming);

  /**
   * 会话头那一格上下文占用：最近一条带着用量的消息，它最后一次调用读进去的
   * 提示词长度。没有它就是没读到过（刷新读回的历史不带花销），那一格不出现。
   */
  const contextTokens = useMemo(() => {
    const messages = session?.messages ?? [];
    for (let i = messages.length - 1; i >= 0; i -= 1) {
      const seen = messages[i].usage?.contextTokens;
      if (seen !== null && seen !== undefined) return seen;
    }
    return null;
  }, [session?.messages]);

  /**
   * 会话头那一格「这一段花了多少」：最近一条带着会话合计的消息。
   *
   * 服务端从账本按会话折出来，界面一条都不加——这里再累加一遍就有了两本账，
   * 而两本账第一次对不上时没人说得出哪一本是对的。刷新读回的历史不带花销，
   * 那一格因此会消失，而不是留着上一次的数冒充现在。
   */
  const sessionSpend = useMemo(() => {
    const messages = session?.messages ?? [];
    for (let i = messages.length - 1; i >= 0; i -= 1) {
      const seen = messages[i].usage?.session;
      if (seen) return seen;
    }
    return null;
  }, [session?.messages]);

  /**
   * 配额余量：这一段的花销变了就重读一次。
   *
   * 不按定时器刷：这里要的是「刚花完的那些之后还剩多少」，没有新花销就没有新
   * 答案，定时器只会把同一个数读第二遍。
   *
   * 触发点取会话合计里的那几个数，不取消息条数：一条消息在轮子开始时就挂上去
   * 了，那时这一轮还没记账，按条数触发读到的是上一轮的余量，而真正记账的那一
   * 帧不会再触发一次——头行的「剩多少」于是永远晚一轮，且看起来完全正常。
   */
  const [quota, setQuota] = useState<ChatQuota | null>(null);
  const [quotaUnreadable, setQuotaUnreadable] = useState(false);
  const spendTouched = sessionSpend
    ? `${sessionSpend.calls}:${sessionSpend.inputTokens}:${sessionSpend.outputTokens}`
    : '';
  useEffect(() => {
    let live = true;
    getChatQuota()
      .then((reading) => {
        if (!live) return;
        setQuota(reading);
        setQuotaUnreadable(false);
      })
      .catch(() => {
        if (!live) return;
        setQuota(null);
        setQuotaUnreadable(true);
      });
    return () => {
      live = false;
    };
  }, [sessionId, spendTouched]);

  /**
   * Esc 与「■ 停止」走同一条路：只在有轮在跑时挂上，停的是这一段上所有在跑的轮。
   *
   * 不避让输入框——Esc 在这里没有第二种含义，而「手还在键盘上、想让它停下」正是
   * 这个键存在的理由。监听挂在 window 上而不是这一面的元素上：焦点多半就在那个
   * 输入框里，元素级监听会在刚说完一句、还没点别处的时候收不到。
   */
  useEffect(() => {
    if (!streaming) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      event.preventDefault();
      stop();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [streaming, stop]);

  if (!session) return null;

  /**
   * 答一次「要不要动手」。
   *
   * 账上的那一行先摘掉再发：答案没送到时服务端仍然是拒绝（连接消失那一头自己
   * 拒绝），所以摘掉不会让一次执行悄悄发生。反过来留着它，屏幕上就一直挂着一个
   * 已经不存在的问。
   */
  const answerPermission = (ask: PermissionAsk, allow: boolean, remember = false) => {
    clientRef.current?.send({
      type: 'permission_response',
      request_id: ask.id,
      allow,
      remember,
    });
    usePermissionStore.getState().answered(ask.id);
  };

  /**
   * 规则面板：第一次打开时向服务端要一份名单，之后每一次撤销的回执都是一整份新名单。
   *
   * 不在挂载时就问：一个人可能从来不开这块面板，而每一次连上来都问一次名单，是把
   * 一件没人看的东西算进每一次连接的开销。
   */
  const toggleRules = () => {
    const next = !rulesOpen;
    setRulesOpen(next);
    if (next) clientRef.current?.send({ type: 'permission_rules', session_id: sessionId });
  };

  /**
   * 约定面板：第一次打开时向服务端要一份名单。与规则面板同一个理由——不开就
   * 不问，别把没人看的东西算进每一次连接的开销。
   */
  const toggleMemory = () => {
    const next = !memoryOpen;
    setMemoryOpen(next);
    if (next) clientRef.current?.send({ type: 'owner_memory' });
  };

  /** 记下一句。回执是记完之后的整份名单，所以这里不自己改本地那一份。 */
  const writeMemory = () => {
    const text = memoryDraft.trim();
    if (!text) return;
    clientRef.current?.send({ type: 'owner_memory_write', text });
    setMemoryDraft('');
  };

  /** 撤掉一句。回执同样是整份名单。 */
  const forgetMemory = (id: string) => {
    clientRef.current?.send({ type: 'owner_memory_forget', id });
  };

  /** 撤掉一条。回执是撤完之后的整份名单，所以这里不自己改本地那一份。 */
  const revokeRule = (id: string) => {
    clientRef.current?.send({
      type: 'permission_rule_revoke',
      session_id: sessionId,
      rule_id: id,
    });
  };

  /** 换这一段对话的模式。名字是闭集里的那一格，服务端不认识时按更严的一格处理。 */
  const changePermissionMode = (next: string) => {
    usePermissionStore.getState().setMode(sessionId, next);
    clientRef.current?.send({ type: 'permission_mode', session_id: sessionId, mode: next });
  };

  return (
    <section
      onMouseDown={onFocus}
      className="relative flex min-h-0 min-w-0 flex-1 flex-col"
      style={{
        borderLeft: active ? '1px solid var(--line-strong)' : '1px solid transparent',
      }}
      aria-label={`对话 ${name}`}
    >
      <header className="flex shrink-0 items-center gap-2 px-4 pt-3 pb-2">
        <span
          aria-hidden
          className="h-1.5 w-1.5 rounded-full"
          style={{
            background: streaming ? 'var(--brass)' : 'var(--muted)',
            boxShadow: streaming ? '0 0 8px var(--brass)' : 'none',
          }}
        />
        {/*
          三行都写「新的对话」时没法指认谁是谁。名字由工作台统一分给这一段，所以
          同时开着的几段不会重名；后面那截才是这次聊的内容。
        */}
        <span className="mono shrink-0 text-[12px] font-medium" style={{ color: 'var(--brass)' }}>
          {name}
        </span>
        {/*
          「Ada」是这一面的叫法，员工名册上那个人叫的是另一个名字（由会话 id 折
          出来）。两个名字之间没有一行字连起来，读的人会以为是两个人——把总线上的
          那个名字写在这里，名册上按它找得到同一段对话。
        */}
        {session.agentId && (
          <Note
            className="mono max-w-[42%] shrink-0 truncate text-[10px] text-[var(--muted)]"
            text={`这一段对话在总线上叫 ${session.agentId}；员工名册上按这个名字找得到同一个人`}
          >
            {session.agentId}
          </Note>
        )}
        <Note as="h2" className="truncate text-[12px] text-[var(--paper-dim)]" text={session.id}>
          {session.title}
        </Note>
        <KindBadge session={session} live={live} />
        {contextTokens !== null && (
          <Note
            className="mono shrink-0 text-[10px] text-[var(--muted)]"
            text="这一轮结束时模型读进去的提示词长度：这段对话现在背着多重的上下文"
          >
            上下文 {compactCount(contextTokens)}
          </Note>
        )}
        {sessionSpend && (
          <Note
            className="mono shrink-0 text-[10px] text-[var(--muted)]"
            text="账本按这一段会话折出来的合计：它下面每一轮的花销逐条加起来就是它。读不到账本时这一格写「读不到」，不是零"
          >
            {sessionSpendLine(sessionSpend)}
          </Note>
        )}
        {(quota !== null || quotaUnreadable) && (
          <Note
            className="mono shrink-0 text-[10px] text-[var(--muted)]"
            text="今天的额度：与管理面执法时读的是同一格。没人配过上限时写「未配置」，那不是「无限」"
          >
            {quotaLine(quota, quotaUnreadable)}
          </Note>
        )}
        {/*
          这一段处在哪一格模式：问两句还是一句不问，是这一面唯一一处能改的东西。
          放在标题行上，因为它是这一段对话的属性，不是某一条消息的。
        */}
        <select
          value={permissionMode}
          onChange={(e) => changePermissionMode(e.target.value)}
          aria-label="权限模式"
          className="mono ml-auto shrink-0 bg-transparent text-[10px] outline-none"
          style={{ color: 'var(--muted)' }}
        >
          <option value="plan">plan · 只读</option>
          <option value="default">default · 动手前问</option>
          <option value="acceptEdits">acceptEdits · 编辑不问</option>
          <option value="bypass">bypass · 全不问</option>
        </select>
        {/*
          留下来的规则：人点过「总是」的那些。入口只有一行字，因为它是设置，不是这一面
          的主体；列表本身体现在下面那块面板里，撤销就在同一行上。
        */}
        <Note
          as="button"
          type="button"
          onClick={toggleRules}
          className="mono shrink-0 px-1 text-[10px] transition-opacity hover:opacity-70"
          style={{ color: rulesOpen ? 'var(--brass)' : 'var(--muted)' }}
          text="看这个人留过哪些「总是允许 / 总是拒绝」，并撤掉其中一条"
        >
          规则
        </Note>
        {/*
          约定：这个人写下的、之后每一轮都加载的那几句。与规则同一处入口——
          两者都是设置，不是这一面的主体。
        */}
        <Note
          as="button"
          type="button"
          onClick={toggleMemory}
          className="mono shrink-0 px-1 text-[10px] transition-opacity hover:opacity-70"
          style={{ color: memoryOpen ? 'var(--brass)' : 'var(--muted)' }}
          text="写一句跨会话生效的约定，之后每一轮开头都会读到；也可以撤掉其中一句"
        >
          约定
        </Note>
        {/*
          目标与变更都是账上的行，所以每一个都能点进去。这里只是入口：这一面
          不知道那一行在账的第几页，所以它把身份交出去，由账那一面落位。
        */}
        {session.goals.length > 0 && (
          <span className="flex shrink-0 items-baseline gap-1 text-[10px] text-[var(--muted)]">
            目标
            {session.goals.map((goal) => (
              <Note
                as="button"
                key={goal.id}
                type="button"
                onClick={() => focusRecord('goal', goal.id)}
                text={`在项目面打开目标 ${goal.id}`}
                className="mono text-[var(--brass)] transition-opacity hover:opacity-70"
              >
                {shortGoal(goal.id)}
              </Note>
            ))}
          </span>
        )}
        {session.changes.length > 0 && (
          <span className="flex min-w-0 shrink items-baseline gap-1 text-[10px] text-[var(--muted)]">
            变更
            {session.changes.slice(-3).map((change) => (
              <Note
                as="button"
                key={change.id}
                type="button"
                onClick={() => focusRecord('change', change.id)}
                text={`在变更账上打开 ${change.id}`}
                className="mono max-w-[12ch] truncate text-[var(--brass)] transition-opacity hover:opacity-70"
              >
                {shortChange(change.id)}
              </Note>
            ))}
          </span>
        )}
        <span className="ml-auto flex items-center gap-3">
          {session.hydrating && (
            <span className="text-[10px] text-[var(--muted)]">正在读回历史…</span>
          )}
          {session.hydrateError && (
            <span className="text-[10px]" style={{ color: 'var(--warn)' }}>
              历史没读到：{session.hydrateError}
            </span>
          )}
          {closable && (
            <button
              type="button"
              onClick={onClose}
              className="text-[11px] text-[var(--muted)] transition-colors hover:text-[var(--danger)]"
              aria-label="关掉这一段对话"
            >
              关闭
            </button>
          )}
        </span>
      </header>

      <div
        ref={scrollerRef}
        onScroll={onScroll}
        className="min-h-0 flex-1 overflow-y-auto px-4 pb-3"
      >
        {session.messages.length === 0 && !session.hydrating && (
          <p className="max-w-[46ch] text-[12px] leading-[1.7] text-[var(--muted)]">
            还没有说过话。说一句你要它做的事——它会先回一句话，然后把这件事拆成任务，
            每一步都会回到这一屏。
          </p>
        )}
        {/*
          对话与执行混在一条时间线上：一次运行里「说了什么」和「做了什么」交替
          发生，分成两栏读的人要自己在两列里对时刻。工具那一行收起来只占一行，
          展开才是原文——几十次调用全展开会把回复挤到屏幕外。
        */}
        <div className="flex flex-col gap-4">
          {timeline.map((item) =>
            item.kind === 'tool' ? (
              <ToolTurn key={`tool-${item.call.id}`} call={item.call} />
            ) : item.kind === 'agent' ? (
              <AgentTurn key={`agent-${item.agent.id}`} turn={item.agent} />
            ) : (
              <Turn key={item.message.id} message={item.message} />
            )
          )}
        </div>
      </div>

      {/*
        这张单子贴在输入框上方，不跟着历史滚走：它回答的是「这件事走到哪了」，
        是现况，不是某一次执行的记录。两位写者就两块，谁的活谁的单。
      */}
      {todoLists.length > 0 && (
        <div className="shrink-0 px-4 pb-1">
          {todoLists.map((list) => (
            <TodoPanel key={list.agent ?? 'unattributed'} list={list} />
          ))}
        </div>
      )}

      {rulesOpen && (
        <div className="shrink-0 px-4 pb-1">
          <div className="mb-1 flex items-baseline gap-2">
            <span className="mono text-[10px]" style={{ color: 'var(--brass)' }}>
              留下来的规则
            </span>
            <Note
              as="button"
              type="button"
              onClick={() =>
                clientRef.current?.send({ type: 'permission_rules', session_id: sessionId })
              }
              className="mono px-1 text-[10px] text-[var(--muted)] transition-opacity hover:opacity-70"
              text="重新从服务端读一份名单"
            >
              刷新
            </Note>
            <Note
              as="button"
              type="button"
              onClick={toggleRules}
              className="mono px-1 text-[10px] text-[var(--muted)] transition-opacity hover:opacity-70"
              text="收起这块面板"
            >
              收起
            </Note>
          </div>
          {/*
            「还在读」与「一条也没有」分开写：前者是这一次还没拿到名单（连接刚起、
            或者服务端这一侧还没接上），后者是这个人真的干净。合起来写，一次失败的
            读取会被读成一次干净的家底。
          */}
          {permissionRules === null ? (
            <p className="mono text-[10px] text-[var(--muted)]">还在读这份名单…</p>
          ) : permissionRules.length === 0 ? (
            <p className="mono text-[10px] text-[var(--muted)]">这个人名下还没有留下过规则。</p>
          ) : (
            <ul className="flex flex-col gap-1">
              {permissionRules.map((rule) => (
                <li key={rule.id} className="flex items-baseline gap-2 text-[11px]">
                  <span
                    className="mono"
                    style={{ color: rule.allow ? 'var(--brass)' : 'var(--danger)' }}
                  >
                    {rule.allow ? '允许' : '拒绝'}
                  </span>
                  <span className="mono">{rule.tool}</span>
                  {rule.subject && (
                    <span className="mono break-all text-[var(--paper-dim)]">{rule.subject}</span>
                  )}
                  <span className="mono text-[10px] text-[var(--muted)]">
                    {rule.scope === 'owner' ? '这个人' : '这一段'}
                  </span>
                  <Note
                    as="button"
                    type="button"
                    onClick={() => revokeRule(rule.id)}
                    className="mono px-1 text-[10px] text-[var(--muted)] transition-opacity hover:opacity-70"
                    text="撤掉这条规则；从此同类的调用会重新问你"
                  >
                    撤销
                  </Note>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
      {memoryOpen && (
        <div className="shrink-0 px-4 pb-1">
          <div className="mb-1 flex items-baseline gap-2">
            <span className="mono text-[10px]" style={{ color: 'var(--brass)' }}>
              写下的约定
            </span>
            <Note
              as="button"
              type="button"
              onClick={() => clientRef.current?.send({ type: 'owner_memory' })}
              className="mono px-1 text-[10px] text-[var(--muted)] transition-opacity hover:opacity-70"
              text="重新从服务端读一份名单"
            >
              刷新
            </Note>
            <Note
              as="button"
              type="button"
              onClick={toggleMemory}
              className="mono px-1 text-[10px] text-[var(--muted)] transition-opacity hover:opacity-70"
              text="收起这块面板"
            >
              收起
            </Note>
          </div>
          {/*
            「还在读」与「一句也没写」分开写，与规则面板同一条理由：合起来写，
            一次失败的读取会被读成一次干净的家底。
          */}
          {memoryEntries === null ? (
            <p className="mono text-[10px] text-[var(--muted)]">还在读这份名单…</p>
          ) : memoryEntries.length === 0 ? (
            <p className="mono text-[10px] text-[var(--muted)]">
              这个人还没有写下过约定。
            </p>
          ) : (
            <ul className="flex flex-col gap-1">
              {memoryEntries.map((entry) => (
                <li key={entry.id} className="flex items-baseline gap-2 text-[11px]">
                  <span className="break-all text-[var(--paper-dim)]">{entry.text}</span>
                  <Note
                    as="button"
                    type="button"
                    onClick={() => forgetMemory(entry.id)}
                    className="mono shrink-0 px-1 text-[10px] text-[var(--muted)] transition-opacity hover:opacity-70"
                    text="撤掉这一句约定：从下一轮起不再加载它"
                  >
                    撤销
                  </Note>
                </li>
              ))}
            </ul>
          )}
          <div className="mt-1 flex items-baseline gap-2">
            <input
              value={memoryDraft}
              onChange={(e) => setMemoryDraft(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault();
                  writeMemory();
                }
              }}
              placeholder="写一句之后每一轮都要读到的约定…"
              className="mono min-w-0 flex-1 bg-transparent text-[11px] outline-none placeholder:text-[var(--muted)]"
            />
            <Note
              as="button"
              type="button"
              onClick={writeMemory}
              className="mono shrink-0 px-1 text-[10px] text-[var(--muted)] transition-opacity hover:opacity-70"
              text="记下这一句：从下一轮起它会被自动读进提示词"
            >
              记住
            </Note>
          </div>
        </div>
      )}
      {permissionAsks.length > 0 && (
        <div className="shrink-0 px-4 pb-1">
          {permissionAsks.map((ask) => (
            <div
              key={ask.id}
              className="mb-2 border-t pt-2"
              style={{ borderColor: 'var(--line-strong)' }}
            >
              <div className="mono text-[10px]" style={{ color: 'var(--brass)' }}>
                {ask.plan ? '计划待批准' : `要动手：${ask.tool}`}（{ask.mode} 模式）
              </div>
              {/*
                计划正文不截：这一份要在点「批准并执行」之前被读完，截到几行就等于
                让人闭眼点头。摘要那种一行字只在「要动手」时出现。
              */}
              {ask.plan ? (
                <div
                  className="mt-1 max-h-64 overflow-auto whitespace-pre-wrap break-words border-l-2 pl-2 text-[12px] text-[var(--paper)]"
                  style={{ borderColor: 'var(--line-strong)' }}
                >
                  {ask.plan}
                </div>
              ) : (
                <pre className="mt-1 max-h-24 overflow-auto whitespace-pre-wrap break-all text-[12px] text-[var(--paper)]">
                  {ask.summary}
                </pre>
              )}
              <div className="mt-2 flex gap-3">
                {/*
                  计划只有两个答案：批准，或者让它继续改。批准不是「允许这一次」——
                  它换的是这一轮的模式，之后每一步动手照旧按模式与规则问人。
                */}
                {ask.plan ? (
                  <>
                    <Note
                      as="button"
                      type="button"
                      onClick={() => answerPermission(ask, true)}
                      className="mono px-1 text-[10px] transition-opacity hover:opacity-70"
                      style={{ color: 'var(--brass)' }}
                      text="批准这份计划：这一轮从此按 default 模式执行，要动手的每一步仍然按模式与规则问你"
                    >
                      批准并执行
                    </Note>
                    <Note
                      as="button"
                      type="button"
                      onClick={() => {
                        clientRef.current?.send({
                          type: 'permission_response',
                          request_id: ask.id,
                          allow: false,
                          reason: '计划没有批准：继续只读调研，把计划改好再来请我批准。',
                        });
                        usePermissionStore.getState().answered(ask.id);
                      }}
                      className="mono px-1 text-[10px] transition-opacity hover:opacity-70"
                      style={{ color: 'var(--muted)' }}
                      text="不执行这份计划；模型会收到这句话，继续在只读模式下改计划"
                    >
                      继续改
                    </Note>
                  </>
                ) : (
                  <>
                <Note
                  as="button"
                  type="button"
                  onClick={() => answerPermission(ask, true)}
                  className="mono px-1 text-[10px] transition-opacity hover:opacity-70"
                  style={{ color: 'var(--brass)' }}
                  text="允许这一次执行；下一次同类调用仍然会问"
                >
                  允许
                </Note>
                <Note
                  as="button"
                  type="button"
                  onClick={() => answerPermission(ask, false)}
                  className="mono px-1 text-[10px] transition-opacity hover:opacity-70"
                  style={{ color: 'var(--danger)' }}
                  text="拒绝这一次，并把理由交回模型；同一个调用再发一遍会得到同一个答案"
                >
                  拒绝
                </Note>
                <Note
                  as="button"
                  type="button"
                  onClick={() => answerPermission(ask, true, true)}
                  className="mono px-1 text-[10px] transition-opacity hover:opacity-70"
                  style={{ color: 'var(--brass)' }}
                  text="允许这一次，并把这个工具碰这个东西的调用记成一条放行规则：下次同类调用不再问，在规则面板里可以撤销"
                >
                  总是允许
                </Note>
                <Note
                  as="button"
                  type="button"
                  onClick={() => answerPermission(ask, false, true)}
                  className="mono px-1 text-[10px] transition-opacity hover:opacity-70"
                  style={{ color: 'var(--danger)' }}
                  text="拒绝这一次，并把这个工具碰这个东西的调用记成一条拒绝规则：下次同类调用直接拒绝不再问，在规则面板里可以撤销"
                >
                  总是拒绝
                </Note>
                  </>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
      <div className="shrink-0 px-4 pb-4 pt-1">
        {/*
          一条发问的行，不是一个输入框。
          没有边框、没有底色、没有「请输入…」的空白盒：这几样合起来是表单的样子，
          读者会以为在填一格东西。这里是一句话的起头——一个提示符，一句还没说出口
          的话，右边是「怎么送出去」。分隔靠上方那一条发丝线，焦点在它上面时才亮。
        */}
        {/* 正随这句走的附件：贴进来或选进来的那几张，发之前认得出、拿得掉。 */}
        {pendingFiles.length > 0 && (
          <div className="flex flex-wrap items-center gap-2 pb-1">
            {pendingFiles.map((file, index) => (
              <span
                key={`${file.name}-${index}`}
                className="mono flex items-center gap-2 border px-2 py-0.5 text-[10px]"
                style={{ borderColor: 'var(--line)', color: 'var(--muted)' }}
              >
                {file.name} · {formatBytes(file.bytes)}
                <button
                  type="button"
                  aria-label={`去掉 ${file.name}`}
                  onClick={() => setPendingFiles((prev) => prev.filter((_, at) => at !== index))}
                  className="transition-colors hover:text-[var(--paper)]"
                >
                  ×
                </button>
              </span>
            ))}
          </div>
        )}
        {attachError && (
          <div className="mono pb-1 text-[10px]" style={{ color: 'var(--danger)' }}>
            {attachError}
          </div>
        )}
        {/*
          整句以 `/` 开头时列出来的命令名单。挑中的是一个名字，不是「这一句」——
          名字后面还要接着打要求，回车才发。
        */}
        {slash && (
          <div
            className="mono mb-1 max-h-48 overflow-auto border text-[11px]"
            style={{ borderColor: 'var(--line)' }}
          >
            {slash.error ? (
              <div className="px-2 py-1 text-[10px]" style={{ color: 'var(--danger)' }}>
                {slash.error}
              </div>
            ) : !slash.loaded ? (
              <div className="px-2 py-1 text-[10px]" style={{ color: 'var(--muted)' }}>
                正在读名单…
              </div>
            ) : slashCandidates.length === 0 ? (
              <div className="px-2 py-1 text-[10px]" style={{ color: 'var(--muted)' }}>
                没有以「{slash.query}」起头的命令
              </div>
            ) : (
              slashCandidates.map((command, index) => (
                <button
                  key={command.name}
                  type="button"
                  // 按下时不许输入框失焦：失焦就先把这份名单收掉了，点空。
                  onMouseDown={(e) => {
                    e.preventDefault();
                    acceptSlash(command);
                  }}
                  onMouseEnter={() =>
                    setSlash((prev) => (prev ? { ...prev, index } : prev))
                  }
                  className="block w-full truncate px-2 py-0.5 text-left"
                  style={{
                    background: index === slash.index ? 'var(--line)' : 'transparent',
                    color: 'var(--paper-dim)',
                  }}
                >
                  /{command.name}
                  <span className="ml-2 text-[10px]" style={{ color: 'var(--muted)' }}>
                    {command.description}
                  </span>
                </button>
              ))
            )}
            {slash.note && (
              <div className="px-2 py-1 text-[10px]" style={{ color: 'var(--muted)' }}>
                {slash.note}
              </div>
            )}
          </div>
        )}
        {/*
          打 `@` 列出来的那一层文件。挑中的是一条路径，不是「这一句」——它接在
          光标处，接着往下打时列表跟着下一层走。
        */}
        {mention && (
          <div
            className="mono mb-1 max-h-48 overflow-auto border text-[11px]"
            style={{ borderColor: 'var(--line)' }}
          >
            {mention.error ? (
              <div className="px-2 py-1 text-[10px]" style={{ color: 'var(--danger)' }}>
                {mention.error}
              </div>
            ) : mentionCandidates.length === 0 ? (
              <div className="px-2 py-1 text-[10px]" style={{ color: 'var(--muted)' }}>
                这一层没有以「{mention.query.leaf}」起头的文件
              </div>
            ) : (
              mentionCandidates.map((entry, index) => (
                <button
                  key={entry.path}
                  type="button"
                  // 按下时不许输入框失焦：失焦就先把这份列表收掉了，点空。
                  onMouseDown={(e) => {
                    e.preventDefault();
                    acceptMention(entry);
                  }}
                  onMouseEnter={() =>
                    setMention((prev) => (prev ? { ...prev, index } : prev))
                  }
                  className="block w-full truncate px-2 py-0.5 text-left"
                  style={{
                    background: index === mention.index ? 'var(--line)' : 'transparent',
                    color: 'var(--paper-dim)',
                  }}
                >
                  {entry.path}
                  {entry.dir ? '/' : ''}
                </button>
              ))
            )}
          </div>
        )}
        <div
          className="flex items-stretch gap-2 border-t pt-2 transition-colors"
          style={{ borderColor: draft || pendingFiles.length > 0 ? 'var(--line-strong)' : 'var(--line)' }}
        >
          <span className="mono flex items-center pl-0.5 text-[12px] text-[var(--brass)]">›</span>
          <Note
            as="button"
            type="button"
            onClick={() => fileInputRef.current?.click()}
            aria-label="附上文件"
            text="附一张图、一份 PDF 或一段音视频；也可以直接把它们贴进这一格"
            className="mono shrink-0 px-1 text-[13px] transition-opacity hover:opacity-70"
            style={{ color: 'var(--muted)' }}
          >
            ＋
          </Note>
          <input
            ref={fileInputRef}
            type="file"
            multiple
            accept="image/*,application/pdf,audio/*,video/*"
            className="hidden"
            onChange={(e) => {
              void attach(e.target.files);
              // 清掉，同一份文件选第二次也要再进一次队列。
              e.target.value = '';
            }}
          />
          <textarea
            ref={draftRef}
            rows={1}
            value={draft}
            onChange={(e) => {
              const value = e.target.value;
              setDraft(value);
              const caret = e.target.selectionStart ?? value.length;
              const query = mentionAt(value, caret);
              setMention((prev) => {
                if (!query) return null;
                // 同一层里继续打字：列表不重取，只换「以什么起头」。
                if (prev && prev.query.dir === query.dir) {
                  return { ...prev, query, index: 0 };
                }
                return { query, entries: [], index: 0, error: null };
              });
              // `/` 只在整句开头算一条命令：一句话中间的斜杠是在打路径，不是在
              // 点技能。名单只在刚打开时取一次，之后继续打字只筛已有的那几行。
              const command = slashQuery(value, caret);
              setSlash((prev) => {
                if (command === null) return null;
                if (prev) return { ...prev, query: command, index: 0 };
                return { query: command, index: 0, list: [], error: null, loaded: false };
              });
            }}
            onFocus={onFocus}
            onBlur={() => {
              setMention(null);
              setSlash(null);
            }}
            onPaste={(e) => {
              // 截图直接贴进来：剪贴板里带文件时，文件才是这一次要说的东西。
              const files = Array.from(e.clipboardData?.items ?? [])
                .filter((item) => item.kind === 'file')
                .map((item) => item.getAsFile())
                .filter((file): file is File => Boolean(file));
              if (files.length === 0) return;
              e.preventDefault();
              void attach(files);
            }}
            onKeyDown={(e) => {
              // 命令名单开着时，上下键与回车/Tab 归名单：这一下挑的是一个名字，
              // 不是把 `/webres` 当成一句话发出去。挑完只补到名字与一个空格上，
              // 要求还要接着打——命令执行的是名字，后面那段是人自己写的。
              if (slash) {
                if (e.key === 'Escape') {
                  e.preventDefault();
                  setSlash(null);
                  return;
                }
                if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                  e.preventDefault();
                  const step = e.key === 'ArrowDown' ? 1 : -1;
                  const size = Math.max(slashCandidates.length, 1);
                  setSlash((prev) =>
                    prev ? { ...prev, index: (prev.index + step + size) % size } : prev
                  );
                  return;
                }
                if (
                  (e.key === 'Enter' || e.key === 'Tab') &&
                  !e.nativeEvent.isComposing &&
                  e.nativeEvent.keyCode !== 229
                ) {
                  const pick = slashCandidates[slash.index] ?? slashCandidates[0];
                  if (pick) {
                    e.preventDefault();
                    acceptSlash(pick);
                    return;
                  }
                }
              }
              // 文件列表开着时，上下键与回车/Tab 归列表：这一下挑的是一条路径，
              // 不是把半句话发出去。Esc 收掉列表，回车回到「发出去」。
              if (mention) {
                if (e.key === 'Escape') {
                  e.preventDefault();
                  setMention(null);
                  return;
                }
                if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                  e.preventDefault();
                  const step = e.key === 'ArrowDown' ? 1 : -1;
                  const size = Math.max(mentionCandidates.length, 1);
                  setMention((prev) =>
                    prev ? { ...prev, index: (prev.index + step + size) % size } : prev
                  );
                  return;
                }
                if (
                  (e.key === 'Enter' || e.key === 'Tab') &&
                  !e.nativeEvent.isComposing &&
                  e.nativeEvent.keyCode !== 229
                ) {
                  const pick = mentionCandidates[mention.index] ?? mentionCandidates[0];
                  if (pick) {
                    e.preventDefault();
                    acceptMention(pick);
                    return;
                  }
                }
              }
              // 回车是「发出去」，Shift+Enter 是「换一行」——多行的一句话（贴一段
              // 报错、写一份步骤）不该逼人先去别处拼好再贴进来。
              if (e.key !== 'Enter' || e.shiftKey) return;
              // 输入法正在选词时，回车是「确认这个词」。在这里发出去的话，一段中文
              // 会在打到一半时被当作一句话送走，而且那一句缺了刚选的那个词。
              if (e.nativeEvent.isComposing || e.nativeEvent.keyCode === 229) return;
              e.preventDefault();
              send();
            }}
            placeholder="要它做什么…（Shift+Enter 换行）"
            aria-label="说一句话"
            className="min-w-0 flex-1 resize-none bg-transparent px-0.5 py-1 text-[13px] leading-[1.6] text-[var(--paper)] outline-none placeholder:text-[var(--muted)]"
          />
          {streaming && (
            <Note
              as="button"
              type="button"
              onClick={stop}
              className="mono shrink-0 px-2 text-[10px] transition-opacity hover:opacity-70"
              style={{ color: 'var(--danger)' }}
              text="停下这一段上正在跑的这一轮；说过的字留着，这一轮记为已中止"
            >
              ■ 停止
            </Note>
          )}
          <button
            type="button"
            onClick={send}
            disabled={!draft.trim() && pendingFiles.length === 0}
            className="mono px-2 text-[10px] transition-opacity disabled:cursor-not-allowed disabled:opacity-0"
            style={{ color: 'var(--brass)' }}
          >
            ↵ 发送
          </button>
        </div>
      </div>
    </section>
  );
}
