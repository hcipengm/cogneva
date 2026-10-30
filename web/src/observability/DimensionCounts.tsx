import { useState } from 'react';
import type { ActorUsage } from '@/api/client';

/**
 * 「谁在花」上面那四个数：执行者 / 上游 / 协议 / 环境各出现了几个。
 *
 * 一个孤零零的整数答不了任何后续问题——「16 个执行者是哪 16 个」「为什么上游
 * 有 6 个，不是只有两个吗」。所以每一格都能点开，点开就是这一维的成员清单，
 * 每个成员带着自己的调用数、失败数和 token；数出来的个数与清单长度来自同一个
 * 数组，不会出现「写着 16、下面只列得出 10」的情况。
 *
 * 每个取值都配一句它到底是什么。这几列里混着两类东西：一类是真正的成员
 * （某个执行者名、某条上游），另一类是占位符——`-` 表示请求在池级熔断那一步
 * 就被拒了、一个上游都没试；`unknown` 表示调用方没有声明。占位符也是折叠表里
 * 真实存在的取值，所以照样计数、照样列出，但从描述里一眼能看出它不是成员。
 */
export function DimensionCounts({ actors }: { actors: ActorUsage[] }) {
  // 默认展开第一维：读者最常问的就是「这些执行者是谁」，而一个默认全收起的
  // 界面会让人以为那些数只是装饰。
  const [open, setOpen] = useState<string | null>(ACTOR.key);

  return (
    <div className="border-l border-[var(--line)] py-2 pl-3 pr-4">
      <div className="flex flex-wrap items-start gap-x-6 gap-y-2">
        {DIMENSIONS.map((dim) => {
          const list = members(actors, dim);
          const expanded = open === dim.key;
          return (
            <button
              key={dim.key}
              type="button"
              onClick={() => setOpen(expanded ? null : dim.key)}
              className="cursor-pointer text-left"
              aria-expanded={expanded}
              title={expanded ? '收起这一维的成员' : '列出这一维的成员'}
            >
              <div className="mono text-[17px] leading-tight text-[var(--paper)]">{list.length}</div>
              <div className="text-[11px] text-[var(--muted)]">
                {dim.title}
                <span className="ml-1 text-[var(--paper-dim)]">{expanded ? '▾' : '▸'}</span>
              </div>
              <div className="text-[11px] text-[var(--muted)] opacity-80">{dim.note(list)}</div>
            </button>
          );
        })}
      </div>

      {DIMENSIONS.filter((dim) => dim.key === open).map((dim) => {
        const list = members(actors, dim);
        return (
          <div key={dim.key} className="mt-2 flex flex-col gap-1" style={{ userSelect: 'text' }}>
            <div className="text-[11px] text-[var(--muted)]">
              {dim.title}一共 {list.length} 个取值（一个取值一行，调用数是这段窗口里它的全部尝试）：
            </div>
            {list.map((member) => {
              const read = dim.read(member.value);
              return (
                <div key={member.value} className="flex flex-wrap items-baseline gap-x-3 text-[11px]">
                  <span className="mono min-w-[9rem] text-[var(--paper)]" style={{ color: read.tone }}>
                    {read.label}
                  </span>
                  <span className="min-w-0 flex-1 truncate text-[var(--muted)]">{read.hint}</span>
                  <span className="mono shrink-0 text-[var(--paper-dim)]">
                    {member.calls.toLocaleString()} 次
                  </span>
                  <span
                    className="mono w-24 shrink-0 text-right"
                    style={{ color: member.failed > 0 ? 'var(--danger)' : 'var(--muted)' }}
                  >
                    失败 {member.failed.toLocaleString()}
                  </span>
                  <span className="mono w-28 shrink-0 text-right text-[var(--paper-dim)]">
                    {(member.input + member.output).toLocaleString()} token
                  </span>
                  <span className="mono w-16 shrink-0 text-right text-[var(--muted)]">
                    {member.rows} 行
                  </span>
                </div>
              );
            })}
            <div className="text-[11px] text-[var(--muted)] opacity-80">{dim.footnote(list)}</div>
          </div>
        );
      })}
    </div>
  );
}

interface Member {
  value: string;
  /** 这个取值在折叠表里出现在几行上（执行者 × 环境 × 上游 × 协议）。 */
  rows: number;
  calls: number;
  failed: number;
  input: number;
  output: number;
}

type Dimension = {
  key: string;
  title: string;
  pick: (actor: ActorUsage) => string;
  read: (value: string) => { label: string; hint: string; tone?: string };
  note: (members: Member[]) => string;
  footnote: (members: Member[]) => string;
};

const ACTOR: Dimension = {
  key: 'actor',
  title: '执行者',
  pick: (a) => a.actor,
  read: (value) => ({ label: value, hint: actorHint(value), tone: value === 'unknown' ? 'var(--warn)' : undefined }),
  note: (list) =>
    list.some((m) => m.value === 'unknown') ? '点开看是谁（含未声明）' : '点开看是谁',
  footnote: () =>
    '执行者名来自网关的闭集：固定的阶段名（eval、memory、ralph…），加 `agent:<角色>` / `self_review:<被评的角色>` 两族前缀。没声明自己是谁的调用方记成 `unknown`——名字缺席是调用方没报，不是网关丢的。',
};

const UPSTREAM: Dimension = {
  key: 'upstream',
  title: '上游',
  pick: (a) => a.upstream,
  read: (value) => {
    if (value === '-') {
      return {
        label: '未发起',
        hint: '请求在池级熔断那一步就被拒了，一个上游都没试——所以这里不是「某个上游」',
        tone: 'var(--warn)',
      };
    }
    const cut = value.lastIndexOf('|');
    if (cut < 0) return { label: value, hint: '网关没有记下模型名' };
    return { label: value.slice(cut + 1), hint: value.slice(0, cut) };
  },
  note: (list) =>
    list.some((m) => m.value === '-') ? '点开看是哪几条（含未发起）' : '点开看是哪几条',
  footnote: (list) =>
    `一个上游 = 一条线路 × 一个模型（账上记成「地址|模型」，上面按模型列、地址跟在后面）。同一个地址挂了两个模型就是两个上游；${
      list.some((m) => m.value === '-') ? '「未发起」不是上游，它记的是没找到能用的线路那几次。' : ''
    }平台上有几条上游是部署配的，不是这里能列的。`,
};

const API_STYLE: Dimension = {
  key: 'api_style',
  title: '协议',
  pick: (a) => a.api_style,
  read: (value) => {
    if (value === 'anthropic') return { label: value, hint: 'Anthropic Messages 协议' };
    if (value === 'openai') return { label: value, hint: 'OpenAI 兼容 Chat Completions 协议' };
    if (value === 'openai-responses') return { label: value, hint: 'OpenAI Responses 协议' };
    return { label: value, hint: '网关不认识的协议名', tone: 'var(--warn)' };
  },
  note: () => '点开看是哪几种',
  footnote: () =>
    '协议是「用哪套报文格式说话」，不是「找哪家厂商」：同一条 Anthropic 协议线路背后可以是任何兼容它的上游，所以协议只有报文格式那么多，厂商可以有更多。`input` 的含义也逐协议不同，缓存命中率因此只在同一协议内比。',
};

const ORIGIN: Dimension = {
  key: 'origin',
  title: '环境',
  pick: (a) => a.origin,
  read: (value) => {
    const known = ORIGIN_LABELS[value];
    if (known) return { label: value, hint: known };
    return { label: value, hint: '调用方声明的环境名不在已登记的集合里', tone: 'var(--warn)' };
  },
  note: (list) =>
    list.some((m) => m.value === 'unknown') ? '点开看是哪几个（含未声明）' : '点开看是哪几个',
  footnote: () =>
    '环境是「谁在替它出面」：`gateway_pool` 是经网关共享上游池发出的，`interactive` 是人在 WebUI 里点的，`resident_worker` 是常驻进程，`sandbox` 是沙盒任务，`eval` 是评测跑批。它们全都走网关——「走网关」和「哪个环境」是两件事，所以别指望这里只有一个值。',
};

const DIMENSIONS = [ACTOR, UPSTREAM, API_STYLE, ORIGIN];

const ORIGIN_LABELS: Record<string, string> = {
  gateway_pool: '经网关共享上游池发出（平台里绝大多数调用）',
  resident_worker: '常驻 worker 进程',
  interactive: '人在 WebUI 里点的',
  sandbox: '沙盒里的任务',
  eval: '评测跑批',
  unknown: '调用方没有声明环境',
};

/** 执行者名的来历。前缀族只有两族，其余是闭集里的固定名字。 */
function actorHint(value: string): string {
  if (value.startsWith('agent:')) return `多智能体里的 ${value.slice('agent:'.length)} 角色`;
  if (value.startsWith('self_review:')) return `自检代理，评审对象是 ${value.slice('self_review:'.length)}`;
  const fixed: Record<string, string> = {
    eval: '评测跑批自己的调用（与 `agent:evaluator` 是两个东西：那是评测的产物，这是评测这个程序）',
    evolution: '自进化循环',
    intent_assess: '意图评估阶段',
    intent_gateway: '意图网关（交互入口的意图路由）',
    memory: '记忆读写',
    ralph: 'ralph 循环',
    self_review: '自检（没有指明被评对象的一侧）',
    skill_extractor: '技能抽取',
    'sandbox-document-organizer': '沙盒里的文档整理',
    web_chat: 'WebUI 对话',
    wiki: '知识库',
    unknown: '调用方没有声明执行者名（例如模型选择器，它发调用时没有报名字）',
  };
  return fixed[value] ?? '不在网关登记的执行者闭集里，可能是新调用方或写错了名字';
}

function members(actors: ActorUsage[], dim: Dimension): Member[] {
  const byValue = new Map<string, Member>();
  for (const actor of actors) {
    const value = dim.pick(actor);
    const member = byValue.get(value) ?? {
      value,
      rows: 0,
      calls: 0,
      failed: 0,
      input: 0,
      output: 0,
    };
    member.rows += 1;
    member.calls += actor.calls;
    member.failed += actor.failed_calls;
    member.input += actor.tokens_input;
    member.output += actor.tokens_output;
    byValue.set(value, member);
  }
  return [...byValue.values()].sort(
    (a, b) =>
      b.input + b.output - (a.input + a.output) ||
      b.calls - a.calls ||
      a.value.localeCompare(b.value)
  );
}
