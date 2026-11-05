import { useState } from 'react';
import type { ActorUsage } from '@/api/client';
import { Note } from '@/ui/Note';

/**
 * 「谁在花」上面那四个数：执行者 / 上游 / 协议 / 环境各出现了几个。
 *
 * 一个孤零零的整数答不了任何后续问题——「16 个执行者是哪 16 个」「为什么上游
 * 有 6 个，不是只有两个吗」。所以每一格都能点开，点开就是这一维的成员清单，
 * 每个成员带着自己的调用数、失败数和 token；数出来的个数与清单长度来自同一个
 * 数组，不会出现「写着 16、下面只列得出 10」的情况。
 *
 * 每个取值都配一句它到底是什么。这几列里混着两类东西：一类是真正的成员
 * （某个执行者名、某条上游、某个环境），另一类是占位符——`-` 表示请求在池级
 * 熔断那一步就被拒了、一个上游都没试；`unknown` 表示调用方没有声明自己是谁、
 * 在哪个环境跑。占位符也是折叠表里真实存在的取值，所以照样列出，但**不进那个数**：
 * 数「平台上有几条上游」时把「未发起」加进去，读者会以为多接了一条线路，而它恰恰
 * 是「一条都没试」；数「有几个环境」时把「没人声明」加进去，屏幕上就有个数指向
 * 一个不存在的地方。`LlmOrigin::Unknown` 的契约原文就是「不计进任何一个环境的
 * 总数」，这一屏只是照着它显示。
 *
 * 「这一维一共几个」还有第二种误读：数出来的只是**这段窗口里出现过的**。一个
 * 闭集登记了六个环境，某段窗口只走过其中一个，屏幕上写「1」不会让任何人想到
 * 另外五个的存在。所以两个带闭集的维（执行者、环境）把登记的整份名单也读进来，
 * 窗口里没出现的那几个照样列出，标「本窗口没有调用」——出现过几个与登记了几个
 * 是两个数，两个都写出来。
 */
export function DimensionCounts({ actors }: { actors: ActorUsage[] }) {
  // 一进来全是收起的。执行者那一维**没有**可展开的清单：它下面那张表就是清单，
  // 而且带表头、两级行、能追问到「哪个环境、哪条上游、哪种协议」。同一个维度在
  // 一屏里画两遍，读者要花一次力气才能确认「上面那串没有表头的名字和下面那张表
  // 说的是同一件事」，而两处一旦有一处落后，看起来就只是两份不一样的读数。
  const [open, setOpen] = useState<string | null>(null);

  return (
    <div className="rail py-2 pr-4">
      <div className="flex flex-wrap items-start gap-x-6 gap-y-2">
        {DIMENSIONS.map((dim) => {
          const list = members(actors, dim);
          // 数的是成员，不是折叠表里的每一格：占位符单列在描述里说。
          const counted = countedMembers(list, dim);
          const absent = registeredButAbsent(list, dim);
          const expandable = dim.expandable !== false;
          const expanded = expandable && open === dim.key;
          return (
            <Note
              as={expandable ? 'button' : 'div'}
              key={dim.key}
              type={expandable ? 'button' : undefined}
              onClick={expandable ? () => setOpen(expanded ? null : dim.key) : undefined}
              className={expandable ? 'cursor-pointer text-left' : 'text-left'}
              aria-expanded={expandable ? expanded : undefined}
              text={
                !expandable
                  ? '这一维的成员清单就在下面的表里'
                  : expanded
                    ? '收起这一维的成员'
                    : '列出这一维的成员'
              }
            >
              <div className="mono text-[17px] leading-tight text-[var(--paper)]">
                {counted.length}
              </div>
              <div className="text-[11px] text-[var(--muted)]">
                {dim.title}
                {expandable && (
                  <span className="ml-1 text-[var(--paper-dim)]">{expanded ? '▾' : '▸'}</span>
                )}
              </div>
              <div className="text-[11px] text-[var(--muted)] opacity-80">
                {dim.note(counted, absent)}
              </div>
            </Note>
          );
        })}
      </div>

      {DIMENSIONS.filter((dim) => dim.key === open).map((dim) => {
        const list = members(actors, dim);
        const counted = countedMembers(list, dim);
        const absent = registeredButAbsent(list, dim);
        return (
          <div key={dim.key} className="mt-2 flex flex-col gap-1" style={{ userSelect: 'text' }}>
            <div className="text-[11px] text-[var(--muted)]">
              {dim.title}一共 {counted.length} 个取值
              {list.length > counted.length
                ? `，另有 ${list.length - counted.length} 类不是成员（「未发起」「未声明」这类占位，照样列出）`
                : ''}
              （一个取值一行，调用数是这段窗口里它的全部尝试）：
            </div>
            {list.map((member) => {
              const read = dim.read(member.value);
              const placeholder = dim.isPlaceholder?.(member.value) ?? false;
              return (
                <div key={member.value} className="flex flex-wrap items-baseline gap-x-3 text-[11px]">
                  <span
                    className="mono min-w-[9rem] text-[var(--paper)]"
                    style={{ color: read.tone, opacity: placeholder ? 0.7 : undefined }}
                  >
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
                    {(member.input + member.cached + member.output).toLocaleString()} token
                  </span>
                  <span className="mono w-16 shrink-0 text-right text-[var(--muted)]">
                    {member.rows} 行
                  </span>
                </div>
              );
            })}
            {/* 登记了、这段窗口没走过的取值：不列出它们，读者会把这个数当成全集。 */}
            {absent.map((value) => {
              const read = dim.read(value);
              return (
                <div
                  key={value}
                  className="flex flex-wrap items-baseline gap-x-3 text-[11px] opacity-50"
                >
                  <span className="mono min-w-[9rem] text-[var(--muted)]">{read.label}</span>
                  <span className="min-w-0 flex-1 truncate text-[var(--muted)]">{read.hint}</span>
                  <span className="mono shrink-0 text-[var(--muted)]">本窗口没有调用</span>
                </div>
              );
            })}
            <div className="text-[11px] text-[var(--muted)] opacity-80">
              {dim.footnote(list, counted, absent)}
            </div>
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
  /** 缓存命中的输入：与 `input` 相加才是整个 prompt。 */
  cached: number;
  output: number;
}

type Dimension = {
  key: string;
  title: string;
  pick: (actor: ActorUsage) => string;
  read: (value: string) => { label: string; hint: string; tone?: string };
  /** 这一维里不是成员、只是占位的取值。占位符照样列出，但不进个数。 */
  isPlaceholder?: (value: string) => boolean;
  /** 这一维的闭集：登记了哪些取值。空窗口也要能看出全集有多大。 */
  registry?: Record<string, string>;
  /**
   * 这一维有没有可展开的成员清单。执行者那一维没有：它的清单是下面那张带表头的
   * 表，在这里再画一份就是同一个维度的一幅半图。
   */
  expandable?: boolean;
  note: (counted: Member[], absent: string[]) => string;
  footnote: (list: Member[], counted: Member[], absent: string[]) => string;
};

// 池级熔断的拒绝行把「没找到能用的线路」记在这个字符上。网关那边是
// `POOLED_REFUSAL_UNNAMED`（`crates/cog-gateway/src/security_gateway.rs`），
// 这里是它的读法：它不是成员，只是占位。
const POOLED_REFUSAL_UNNAMED = '-';

// 调用方一个名字都没报时落在这一格上。契约在 `crates/cog-core/src/contract/usage_ledger.rs`
// 的 `LlmOrigin::Unknown`：**「不是一个调用能来自的地方，因此既不进 `DECLARED`，
// 也不进任何一个环境的总数」**——所以它和 `-` 一样是占位，不是成员。门
// （`check-web-token-panel.sh` 第 8 段）把这个字面量与代码里 `Unknown` 那一臂比对，
// 两边漂了的唯一症状是面板把「没人认领的那一段」数成一个环境。
const UNKNOWN_ORIGIN = 'unknown';

// 执行者那一维的同一个占位。契约在网关的 `UNKNOWN_ACTOR`
// （`crates/cog-gateway/src/actor_labels.rs`）：没在请求上声明自己是谁的调用方落这里。
// 与上面那个分开写，是因为它们答的是两份不同的声明——两处一旦漂开，这条一行的读法
// 就会替另一份契约说话。门（第 8 段）逐个比对。
const UNKNOWN_ACTOR_NAME = 'unknown';

const ORIGIN_LABELS: Record<string, string> = {
  gateway_pool: '经网关共享上游池发出（平台里绝大多数调用）',
  resident_worker: '常驻 worker 进程',
  interactive: '人在 WebUI 里点的',
  sandbox: '沙盒里的任务',
  eval: '评测跑批',
  unknown: '调用方没有声明环境',
};

/**
 * 闭集里每一个固定名字的来历。
 *
 * 名单的正本是网关的 `actor_labels.rs`（`ACTOR_LABELS` ＋ `UNKNOWN_ACTOR`）：那里决定
 * 什么值能落到这一列上，这里只写读法。门（`check-web-token-panel.sh` 第 7 段）把两边
 * 的名单逐名比对，多一个少一个都红——否则网关新加一个执行者时，这一屏会照着旧表把
 * 它显示成「不在闭集里，可能是写错了名字」，把一次正常的扩词报成一次事故。
 *
 * 它同时是**这一维的闭集**（`registry`）：窗口里没出现过的名字照样列出来，标
 * 「本窗口没有调用」——只列出现过的，读者会把这个数当成全集。
 */
const ACTOR_MEANINGS: Record<string, string> = {
  eval: '评测跑批这个程序自己的调用（三个读成「评测」的名字里最外面的一个：`agent:evaluator` 是被评的那个角色，`self_review:evaluator` 是它审自己）',
  ralph: 'ralph 循环',
  self_review: '自检（没有指明被评对象的一侧）',
  'sandbox-document-organizer': '沙盒里的文档整理',
  unknown: '没有身份的调用——不是某个角色，是一次缺陷。网关在付钱之前就拒收没带 `x-cogneva-actor` 的调用（400，零花销），所以这一格的花销只该有 2026-11-03 之前的历史；此后它再涨，涨的是被拒的次数，说明有调用方没报身份，要按缺陷查',
};

/**
 * 已退役的执行者名：闭集里没有了，历史窗口里还有行。
 *
 * 它们不进 `registry`（那是今天的闭集，列出来会变成一行「本窗口没有调用」的假
 * 名字），只用于读旧行的来历——否则历史窗口会把一次制度变更显示成「写错了名字」。
 */
const RETIRED_ACTOR_MEANINGS: Record<string, string> = {
  web_chat: '历史（2026-11-05 之前）：界面对话统一记在这一格；此后每位员工按自己的 `agent:<id>` 落账',
  intent_gateway: '历史（2026-11-05 之前）：网关自己的 `/v1/intent` 意图分类；该入口已删除，意图由对话员工自己判',
  wiki: '历史（2026-11-05 之前）：知识库维护者；该维护者从未被装配，已删除',
  intent_assess: '历史（2026-11-05 之前）：协作执行器里那条专门的意图判定分支；此后判定作为一件 `platform_agent_task` 交给常驻员工做',
  skill_extractor: '历史（2026-11-05 之前）：从成熟模式抽取技能配置；这条路径没有任何生产调用方，已删除',
  memory: '历史（2026-11-05 之前）：记忆抽取自己叫模型；此后抽取作为一件 `platform_agent_task` 交给名册上的员工，账记在接活的人 `agent:<id>` 上',
  evolution: '历史（2026-11-05 之前）：自进化循环的几个判定（精炼技能、合成 hook、建议工具变体）自己叫模型；此后它们作为 `platform_agent_task` 交给名册上的员工，账记在接活的人 `agent:<id>` 上',
};

/**
 * 两族里的角色名：平台会为它建 agent 的那些名字。
 *
 * 闭集不止上面那几个固定名——`agent:<角色>` / `self_review:<角色>` 是两族前缀，
 * 族员是配置里的角色名。族员不写出来，上面那几行就会被当成全集，而表里实际
 * 还会多出两族各一列；读者数不出「2 族到底几个」，也没法和表对上。
 *
 * 正本是 `cog_core::roles::ALL`。这里抄的是**会以 agent 身份调用模型的角色**：
 * `external-worker` 不在其中——它是靠心跳占位的登记角色，从不跑 agent，永远不会有
 * 它的一行（列出来就是一行永远「本窗口没有调用」的假名字）。门
 * （`check-web-token-panel.sh` 第 7 段）把这张表与那个常量逐名比对，多一个少一个都红。
 */
export const AGENT_ROLES = ['planner', 'generator', 'evaluator', 'moderator', 'merger'];

/** 两族前缀。正本是网关的 `ACTOR_LABEL_PREFIXES`，门同名逐条比对。 */
export const ACTOR_FAMILY_PREFIXES = ['agent:', 'self_review:'] as const;

/**
 * 两族的全体成员：每个角色在每个前缀下各一个名字。
 *
 * 两个族员数相同，因为同一位角色既以 `agent:<角色>` 干活，又以
 * `self_review:<角色>` 审自己刚写的那份输出——同一个人干的第二件事，不是另一个
 * 执行者。
 */
export function familyActorNames(): string[] {
  return ACTOR_FAMILY_PREFIXES.flatMap((prefix) =>
    AGENT_ROLES.map((role) => `${prefix}${role}`)
  );
}

/**
 * 闭集里的执行者名，不含占位符 `unknown`：固定名 ＋ 两族成员，个数由表算出。
 *
 * 「谁在花」那张表也要按闭集列全：本窗口没出现过的名字照样占一行，标「本窗口
 * 没有调用」。只列出现过的，读者会把这个数当成全集——而闭集才是「平台上有哪些
 * 执行者」。名字只有这一份正本，表与计数格都从这里取。
 */
export function registeredActorNames(): string[] {
  return [
    ...Object.keys(ACTOR_MEANINGS).filter((name) => name !== UNKNOWN_ACTOR_NAME),
    ...familyActorNames(),
  ];
}

const ACTOR: Dimension = {
  key: 'actor',
  title: '执行者',
  pick: (a) => a.actor,
  read: (value) => ({
    label: value,
    hint: actorHint(value),
    tone: value === UNKNOWN_ACTOR_NAME ? 'var(--warn)' : undefined,
  }),
  isPlaceholder: (value) => value === UNKNOWN_ACTOR_NAME,
  registry: ACTOR_MEANINGS,
  expandable: false,
  note: (counted, absent) => {
    // 表里还留着一格 `unknown`，它是占位不是名字：读法要留着（否则它显示成
    // 「不在闭集里」），但不算进「登记了几个名字」。
    const fixed = Object.keys(ACTOR_MEANINGS).length - 1;
    const perFamily = AGENT_ROLES.length;
    const familyTotal = familyActorNames().length;
    const named = fixed + familyTotal;
    const shape =
      `闭集共 ${named} 个：${fixed} 个固定名 ＋ 2 族共 ${familyTotal} 个` +
      `（agent: ${perFamily} 个、self_review: ${perFamily} 个）`;
    return absent.length > 0
      ? `本窗口出现 ${counted.length} 个；${shape}。明细见下表，表底有总计。`
      : `见下表：${shape}，表底有总计。`;
  },
  footnote: () =>
    '执行者名来自网关的闭集：固定的阶段名（eval、ralph…），加 `agent:<角色>` / `self_review:<被评的角色>` 两族前缀。两族的族员是同一份角色名单——每位角色既以 `agent:<角色>` 干活，又以 `self_review:<角色>` 审自己刚写的那份输出，所以两族一样大。身份是网关的前提条件：没带执行者头的调用在付钱之前就被 400 拒掉，所以 `unknown` 那一格的花销只该有历史；2026-11-03 之后它的调用数再涨，涨的是被拒的匿名请求，不是一类执行者。闭集里登记了、这段窗口没出现过的名字也列在下面，标「本窗口没有调用」：出现过几个不是全集，表底那一行总计才是这一屏所有行的和。',
};

const UPSTREAM: Dimension = {
  key: 'upstream',
  title: '上游',
  pick: (a) => a.upstream,
  isPlaceholder: (value) => value === POOLED_REFUSAL_UNNAMED,
  read: (value) => {
    if (value === POOLED_REFUSAL_UNNAMED) {
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
  note: (counted) => `点开看是哪几条（${counted.length} 条在用的）`,
  footnote: (list, counted) =>
    `一个上游 = 一条线路 × 一个模型（账上记成「地址|模型」，上面按模型列、地址跟在后面）。同一个地址挂了两个模型就是两个上游；${
      list.length > counted.length ? '「未发起」不是上游、不进上面的个数，它记的是没找到能用的线路那几次。' : ''
    }平台上有几条上游是部署配的，不是这里能列的。`,
};

const API_STYLE: Dimension = {
  key: 'api_style',
  title: '协议',
  pick: (a) => a.api_style,
  isPlaceholder: (value) => value === POOLED_REFUSAL_UNNAMED,
  read: (value) => {
    // `-` 不是一种协议，是「一个上游都没试」的占位：池级熔断的拒绝行上没有
    // 协议可说，账上就拿这一格占位。把它当成协议名数进「有几种协议」，
    // 读者会以为平台多接了一套报文格式。
    if (value === POOLED_REFUSAL_UNNAMED) {
      return {
        label: '未发起',
        hint: '请求在池级熔断那一步就被拒了，没有走到任何一个协议面',
        tone: 'var(--warn)',
      };
    }
    if (value === 'anthropic') return { label: value, hint: 'Anthropic Messages 协议' };
    if (value === 'openai') return { label: value, hint: 'OpenAI 兼容 Chat Completions 协议' };
    if (value === 'openai-responses') return { label: value, hint: 'OpenAI Responses 协议' };
    return { label: value, hint: '网关不认识的协议名', tone: 'var(--warn)' };
  },
  note: (counted) => `点开看是哪几种（${counted.length} 种在用）`,
  footnote: (list, counted) =>
    `协议是「用哪套报文格式说话」，不是「找哪家厂商」：同一条 Anthropic 协议线路背后可以是任何兼容它的上游，所以协议只有报文格式那么多，厂商可以有更多。\`input\` 的含义也逐协议不同，缓存命中率因此只在同一协议内比。${
      list.length > counted.length ? '「未发起」不是协议、不进上面的个数，它记的是熔断在选协议之前就挡下的那几次。' : ''
    }`,
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
  // `unknown` 不是一个环境，只是「没人说」。它照样列出（那一段花销真实存在），
  // 但不进个数——否则屏幕上「环境有 4 个」里有 1 个根本不指向任何地方。
  isPlaceholder: (value) => value === UNKNOWN_ORIGIN,
  registry: ORIGIN_LABELS,
  note: (counted, absent) => {
    // 同上：登记表里那一格 `unknown` 不是环境。
    const declared = Object.keys(ORIGIN_LABELS).length - 1;
    if (absent.length > 0) return `本窗口出现 ${counted.length} 个，闭集共 ${declared} 个`;
    return `点开看是哪几个（闭集 ${declared} 个全出现过）`;
  },
  footnote: (list, counted, absent) => {
    const environments = Object.keys(ORIGIN_LABELS).filter((k) => k !== UNKNOWN_ORIGIN);
    const unattributed = list.length > counted.length;
    return (
      `环境是「谁在替它出面」：${environments.map((k) => `\`${k}\``).join('、')}。` +
      '它们全都走网关——「走网关」和「哪个环境」是两件事，所以别指望这里只有一个值。' +
      '`unknown` 不在其中：它不是某个环境，是调用方一个名字都没报，那一段花销照实列出但不算进「几个环境」。' +
      (absent.length > 0
        ? `闭集登记了 ${environments.length} 个环境，这段窗口只走过 ${counted.length} 个；没出现的也在下面列着，标「本窗口没有调用」——出现过几个不是全集。`
        : '') +
      (unattributed ? '下面标了 `unknown` 的那一行是没人认领的一段：它此刻有花销，只是说不出是谁的。' : '')
    );
  },
};

/**
 * 执行者名的来历。前缀族只有两族，其余是闭集里的固定名字。
 *
 * 三个都读成「评测」的名字要分清，否则「谁花的」这一栏在同一件事上给出三个数：
 * `eval` 是评测程序自己在跑；`agent:evaluator` 是多智能体里 evaluator 这个角色
 * 在给别人的产物打分；`self_review:evaluator` 是 evaluator 在审它自己刚写的那份
 * 输出。前两个是两个人，第三个是同一个人干的第二件事。
 */
export function actorHint(value: string): string {
  if (value.startsWith('agent:')) {
    const who = value.slice('agent:'.length);
    // 闭集里还有一族 `agent:<角色>`（planner…merger），族外的是某一位员工自己的
    // id：界面上那段对话、一个常驻员工都以它落账。两句话分开，读者才分得清
    // 「一个角色」与「一个人」。
    if (!AGENT_ROLES.includes(who)) {
      return `员工 ${who} 自己的调用（账上按 id 认人，不按角色）`;
    }
    return who === 'evaluator'
      ? '多智能体里的 evaluator 角色在给别人的产物打分（与 `eval`、`self_review:evaluator` 是三件事）'
      : `多智能体里的 ${who} 角色`;
  }
  if (value.startsWith('self_review:')) {
    const role = value.slice('self_review:'.length);
    return `${role} 在审它自己刚写的那份输出（同一个执行者的第二件事，不是另一个执行者）`;
  }
  return (
    ACTOR_MEANINGS[value] ??
    RETIRED_ACTOR_MEANINGS[value] ??
    '不在网关登记的执行者闭集里，可能是新调用方或写错了名字'
  );
}

/** 这一维真正算数的成员：占位符照样列出，但不计入「有几个」。 */
function countedMembers(list: Member[], dim: Dimension): Member[] {
  return dim.isPlaceholder ? list.filter((m) => !dim.isPlaceholder!(m.value)) : list;
}

/** 闭集里登记了、这段窗口里一次也没出现过的取值。 */
function registeredButAbsent(list: Member[], dim: Dimension): string[] {
  if (!dim.registry) return [];
  const seen = new Set(list.map((m) => m.value));
  // 占位符不是闭集的成员，所以「本窗口没有调用」这句话对它无意义：`unknown`
  // 的意思是「没有人报」，不是「这个环境今天没跑」。把它列成没跑的环境，正好
  // 把要分开的两件事合回去。剔除写在返回值这一句里，不另绑变量——门要判的正是
  // 「这一句有没有剔」，中间多一层绑定就判不到了。
  return Object.keys(dim.registry).filter(
    (value) => !seen.has(value) && !(dim.isPlaceholder?.(value) ?? false)
  );
}

const DIMENSIONS = [ACTOR, UPSTREAM, API_STYLE, ORIGIN];

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
      cached: 0,
      output: 0,
    };
    member.rows += 1;
    member.calls += actor.calls;
    member.failed += actor.failed_calls;
    member.input += actor.tokens_input;
    member.cached += actor.tokens_cached;
    member.output += actor.tokens_output;
    byValue.set(value, member);
  }
  return [...byValue.values()].sort(
    (a, b) =>
      b.input + b.cached + b.output - (a.input + a.cached + a.output) ||
      b.calls - a.calls ||
      a.value.localeCompare(b.value)
  );
}
