import { useCallback, useEffect, useRef, useState } from 'react';
import {
  getAgentConversation,
  listAgents,
  sendAgentPrompt,
  type AgentConversationLine,
  type AgentSummary,
} from '@/api/client';

/**
 * 名单 + 一个 agent 的会话，以及发送入口。
 *
 * 发送之后**重新读一遍**会话，而不是把刚发的那句直接塞进本地列表：落定的形状
 * （回了什么、超时没回、被拒了）是服务端写的，本地先插一行就只能猜它长什么样，
 * 猜出来的那一行与实际写下来的那一行不一致时，读的人无从分辨哪个是真的。
 *
 * 读取失败不吞掉：名单读不到与「没有 agent 在岗」长得一样，会话读不到与「没说过
 * 话」长得一样，两者都必须各自说出来。
 */
export function useAgentConversation(enabled: boolean, waitSecs = 60) {
  const [agents, setAgents] = useState<AgentSummary[]>([]);
  const [agentsError, setAgentsError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [lines, setLines] = useState<AgentConversationLine[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
  const [sendError, setSendError] = useState<string | null>(null);
  const selectedRef = useRef<string | null>(null);
  selectedRef.current = selected;

  const refreshAgents = useCallback(async () => {
    try {
      const roster = await listAgents();
      setAgents(roster);
      setAgentsError(null);
      // 选中的那个下线了就把选中让给名单里的第一个：留着一个已不在岗的名字，
      // 面板会一直对一个不会回话的地址说。
      setSelected((current) => {
        if (current && roster.some((a) => a.agent_id === current)) return current;
        return roster[0]?.agent_id ?? null;
      });
    } catch (e) {
      const err = e as { status?: number; message?: string };
      setAgents([]);
      setAgentsError(err?.message?.trim() || `读不到 agent 名单（HTTP ${err?.status ?? '?'}）`);
    }
  }, []);

  const refreshConversation = useCallback(async (agentId: string | null) => {
    if (!agentId) {
      setLines([]);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      setLines(await getAgentConversation(agentId));
    } catch (e) {
      const err = e as { status?: number; message?: string };
      setLines([]);
      setError(err?.message?.trim() || `读不到会话（HTTP ${err?.status ?? '?'}）`);
    } finally {
      setLoading(false);
    }
  }, []);

  const send = useCallback(
    async (text: string) => {
      const agentId = selectedRef.current;
      const body = text.trim();
      if (!agentId || !body) return;
      setSending(true);
      setSendError(null);
      try {
        const outcome = await sendAgentPrompt(agentId, body, waitSecs);
        if (outcome.error) setSendError(outcome.error);
      } finally {
        setSending(false);
        await refreshConversation(agentId);
      }
    },
    [refreshConversation, waitSecs]
  );

  useEffect(() => {
    if (enabled) refreshAgents();
  }, [enabled, refreshAgents]);

  useEffect(() => {
    if (enabled) refreshConversation(selected);
  }, [enabled, selected, refreshConversation]);

  return {
    agents,
    agentsError,
    selected,
    select: setSelected,
    lines,
    loading,
    error,
    sending,
    sendError,
    send,
    refreshAgents,
    refreshConversation: () => refreshConversation(selectedRef.current),
  };
}
