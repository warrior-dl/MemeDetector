import { useQuery } from "@tanstack/react-query";
import { fetchJson } from "../../data/api";
import type {
  AgentConversationDetail,
  AgentConversationListParams,
  AgentConversationListResponse,
} from "../../data/types";

export function useAgentConversations(params: AgentConversationListParams = {}) {
  return useQuery({
    queryKey: ["agent-conversations", params],
    queryFn: () => {
      const searchParams = new URLSearchParams({
        limit: String(params.limit ?? 20),
        offset: String(params.offset ?? 0),
      });
      if (params.runId) {
        searchParams.set("run_id", params.runId);
      }
      if (params.agentName) {
        searchParams.set("agent_name", params.agentName);
      }
      if (params.word) {
        searchParams.set("word", params.word);
      }
      if (params.status) {
        searchParams.set("status", params.status);
      }
      return fetchJson<AgentConversationListResponse>(
        `/api/v1/agent-conversations?${searchParams.toString()}`,
      );
    },
  });
}

export function useAgentConversation(conversationId?: string) {
  return useQuery({
    queryKey: ["agent-conversation", conversationId],
    queryFn: () =>
      fetchJson<AgentConversationDetail>(
        `/api/v1/agent-conversations/${encodeURIComponent(conversationId ?? "")}`,
      ),
    enabled: Boolean(conversationId),
  });
}
