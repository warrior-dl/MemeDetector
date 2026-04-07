import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { fetchJson } from "../../data/api";
import type {
  AgentConversationDetail,
  AgentConversationListParams,
  AgentConversationListResponse,
  CandidateVerifyResponse,
  CandidatesPageParams,
  CandidatesPageResponse,
  CandidateSourcesResponse,
} from "../../data/types";

export function useCandidatesPage(params: CandidatesPageParams = {}) {
  return useQuery({
    queryKey: ["candidates", "page", params],
    queryFn: () => {
      const searchParams = new URLSearchParams({
        limit: String(params.limit ?? 20),
        offset: String(params.offset ?? 0),
      });
      if (params.status) {
        searchParams.set("status", params.status);
      }
      if (params.keyword) {
        searchParams.set("keyword", params.keyword);
      }
      return fetchJson<CandidatesPageResponse>(`/api/v1/candidates/page?${searchParams.toString()}`);
    },
  });
}

export function useCandidateSources(word?: string) {
  return useQuery({
    queryKey: ["candidate", "sources", word],
    queryFn: () =>
      fetchJson<CandidateSourcesResponse>(
        `/api/v1/candidates/${encodeURIComponent(word ?? "")}/sources?limit=120`,
      ),
    enabled: Boolean(word),
  });
}

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

export function useVerifyCandidate() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ word, action }: { word: string; action: "accept" | "reject" }) =>
      fetchJson<CandidateVerifyResponse>(
        `/api/v1/candidates/${encodeURIComponent(word)}/verify?action=${action}`,
        { method: "POST" },
      ),
    onSuccess: async (_, variables) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["candidates"] }),
        queryClient.invalidateQueries({ queryKey: ["candidate", "sources", variables.word] }),
        queryClient.invalidateQueries({ queryKey: ["stats"] }),
      ]);
    },
  });
}
