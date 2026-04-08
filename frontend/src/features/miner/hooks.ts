import { useQuery } from "@tanstack/react-query";
import { fetchJson } from "../../data/api";
import type {
  MinerCommentInsightItem,
  MinerCommentInsightsPageParams,
  MinerCommentInsightsPageResponse,
} from "../../data/types";

export function useMinerCommentInsightsPage(params: MinerCommentInsightsPageParams = {}) {
  return useQuery({
    queryKey: ["miner", "comment-insights", params],
    enabled: params.enabled ?? true,
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
      if (params.bvid) {
        searchParams.set("bvid", params.bvid);
      }
      if (params.onlyMemeCandidates) {
        searchParams.set("only_meme_candidates", "true");
      }
      if (params.onlyInsiderKnowledge) {
        searchParams.set("only_insider_knowledge", "true");
      }
      return fetchJson<MinerCommentInsightsPageResponse>(
        `/api/v1/miner/comment-insights?${searchParams.toString()}`,
      );
    },
  });
}

export function useMinerCommentInsightDetail(insightId?: string) {
  return useQuery({
    queryKey: ["miner", "comment-insight", insightId],
    queryFn: () =>
      fetchJson<MinerCommentInsightItem>(
        `/api/v1/miner/comment-insights/${encodeURIComponent(insightId ?? "")}`,
      ),
    enabled: Boolean(insightId),
  });
}
