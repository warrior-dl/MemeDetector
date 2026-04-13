import { useQuery } from "@tanstack/react-query";
import { fetchJson } from "../../data/api";
import type {
  BundleDetailResponse,
  ResearchBundlesPageParams,
  ResearchBundlesPageResponse,
} from "../../data/types";

export function useResearchBundlesPage(params: ResearchBundlesPageParams = {}) {
  return useQuery({
    queryKey: ["research-bundles", "page", params],
    queryFn: () => {
      const searchParams = new URLSearchParams({
        limit: String(params.limit ?? 20),
        offset: String(params.offset ?? 0),
      });
      if (params.status) {
        searchParams.set("status", params.status);
      }
      if (params.queuedOnly) {
        searchParams.set("queued_only", "true");
      }
      if (params.keyword) {
        searchParams.set("keyword", params.keyword);
      }
      return fetchJson<ResearchBundlesPageResponse>(
        `/api/v1/research/bundles/page?${searchParams.toString()}`,
      );
    },
  });
}

export function useResearchBundleDetail(bundleId?: string) {
  return useQuery({
    queryKey: ["research-bundle", bundleId],
    queryFn: () =>
      fetchJson<BundleDetailResponse>(`/api/v1/research/bundles/${encodeURIComponent(bundleId ?? "")}`),
    enabled: Boolean(bundleId),
  });
}
