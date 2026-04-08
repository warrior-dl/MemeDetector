import { useQuery } from "@tanstack/react-query";
import { fetchJson } from "../../data/api";
import type {
  ScoutRawVideoDetail,
  ScoutRawVideosPageParams,
  ScoutRawVideosPageResponse,
} from "../../data/types";

export function useScoutRawVideosPage(params: ScoutRawVideosPageParams = {}) {
  return useQuery({
    queryKey: ["scout", "raw-videos", params],
    queryFn: () => {
      const searchParams = new URLSearchParams({
        limit: String(params.limit ?? 20),
        offset: String(params.offset ?? 0),
      });
      if (params.candidateStatus) {
        searchParams.set("candidate_status", params.candidateStatus);
      }
      if (params.partition) {
        searchParams.set("partition", params.partition);
      }
      if (params.keyword) {
        searchParams.set("keyword", params.keyword);
      }
      return fetchJson<ScoutRawVideosPageResponse>(
        `/api/v1/scout/raw-videos?${searchParams.toString()}`,
      );
    },
  });
}

export function useScoutRawVideoDetail(bvid?: string, collectedDate?: string) {
  return useQuery({
    queryKey: ["scout", "raw-video", bvid, collectedDate],
    queryFn: () =>
      fetchJson<ScoutRawVideoDetail>(
        `/api/v1/scout/raw-videos/${encodeURIComponent(
          bvid ?? "",
        )}?collected_date=${encodeURIComponent(collectedDate ?? "")}`,
      ),
    enabled: Boolean(bvid && collectedDate),
  });
}
