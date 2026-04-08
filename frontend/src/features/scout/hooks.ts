import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { fetchJson } from "../../data/api";
import type {
  ScoutRawVideoDetail,
  ScoutRawVideoStageUpdateResponse,
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

export function useUpdateScoutRawVideoStage() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      bvid,
      collectedDate,
      stage,
    }: {
      bvid: string;
      collectedDate: string;
      stage: "scouted" | "mined" | "researched";
    }) =>
      fetchJson<ScoutRawVideoStageUpdateResponse>(
        `/api/v1/scout/raw-videos/${encodeURIComponent(bvid)}/stage`,
        {
          method: "POST",
          body: JSON.stringify({
            collected_date: collectedDate,
            stage,
          }),
        },
      ),
    onSuccess: async (_, variables) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["scout"] }),
        queryClient.invalidateQueries({
          queryKey: ["scout", "raw-video", variables.bvid, variables.collectedDate],
        }),
        queryClient.invalidateQueries({ queryKey: ["miner"] }),
      ]);
    },
  });
}
