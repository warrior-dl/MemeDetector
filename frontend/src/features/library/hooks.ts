import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { fetchJson } from "../../data/api";
import type { MemeItem, MemeSearchParams, MemeSearchResponse } from "../../data/types";

export function useMemes(params: MemeSearchParams = {}) {
  return useQuery({
    queryKey: ["memes", params],
    queryFn: () => {
      const searchParams = new URLSearchParams({
        limit: String(params.limit ?? 50),
        offset: String(params.offset ?? 0),
      });
      searchParams.set("sort_by", "updated_at:desc");
      if (params.query) {
        searchParams.set("q", params.query);
        return fetchJson<MemeSearchResponse>(`/api/v1/memes/search?${searchParams.toString()}`);
      }
      if (params.verifiedOnly) {
        searchParams.set("verified_only", "true");
      }
      if (params.category) {
        searchParams.set("category", params.category);
      }
      if (params.lifecycle) {
        searchParams.set("lifecycle", params.lifecycle);
      }
      return fetchJson<MemeSearchResponse>(`/api/v1/memes?${searchParams.toString()}`);
    },
  });
}

export function useMemeDetail(memeId?: string) {
  return useQuery({
    queryKey: ["meme", memeId],
    queryFn: () => fetchJson<MemeItem>(`/api/v1/memes/${encodeURIComponent(memeId ?? "")}`),
    enabled: Boolean(memeId),
  });
}

export function useVerifyMeme() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ memeId, verified }: { memeId: string; verified: boolean }) =>
      fetchJson<{ id: string; human_verified: boolean }>(
        `/api/v1/memes/${encodeURIComponent(memeId)}/verify?verified=${verified ? "true" : "false"}`,
        { method: "POST" },
      ),
    onSuccess: async (_, variables) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["memes"] }),
        queryClient.invalidateQueries({ queryKey: ["meme", variables.memeId] }),
      ]);
    },
  });
}
