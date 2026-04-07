import { useQuery } from "@tanstack/react-query";
import { fetchJson } from "../../data/api";
import type { StatsResponse } from "../../data/types";

export function useDashboardStats() {
  return useQuery({
    queryKey: ["stats"],
    queryFn: () => fetchJson<StatsResponse>("/api/v1/stats"),
  });
}
