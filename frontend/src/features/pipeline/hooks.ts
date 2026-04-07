import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { fetchJson } from "../../data/api";
import type { JobItem, RunListParams, RunItem, TriggerJobResponse } from "../../data/types";

export function useRuns(params: RunListParams = {}) {
  return useQuery({
    queryKey: ["runs", params],
    queryFn: () => {
      const searchParams = new URLSearchParams({
        limit: String(params.limit ?? 50),
      });
      if (params.jobName) {
        searchParams.set("job_name", params.jobName);
      }
      if (params.status) {
        searchParams.set("status", params.status);
      }
      return fetchJson<RunItem[]>(`/api/v1/runs?${searchParams.toString()}`);
    },
  });
}

export function useRunDetail(runId?: string) {
  return useQuery({
    queryKey: ["run", runId],
    queryFn: () => fetchJson<RunItem>(`/api/v1/runs/${encodeURIComponent(runId ?? "")}`),
    enabled: Boolean(runId),
  });
}

export function useJobs() {
  return useQuery({
    queryKey: ["jobs"],
    queryFn: () => fetchJson<JobItem[]>("/api/v1/jobs"),
  });
}

export function useTriggerJob() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (jobName: string) =>
      fetchJson<TriggerJobResponse>(`/api/v1/jobs/${encodeURIComponent(jobName)}/run`, {
        method: "POST",
      }),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["runs"] }),
        queryClient.invalidateQueries({ queryKey: ["jobs"] }),
      ]);
    },
  });
}
