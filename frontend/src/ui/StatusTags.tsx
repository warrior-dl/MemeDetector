import { Tag } from "antd";

export function CandidateStatusTag({ status }: { status?: string }) {
  const normalized = String(status || "pending").toLowerCase();
  const colorMap: Record<string, string> = {
    pending: "processing",
    accepted: "success",
    rejected: "error",
  };
  const labelMap: Record<string, string> = {
    pending: "待处理",
    accepted: "已接受",
    rejected: "已拒绝",
  };
  return <Tag color={colorMap[normalized]}>{labelMap[normalized] || normalized}</Tag>;
}

export function ConversationStatusTag({ status }: { status?: string }) {
  const normalized = String(status || "running").toLowerCase();
  const colorMap: Record<string, string> = {
    running: "processing",
    success: "success",
    failed: "error",
  };
  const labelMap: Record<string, string> = {
    running: "运行中",
    success: "成功",
    failed: "失败",
  };
  return <Tag color={colorMap[normalized]}>{labelMap[normalized] || normalized}</Tag>;
}

export function RunStatusTag({ status }: { status?: string }) {
  const normalized = String(status || "running").toLowerCase();
  const colorMap: Record<string, string> = {
    running: "processing",
    success: "success",
    failed: "error",
  };
  const labelMap: Record<string, string> = {
    running: "运行中",
    success: "成功",
    failed: "失败",
  };
  return <Tag color={colorMap[normalized]}>{labelMap[normalized] || normalized}</Tag>;
}

export function MinerInsightStatusTag({ status }: { status?: string }) {
  const normalized = String(status || "pending").toLowerCase();
  const colorMap: Record<string, string> = {
    pending: "processing",
    processed: "success",
  };
  const labelMap: Record<string, string> = {
    pending: "待提取候选",
    processed: "已提取候选",
  };
  return <Tag color={colorMap[normalized]}>{labelMap[normalized] || normalized}</Tag>;
}
