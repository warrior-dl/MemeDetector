import { Tag } from "antd";

export function BundleStatusTag({ status }: { status?: string }) {
  const normalized = String(status || "bundled").toLowerCase();
  const colorMap: Record<string, string> = {
    pending: "default",
    inspected: "cyan",
    bundled: "processing",
    researched: "success",
  };
  const labelMap: Record<string, string> = {
    pending: "待检查",
    inspected: "已检查",
    bundled: "待研判",
    researched: "已研判",
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

export function HypothesisStatusTag({ status }: { status?: string }) {
  const normalized = String(status || "pending").toLowerCase();
  const colorMap: Record<string, string> = {
    pending: "default",
    evidenced: "cyan",
    queued: "processing",
    accepted: "success",
    rejected: "error",
    manual_review: "warning",
    superseded: "default",
    merged: "purple",
  };
  const labelMap: Record<string, string> = {
    pending: "待补证据",
    evidenced: "证据已补齐",
    queued: "已排队",
    accepted: "已采纳",
    rejected: "已驳回",
    manual_review: "人工复核",
    superseded: "已替代",
    merged: "已合并",
  };
  return <Tag color={colorMap[normalized]}>{labelMap[normalized] || normalized}</Tag>;
}

export function ResearchDecisionTag({ decision }: { decision?: string }) {
  const normalized = String(decision || "manual_review").toLowerCase();
  const colorMap: Record<string, string> = {
    accept: "success",
    reject: "error",
    rewrite_title: "gold",
    manual_review: "warning",
    merge_into_existing: "purple",
  };
  const labelMap: Record<string, string> = {
    accept: "接受",
    reject: "拒绝",
    rewrite_title: "改写标题",
    manual_review: "人工复核",
    merge_into_existing: "并入已有词条",
  };
  return <Tag color={colorMap[normalized]}>{labelMap[normalized] || normalized}</Tag>;
}

export function MinerInsightStatusTag({ status }: { status?: string }) {
  const normalized = String(status || "discarded").toLowerCase();
  const colorMap: Record<string, string> = {
    pending_bundle: "processing",
    bundling: "cyan",
    bundled: "success",
    bundle_failed: "error",
    discarded: "default",
  };
  const labelMap: Record<string, string> = {
    pending_bundle: "待生成证据包",
    bundling: "生成中",
    bundled: "已生成证据包",
    bundle_failed: "证据包失败",
    discarded: "已淘汰",
  };
  return <Tag color={colorMap[normalized]}>{labelMap[normalized] || normalized}</Tag>;
}
