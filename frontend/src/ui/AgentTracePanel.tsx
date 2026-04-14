import { Button, Collapse, Descriptions, Empty, List, Space, Tag, Typography } from "antd";
import type { AgentTraceDetail, AgentTraceStep } from "../data/types";
import { JsonPanel } from "./JsonPanel";
import { ConversationStatusTag } from "./StatusTags";
import { formatDateTime, formatDuration } from "../utils/format";

interface AgentTracePanelProps {
  trace?: AgentTraceDetail;
  emptyText?: string;
}

export function AgentTracePanel({ trace, emptyText = "暂无 Trace 记录" }: AgentTracePanelProps) {
  if (!trace) {
    return <Empty description={emptyText} />;
  }

  const llmCallCount = trace.steps.filter((step) => step.event_type === "llm_generation").length;
  const toolCount = trace.steps.filter((step) => step.event_type === "tool").length;

  return (
    <Space direction="vertical" size={12} style={{ width: "100%" }}>
      <Descriptions
        size="small"
        bordered
        column={{ xs: 1, sm: 2 }}
        items={[
          {
            key: "status",
            label: "状态",
            children: <ConversationStatusTag status={trace.conversation.status} />,
          },
          {
            key: "started",
            label: "开始时间",
            children: formatDateTime(trace.conversation.started_at),
          },
          {
            key: "finished",
            label: "结束时间",
            children: formatDateTime(trace.conversation.finished_at),
          },
          {
            key: "messageCount",
            label: "消息数",
            children: String(trace.conversation.message_count ?? 0),
          },
          {
            key: "llmCount",
            label: "LLM 调用",
            children: String(llmCallCount),
          },
          {
            key: "toolCount",
            label: "工具步骤",
            children: String(toolCount),
          },
          {
            key: "tokens",
            label: "Token",
            children:
              typeof trace.conversation.token_usage?.total_tokens === "number"
                ? String(trace.conversation.token_usage.total_tokens)
                : "--",
          },
          {
            key: "langfuse",
            label: "Langfuse",
            children: trace.conversation.langfuse_public_url ? (
              <Button
                size="small"
                href={trace.conversation.langfuse_public_url}
                target="_blank"
              >
                打开 Trace
              </Button>
            ) : (
              "--"
            ),
          },
        ]}
      />
      <List
        dataSource={trace.steps}
        locale={{ emptyText: <Empty description={emptyText} /> }}
        renderItem={(step) => (
          <List.Item style={{ display: "block", paddingInline: 0 }}>
            <TraceStepCard step={step} />
          </List.Item>
        )}
      />
    </Space>
  );
}

function TraceStepCard({ step }: { step: AgentTraceStep }) {
  return (
    <div
      style={{
        border: "1px solid #e5e7eb",
        borderRadius: 12,
        padding: 12,
        background: "#fff",
      }}
    >
      <Space direction="vertical" size={8} style={{ width: "100%" }}>
        <Space wrap>
          <Tag color={resolveEventColor(step.event_type)}>{step.event_type}</Tag>
          <Tag>{step.stage}</Tag>
          <ConversationStatusTag status={step.status} />
          <Typography.Text strong>{step.title}</Typography.Text>
        </Space>
        <Typography.Text type="secondary">
          {formatDateTime(step.started_at)} · {formatDuration((step.duration_ms ?? 0) / 1000)}
        </Typography.Text>
        <Typography.Text>{step.summary || "暂无摘要"}</Typography.Text>
        <Collapse
          size="small"
          items={[
            {
              key: "details",
              label: "查看输入 / 输出",
              children: (
                <Space direction="vertical" size={12} style={{ width: "100%" }}>
                  <JsonPanel title="输入" value={step.input} />
                  <JsonPanel title="输出" value={step.output} />
                  <JsonPanel title="Metadata" value={step.metadata} />
                </Space>
              ),
            },
          ]}
        />
      </Space>
    </div>
  );
}

function resolveEventColor(eventType?: string) {
  const normalized = String(eventType || "").toLowerCase();
  if (normalized === "llm_generation") {
    return "processing";
  }
  if (normalized === "decision") {
    return "success";
  }
  if (normalized === "error") {
    return "error";
  }
  if (normalized === "persist") {
    return "cyan";
  }
  return "default";
}
