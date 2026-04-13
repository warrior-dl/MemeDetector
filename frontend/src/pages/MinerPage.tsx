import {
  Alert,
  Checkbox,
  Col,
  Descriptions,
  Empty,
  Input,
  List,
  Pagination,
  Row,
  Select,
  Space,
  Spin,
  Tabs,
  Tag,
  Typography,
} from "antd";
import { useDeferredValue, useEffect, useState } from "react";
import { useAgentConversation, useAgentConversations } from "../features/agents/hooks";
import {
  useMinerCommentInsightDetail,
  useMinerCommentInsightsPage,
} from "../features/miner/hooks";
import { JsonPanel } from "../ui/JsonPanel";
import { PageSection } from "../ui/PageSection";
import { ConversationStatusTag, MinerInsightStatusTag } from "../ui/StatusTags";
import { formatDateTime, shortId } from "../utils/format";

export function MinerPage() {
  const [status, setStatus] = useState<string>("pending_bundle");
  const [keywordInput, setKeywordInput] = useState("");
  const [bvidInput, setBvidInput] = useState("");
  const [onlyMemeCandidates, setOnlyMemeCandidates] = useState(false);
  const [onlyInsiderKnowledge, setOnlyInsiderKnowledge] = useState(false);
  const deferredKeyword = useDeferredValue(keywordInput.trim());
  const deferredBvid = useDeferredValue(bvidInput.trim());
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [selectedInsightId, setSelectedInsightId] = useState<string>();
  const [selectedConversationId, setSelectedConversationId] = useState<string>();

  const insightsQuery = useMinerCommentInsightsPage({
    status: status || undefined,
    keyword: deferredKeyword || undefined,
    bvid: deferredBvid || undefined,
    onlyMemeCandidates,
    onlyInsiderKnowledge,
    limit: pageSize,
    offset: (page - 1) * pageSize,
  });
  const insightDetailQuery = useMinerCommentInsightDetail(selectedInsightId);
  const selectedInsight =
    insightDetailQuery.data ??
    insightsQuery.data?.items.find((item) => item.insight_id === selectedInsightId);

  useEffect(() => {
    const items = insightsQuery.data?.items ?? [];
    if (!items.length) {
      if (selectedInsightId) {
        setSelectedInsightId(undefined);
      }
      return;
    }
    if (!selectedInsightId) {
      setSelectedInsightId(items[0].insight_id);
    }
  }, [insightsQuery.data, selectedInsightId]);

  const sameVideoInsightsQuery = useMinerCommentInsightsPage({
    bvid: selectedInsight?.bvid,
    limit: 100,
    offset: 0,
    enabled: Boolean(selectedInsight?.bvid),
  });
  const sameVideoInsightItems = sameVideoInsightsQuery.data?.items ?? [];

  const conversationsQuery = useAgentConversations({
    agentName: "miner",
    word: selectedInsight?.bvid,
    limit: 20,
    offset: 0,
  });
  const conversationItems = conversationsQuery.data?.items ?? [];

  useEffect(() => {
    if (!conversationItems.length) {
      if (selectedConversationId) {
        setSelectedConversationId(undefined);
      }
      return;
    }
    if (!selectedConversationId || !conversationItems.some((item) => item.id === selectedConversationId)) {
      setSelectedConversationId(conversationItems[0].id);
    }
  }, [conversationItems, selectedConversationId]);

  const conversationDetailQuery = useAgentConversation(selectedConversationId);
  const videoContext = selectedInsight?.video_context ?? {};

  return (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={10}>
        <PageSection
          title="评论线索"
          subtitle="这是 Miner Stage 1 的结果页：先看评论初筛，再决定哪些评论会进入证据包生成。"
          extra={
            <Space wrap>
              <Tag color="blue">当前 {insightsQuery.data?.total ?? 0} 条</Tag>
            </Space>
          }
        >
          <Space direction="vertical" size={12} style={{ width: "100%" }}>
            <Space wrap style={{ width: "100%" }}>
              <Select
                allowClear
                placeholder="状态"
                value={status || undefined}
                style={{ minWidth: 150 }}
                options={[
                    { label: "待生成证据包", value: "pending_bundle" },
                    { label: "生成中", value: "bundling" },
                    { label: "已生成证据包", value: "bundled" },
                    { label: "证据包失败", value: "bundle_failed" },
                    { label: "已淘汰", value: "discarded" },
                ]}
                onChange={(value) => {
                  setStatus(value ?? "");
                  setPage(1);
                }}
              />
              <Input
                allowClear
                placeholder="搜索标题、评论或理由"
                value={keywordInput}
                style={{ minWidth: 220 }}
                onChange={(event) => {
                  setKeywordInput(event.target.value);
                  setPage(1);
                }}
              />
              <Input
                allowClear
                placeholder="按 BVID 过滤"
                value={bvidInput}
                style={{ minWidth: 180 }}
                onChange={(event) => {
                  setBvidInput(event.target.value);
                  setPage(1);
                }}
              />
            </Space>

            <Space wrap>
              <Checkbox
                checked={onlyMemeCandidates}
                onChange={(event) => {
                  setOnlyMemeCandidates(event.target.checked);
                  setPage(1);
                }}
              >
                仅潜在梗
              </Checkbox>
              <Checkbox
                checked={onlyInsiderKnowledge}
                onChange={(event) => {
                  setOnlyInsiderKnowledge(event.target.checked);
                  setPage(1);
                }}
              >
                仅圈内知识
              </Checkbox>
            </Space>

            {insightsQuery.isLoading ? (
              <Spin />
            ) : insightsQuery.error ? (
              <Alert type="error" message="Miner 列表加载失败" description={String(insightsQuery.error)} />
            ) : (
              <>
                <List
                  dataSource={insightsQuery.data?.items ?? []}
                  locale={{ emptyText: <Empty description="暂无评论线索" /> }}
                  renderItem={(item) => (
                    <List.Item
                      style={{
                        cursor: "pointer",
                        paddingInline: 12,
                        borderRadius: 12,
                        background:
                          item.insight_id === selectedInsightId ? "rgba(59, 130, 246, 0.08)" : "transparent",
                      }}
                      onClick={() => setSelectedInsightId(item.insight_id)}
                    >
                      <List.Item.Meta
                        title={
                          <Space wrap>
                            <Typography.Text strong>{item.comment_text || "暂无评论正文"}</Typography.Text>
                            <MinerInsightStatusTag status={item.status} />
                            {item.is_meme_candidate ? <Tag color="green">潜在梗</Tag> : null}
                            {item.is_insider_knowledge ? <Tag color="gold">圈内知识</Tag> : null}
                          </Space>
                        }
                        description={
                          <Space direction="vertical" size={4} style={{ width: "100%" }}>
                            <Typography.Text type="secondary">
                              {item.title || "无标题视频"} · {item.partition || "未分区"} · {item.bvid}
                            </Typography.Text>
                            <Typography.Text type="secondary">
                              {item.collected_date || "--"} · {shortId(item.insight_id)}
                            </Typography.Text>
                            <Typography.Text type="secondary">
                              证据包：{item.bundle_id ? shortId(item.bundle_id) : "未生成"}
                            </Typography.Text>
                            <Typography.Text type="secondary">{item.reason || "暂无说明"}</Typography.Text>
                          </Space>
                        }
                      />
                      <Typography.Text>{Number(item.confidence || 0).toFixed(2)}</Typography.Text>
                    </List.Item>
                  )}
                />
                <Pagination
                  align="end"
                  current={page}
                  pageSize={pageSize}
                  total={insightsQuery.data?.total ?? 0}
                  showSizeChanger
                  showTotal={(total) => `共 ${total} 条`}
                  onChange={(nextPage, nextPageSize) => {
                    setPage(nextPage);
                    setPageSize(nextPageSize);
                  }}
                />
              </>
            )}
          </Space>
        </PageSection>
      </Col>

      <Col xs={24} xl={14}>
        <PageSection
          title={selectedInsight ? `评论线索 ${shortId(selectedInsight.insight_id)}` : "评论线索详情"}
          subtitle={
            selectedInsight
              ? `${selectedInsight.title || "无标题视频"} · ${selectedInsight.bvid}`
              : "逐条确认评论、视频上下文和入队去向，判断这条线索是否已经顺利进入 Stage 2。"
          }
        >
          {!selectedInsightId ? (
            <Empty description="左侧选择一条 Miner 结果" />
          ) : insightDetailQuery.isLoading ? (
            <Spin />
          ) : insightDetailQuery.error ? (
            <Alert type="error" message="Miner 详情加载失败" description={String(insightDetailQuery.error)} />
          ) : !selectedInsight ? (
            <Empty description="详情不存在" />
          ) : (
            <Tabs
              items={[
                {
                  key: "overview",
                  label: "概览",
                  children: (
                    <Space direction="vertical" size={12} style={{ width: "100%" }}>
                      <DescriptionsBlock insight={selectedInsight} />
                      <div>
                        <Typography.Text strong>Stage 2 去向</Typography.Text>
                        <div style={{ marginTop: 8 }}>
                          <Space wrap>
                            <MinerInsightStatusTag status={selectedInsight.status} />
                            {selectedInsight.bundle_id ? <Tag color="success">bundle {shortId(selectedInsight.bundle_id)}</Tag> : null}
                            {selectedInsight.bundle_status ? <Tag color="blue">{selectedInsight.bundle_status}</Tag> : null}
                          </Space>
                        </div>
                      </div>
                      <div>
                        <Typography.Text strong>评论正文</Typography.Text>
                        <Typography.Paragraph style={{ marginTop: 8, whiteSpace: "pre-wrap" }}>
                          {selectedInsight.comment_text || "暂无评论正文"}
                        </Typography.Paragraph>
                      </div>
                      <div>
                        <Typography.Text strong>Miner 理由</Typography.Text>
                        <Typography.Paragraph style={{ marginTop: 8 }}>
                          {selectedInsight.reason || "暂无说明"}
                        </Typography.Paragraph>
                      </div>
                      {selectedInsight.description ? (
                        <div>
                          <Typography.Text strong>视频简介</Typography.Text>
                          <Typography.Paragraph style={{ marginTop: 8, whiteSpace: "pre-wrap" }}>
                            {selectedInsight.description}
                          </Typography.Paragraph>
                        </div>
                      ) : null}
                    </Space>
                  ),
                },
                {
                  key: "same-video",
                  label: `同视频评论 (${sameVideoInsightsQuery.data?.total ?? 0})`,
                  children: sameVideoInsightsQuery.isLoading ? (
                    <Spin />
                  ) : sameVideoInsightsQuery.error ? (
                    <Alert
                      type="error"
                      message="同视频评论加载失败"
                      description={String(sameVideoInsightsQuery.error)}
                    />
                  ) : !sameVideoInsightItems.length ? (
                    <Empty description="这个 BVID 暂无其他 Miner 评论线索" />
                  ) : (
                    <List
                      dataSource={sameVideoInsightItems}
                      locale={{ emptyText: <Empty description="暂无同视频评论" /> }}
                      renderItem={(item) => (
                        <List.Item
                          style={{
                            cursor: "pointer",
                            paddingInline: 12,
                            borderRadius: 12,
                            background:
                              item.insight_id === selectedInsightId
                                ? "rgba(59, 130, 246, 0.08)"
                                : "transparent",
                          }}
                          onClick={() => setSelectedInsightId(item.insight_id)}
                        >
                          <List.Item.Meta
                            title={
                              <Space wrap>
                                <Typography.Text strong>{item.comment_text || "暂无评论正文"}</Typography.Text>
                                <MinerInsightStatusTag status={item.status} />
                                {item.is_meme_candidate ? <Tag color="green">潜在梗</Tag> : null}
                                {item.is_insider_knowledge ? <Tag color="gold">圈内知识</Tag> : null}
                              </Space>
                            }
                            description={
                              <Space direction="vertical" size={4} style={{ width: "100%" }}>
                                <Typography.Text type="secondary">
                                    {item.collected_date || "--"} · {shortId(item.insight_id)} · 置信度{" "}
                                    {Number(item.confidence || 0).toFixed(2)}
                                  </Typography.Text>
                                  <Typography.Text type="secondary">
                                    证据包：{item.bundle_id ? shortId(item.bundle_id) : "未生成"}
                                  </Typography.Text>
                                  <Typography.Text type="secondary">{item.reason || "暂无说明"}</Typography.Text>
                                </Space>
                              }
                          />
                        </List.Item>
                      )}
                    />
                  ),
                },
                {
                  key: "context",
                  label: "视频上下文",
                  children: (
                    <Space direction="vertical" size={12} style={{ width: "100%" }}>
                      <ContextBlock title="视频摘要" value={readContextText(videoContext, "summary")} />
                      <ContextBlock title="内容正文" value={readContextText(videoContext, "content_text")} />
                      <ContextBlock
                        title="字幕摘录"
                        value={readContextText(videoContext, "transcript_excerpt")}
                      />
                      <JsonPanel title="完整上下文 JSON" value={videoContext} />
                    </Space>
                  ),
                },
                {
                  key: "conversations",
                  label: `Stage 1 对话 (${conversationsQuery.data?.total ?? 0})`,
                  children: conversationsQuery.isLoading ? (
                    <Spin />
                  ) : conversationsQuery.error ? (
                    <Alert
                      type="error"
                      message="Miner 对话加载失败"
                      description={String(conversationsQuery.error)}
                    />
                  ) : !conversationItems.length ? (
                    <Empty description="这个 BVID 还没有 Miner 对话记录" />
                  ) : (
                    <Row gutter={[16, 16]}>
                      <Col xs={24} lg={10}>
                        <List
                          dataSource={conversationItems}
                          renderItem={(item) => (
                            <List.Item
                              style={{
                                cursor: "pointer",
                                paddingInline: 12,
                                borderRadius: 12,
                                background:
                                  item.id === selectedConversationId
                                    ? "rgba(15, 118, 110, 0.08)"
                                    : "transparent",
                              }}
                              onClick={() => setSelectedConversationId(item.id)}
                            >
                              <List.Item.Meta
                                title={
                                  <Space wrap>
                                    <Typography.Text strong>{formatDateTime(item.started_at)}</Typography.Text>
                                    <ConversationStatusTag status={item.status} />
                                  </Space>
                                }
                                description={
                                  <Space direction="vertical" size={4} style={{ width: "100%" }}>
                                    <Typography.Text type="secondary">
                                      run: {shortId(item.run_id)} · 消息 {item.message_count ?? 0} 条
                                    </Typography.Text>
                                    <Typography.Text>{item.summary || "暂无摘要"}</Typography.Text>
                                  </Space>
                                }
                              />
                            </List.Item>
                          )}
                        />
                      </Col>
                      <Col xs={24} lg={14}>
                        {conversationDetailQuery.isLoading ? (
                          <Spin />
                        ) : conversationDetailQuery.error ? (
                          <Alert
                            type="error"
                            message="对话详情加载失败"
                            description={String(conversationDetailQuery.error)}
                          />
                        ) : !conversationDetailQuery.data ? (
                          <Empty description="未选中对话" />
                        ) : (
                          <Space direction="vertical" size={12} style={{ width: "100%" }}>
                            <Descriptions
                              size="small"
                              bordered
                              column={1}
                              items={[
                                {
                                  key: "status",
                                  label: "状态",
                                  children: (
                                    <ConversationStatusTag status={conversationDetailQuery.data.status} />
                                  ),
                                },
                                {
                                  key: "run",
                                  label: "运行记录",
                                  children: shortId(conversationDetailQuery.data.run_id),
                                },
                                {
                                  key: "started",
                                  label: "开始时间",
                                  children: formatDateTime(conversationDetailQuery.data.started_at),
                                },
                                {
                                  key: "finished",
                                  label: "结束时间",
                                  children: formatDateTime(conversationDetailQuery.data.finished_at),
                                },
                              ]}
                            />
                            {conversationDetailQuery.data.error_message ? (
                              <Alert
                                type="error"
                                message="执行错误"
                                description={conversationDetailQuery.data.error_message}
                              />
                            ) : null}
                            <JsonPanel title="最终输出" value={conversationDetailQuery.data.output} />
                            <JsonPanel title="完整消息" value={conversationDetailQuery.data.messages} />
                          </Space>
                        )}
                      </Col>
                    </Row>
                  ),
                },
              ]}
            />
          )}
        </PageSection>
      </Col>
    </Row>
  );
}

function DescriptionsBlock({ insight }: { insight: NonNullable<ReturnType<typeof useMinerCommentInsightDetail>["data"]> }) {
  return (
    <Descriptions
      size="small"
      bordered
      column={{ xs: 1, sm: 2 }}
      items={[
        {
          key: "status",
          label: "状态",
          children: <MinerInsightStatusTag status={insight.status} />,
        },
        {
          key: "confidence",
          label: "置信度",
          children: Number(insight.confidence || 0).toFixed(2),
        },
        {
          key: "bvid",
          label: "BVID",
          children: insight.bvid,
        },
        {
          key: "collected_date",
          label: "采集日期",
          children: insight.collected_date || "--",
        },
        {
          key: "partition",
          label: "分区",
          children: insight.partition || "--",
        },
        {
          key: "flags",
          label: "命中标签",
          children: (
            <Space wrap>
              {insight.is_meme_candidate ? <Tag color="green">潜在梗</Tag> : null}
              {insight.is_insider_knowledge ? <Tag color="gold">圈内知识</Tag> : null}
              {!insight.is_meme_candidate && !insight.is_insider_knowledge ? <Tag>普通评论</Tag> : null}
            </Space>
          ),
        },
        {
          key: "tags",
          label: "视频标签",
          children: insight.tags?.length ? (
            <Space wrap>
              {insight.tags.map((tag) => (
                <Tag key={tag}>{tag}</Tag>
              ))}
            </Space>
          ) : (
            "--"
          ),
        },
        {
          key: "video",
          label: "原视频",
          children: insight.url ? (
            <Typography.Link href={insight.url} target="_blank">
              打开视频
            </Typography.Link>
          ) : (
            "--"
          ),
        },
        {
          key: "created",
          label: "创建时间",
          children: formatDateTime(insight.created_at),
        },
        {
          key: "updated",
          label: "更新时间",
          children: formatDateTime(insight.updated_at),
        },
      ]}
    />
  );
}

function ContextBlock({ title, value }: { title: string; value: string }) {
  return (
    <div>
      <Typography.Text strong>{title}</Typography.Text>
      <Typography.Paragraph style={{ marginTop: 8, whiteSpace: "pre-wrap" }}>
        {value || "暂无内容"}
      </Typography.Paragraph>
    </div>
  );
}

function readContextText(context: Record<string, unknown>, key: string) {
  const value = context[key];
  return typeof value === "string" ? value : "";
}
