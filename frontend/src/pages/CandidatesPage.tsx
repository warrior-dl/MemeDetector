import {
  Alert,
  Button,
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
import { useEffect, useState } from "react";
import {
  useAgentConversation,
  useAgentConversations,
  useCandidatesPage,
  useCandidateSources,
  useVerifyCandidate,
} from "../features/candidates/hooks";
import { PageSection } from "../ui/PageSection";
import { JsonPanel } from "../ui/JsonPanel";
import { CandidateStatusTag, ConversationStatusTag } from "../ui/StatusTags";
import { formatDateTime, shortId } from "../utils/format";

export function CandidatesPage() {
  const [status, setStatus] = useState<string>();
  const [keywordInput, setKeywordInput] = useState("");
  const [keyword, setKeyword] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [selectedWord, setSelectedWord] = useState<string>();
  const [selectedConversationId, setSelectedConversationId] = useState<string>();

  const candidatesQuery = useCandidatesPage({
    status,
    keyword,
    limit: pageSize,
    offset: (page - 1) * pageSize,
  });

  useEffect(() => {
    const items = candidatesQuery.data?.items ?? [];
    if (!items.length) {
      if (selectedWord) {
        setSelectedWord(undefined);
      }
      return;
    }
    if (!selectedWord || !items.some((item) => item.word === selectedWord)) {
      setSelectedWord(items[0].word);
    }
  }, [candidatesQuery.data, selectedWord]);

  const sourcesQuery = useCandidateSources(selectedWord);
  const conversationsQuery = useAgentConversations({
    agentName: "researcher",
    word: selectedWord,
    limit: 20,
    offset: 0,
  });

  useEffect(() => {
    const items = conversationsQuery.data?.items ?? [];
    if (!items.length) {
      if (selectedConversationId) {
        setSelectedConversationId(undefined);
      }
      return;
    }
    if (!selectedConversationId || !items.some((item) => item.id === selectedConversationId)) {
      setSelectedConversationId(items[0].id);
    }
  }, [conversationsQuery.data, selectedConversationId]);

  const conversationDetailQuery = useAgentConversation(selectedConversationId);
  const verifyMutation = useVerifyCandidate();
  const selectedCandidate =
    candidatesQuery.data?.items.find((item) => item.word === selectedWord) ?? sourcesQuery.data?.candidate;
  const conversationItems = conversationsQuery.data?.items ?? [];

  return (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={9}>
        <PageSection
          title="候选列表"
          subtitle="按状态和关键字筛选，直接进入右侧做溯源、看对话和人工审核。"
          extra={
            <Button onClick={() => candidatesQuery.refetch()} loading={candidatesQuery.isFetching}>
              刷新
            </Button>
          }
        >
          <Space direction="vertical" size={12} style={{ width: "100%" }}>
            <Space wrap style={{ width: "100%" }}>
              <Select
                allowClear
                placeholder="状态"
                value={status}
                style={{ minWidth: 140 }}
                options={[
                  { label: "待处理", value: "pending" },
                  { label: "已接受", value: "accepted" },
                  { label: "已拒绝", value: "rejected" },
                ]}
                onChange={(value) => {
                  setStatus(value);
                  setPage(1);
                }}
              />
              <Input.Search
                allowClear
                placeholder="搜索候选词、解释或评论样本"
                value={keywordInput}
                style={{ minWidth: 260 }}
                onChange={(event) => setKeywordInput(event.target.value)}
                onSearch={(value) => {
                  setKeyword(value.trim());
                  setKeywordInput(value);
                  setPage(1);
                }}
              />
            </Space>

            {candidatesQuery.isLoading ? (
              <Spin />
            ) : candidatesQuery.error ? (
              <Alert type="error" message="候选加载失败" description={String(candidatesQuery.error)} />
            ) : (
              <>
                <List
                  dataSource={candidatesQuery.data?.items ?? []}
                  locale={{ emptyText: <Empty description="暂无候选" /> }}
                  renderItem={(item) => (
                    <List.Item
                      style={{
                        cursor: "pointer",
                        paddingInline: 12,
                        borderRadius: 12,
                        background: item.word === selectedWord ? "rgba(15, 118, 110, 0.08)" : "transparent",
                      }}
                      onClick={() => setSelectedWord(item.word)}
                    >
                      <List.Item.Meta
                        title={
                          <Space wrap>
                            <Typography.Text strong>{item.word}</Typography.Text>
                            <CandidateStatusTag status={item.status} />
                            {item.is_new_word ? <Tag color="blue">新词</Tag> : null}
                          </Space>
                        }
                        description={item.explanation || item.sample_comments || "暂无说明"}
                      />
                      <Typography.Text>{Number(item.score || 0).toFixed(2)}</Typography.Text>
                    </List.Item>
                  )}
                />
                <Pagination
                  align="end"
                  current={page}
                  pageSize={pageSize}
                  total={candidatesQuery.data?.total ?? 0}
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

      <Col xs={24} xl={15}>
        <PageSection
          title={selectedCandidate?.word || "候选详情"}
          subtitle="把来源线索、关联视频和评论命中整合在一个工作台里。"
        >
          {!selectedWord ? (
            <Empty description="左侧选择一个候选词" />
          ) : sourcesQuery.isLoading ? (
            <Spin />
          ) : sourcesQuery.error ? (
            <Alert type="error" message="候选详情加载失败" description={String(sourcesQuery.error)} />
          ) : (
            <Tabs
              items={[
                {
                  key: "overview",
                  label: "概览",
                  children: (
                    <Space direction="vertical" size={12} style={{ width: "100%" }}>
                      <Descriptions
                        size="small"
                        bordered
                        column={{ xs: 1, sm: 2 }}
                        items={[
                          {
                            key: "status",
                            label: "状态",
                            children: <CandidateStatusTag status={selectedCandidate?.status} />,
                          },
                          {
                            key: "score",
                            label: "分数",
                            children: Number(selectedCandidate?.score || 0).toFixed(2),
                          },
                          {
                            key: "detected_at",
                            label: "检测时间",
                            children: formatDateTime(selectedCandidate?.detected_at),
                          },
                          {
                            key: "refs",
                            label: "关联视频",
                            children: String(sourcesQuery.data?.video_refs?.length ?? 0),
                          },
                          {
                            key: "insights",
                            label: "来源线索",
                            children: String(sourcesQuery.data?.source_insights?.length ?? 0),
                          },
                          {
                            key: "conversations",
                            label: "Research 对话",
                            children: String(conversationItems.length),
                          },
                        ]}
                      />
                      <div>
                        <Typography.Text strong>候选解释</Typography.Text>
                        <Typography.Paragraph style={{ marginTop: 8 }}>
                          {selectedCandidate?.explanation || "暂无解释"}
                        </Typography.Paragraph>
                      </div>
                      <div>
                        <Typography.Text strong>评论样本</Typography.Text>
                        <Typography.Paragraph
                          type="secondary"
                          style={{ marginTop: 8, whiteSpace: "pre-wrap" }}
                        >
                          {selectedCandidate?.sample_comments || "暂无评论样本"}
                        </Typography.Paragraph>
                      </div>
                    </Space>
                  ),
                },
                {
                  key: "video-refs",
                  label: `关联视频 (${sourcesQuery.data?.video_refs?.length ?? 0})`,
                  children: (
                    <List
                      dataSource={sourcesQuery.data?.video_refs ?? []}
                      locale={{ emptyText: <Empty description="没有关联视频" /> }}
                      renderItem={(item) => (
                        <List.Item>
                          <List.Item.Meta
                            title={
                              <Space wrap>
                                <Typography.Text strong>{item.title || item.bvid}</Typography.Text>
                                <Tag>{item.partition || "未分区"}</Tag>
                                {item.url ? (
                                  <Typography.Link href={item.url} target="_blank">
                                    打开原视频
                                  </Typography.Link>
                                ) : null}
                              </Space>
                            }
                            description={
                              <Space direction="vertical" size={6} style={{ width: "100%" }}>
                                <Typography.Text type="secondary">
                                  BVID: {item.bvid} · 匹配评论 {item.matched_comment_count ?? 0} 条
                                </Typography.Text>
                                {item.matched_comments?.length ? (
                                  <Typography.Paragraph
                                    style={{ marginBottom: 0, whiteSpace: "pre-wrap" }}
                                  >
                                    {item.matched_comments.map((comment) => `- ${comment}`).join("\n")}
                                  </Typography.Paragraph>
                                ) : null}
                              </Space>
                            }
                          />
                        </List.Item>
                      )}
                    />
                  ),
                },
                {
                  key: "source-insights",
                  label: `来源线索 (${sourcesQuery.data?.source_insights?.length ?? 0})`,
                  children: (
                    <List
                      dataSource={sourcesQuery.data?.source_insights ?? []}
                      locale={{ emptyText: <Empty description="没有来源评论线索" /> }}
                      renderItem={(item) => (
                        <List.Item>
                          <List.Item.Meta
                            title={
                              <Space wrap>
                                <Typography.Text strong>{item.title || item.bvid}</Typography.Text>
                                {item.is_meme_candidate ? <Tag color="green">潜在梗</Tag> : null}
                                {item.is_insider_knowledge ? <Tag color="gold">圈内知识</Tag> : null}
                                {item.matched_by_candidate_word ? <Tag color="cyan">命中候选词</Tag> : null}
                                {item.matched_by_video_ref_comments ? <Tag color="purple">命中视频评论</Tag> : null}
                              </Space>
                            }
                            description={
                              <Space direction="vertical" size={6} style={{ width: "100%" }}>
                                <Typography.Text>{item.comment_text || "暂无评论正文"}</Typography.Text>
                                <Typography.Text type="secondary">
                                  {item.reason || "暂无说明"}
                                </Typography.Text>
                              </Space>
                            }
                          />
                          <Typography.Text>{Number(item.confidence || 0).toFixed(2)}</Typography.Text>
                        </List.Item>
                      )}
                    />
                  ),
                },
                {
                  key: "conversations",
                  label: `Researcher 对话 (${conversationsQuery.data?.total ?? 0})`,
                  children: !selectedWord ? (
                    <Empty description="先选择候选词" />
                  ) : conversationsQuery.isLoading ? (
                    <Spin />
                  ) : conversationsQuery.error ? (
                    <Alert
                      type="error"
                      message="Researcher 对话加载失败"
                      description={String(conversationsQuery.error)}
                    />
                  ) : !conversationItems.length ? (
                    <Empty description="这个候选词还没有 Researcher 对话" />
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
                                    ? "rgba(194, 65, 12, 0.08)"
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
                            <div>
                              <Typography.Text strong>摘要</Typography.Text>
                              <Typography.Paragraph style={{ marginTop: 8 }}>
                                {conversationDetailQuery.data.summary || "暂无摘要"}
                              </Typography.Paragraph>
                            </div>
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
                {
                  key: "review",
                  label: "审核操作",
                  children: (
                    <Space direction="vertical" size={16} style={{ width: "100%" }}>
                      <Alert
                        type="info"
                        message="人工审核会直接更新候选状态"
                        description="接受/拒绝后会刷新候选列表、来源线索和统计面板。正式入库仍以 Research 流程产出的梗库词条为准。"
                      />
                      <Descriptions
                        size="small"
                        bordered
                        column={1}
                        items={[
                          {
                            key: "word",
                            label: "候选词",
                            children: selectedCandidate?.word || "--",
                          },
                          {
                            key: "status",
                            label: "当前状态",
                            children: <CandidateStatusTag status={selectedCandidate?.status} />,
                          },
                          {
                            key: "score",
                            label: "当前分数",
                            children: Number(selectedCandidate?.score || 0).toFixed(2),
                          },
                        ]}
                      />
                      <Space wrap>
                        <Button
                          type="primary"
                          onClick={() => {
                            if (selectedCandidate?.word) {
                              verifyMutation.mutate({ word: selectedCandidate.word, action: "accept" });
                            }
                          }}
                          loading={verifyMutation.isPending && verifyMutation.variables?.action === "accept"}
                        >
                          接受候选
                        </Button>
                        <Button
                          danger
                          onClick={() => {
                            if (selectedCandidate?.word) {
                              verifyMutation.mutate({ word: selectedCandidate.word, action: "reject" });
                            }
                          }}
                          loading={verifyMutation.isPending && verifyMutation.variables?.action === "reject"}
                        >
                          拒绝候选
                        </Button>
                      </Space>
                      {verifyMutation.error ? (
                        <Alert
                          type="error"
                          message="审核提交失败"
                          description={String(verifyMutation.error)}
                        />
                      ) : null}
                    </Space>
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
