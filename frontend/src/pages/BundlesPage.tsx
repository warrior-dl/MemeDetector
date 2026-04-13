import {
  Alert,
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
import { useResearchBundleDetail, useResearchBundlesPage } from "../features/research/hooks";
import { PageSection } from "../ui/PageSection";
import { BundleStatusTag, HypothesisStatusTag, ResearchDecisionTag } from "../ui/StatusTags";
import { formatDateTime } from "../utils/format";

export function BundlesPage() {
  const [status, setStatus] = useState<string>();
  const [keywordInput, setKeywordInput] = useState("");
  const [keyword, setKeyword] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [selectedBundleId, setSelectedBundleId] = useState<string>();

  const bundlesQuery = useResearchBundlesPage({
    status,
    keyword,
    limit: pageSize,
    offset: (page - 1) * pageSize,
  });
  const bundleDetailQuery = useResearchBundleDetail(selectedBundleId);

  useEffect(() => {
    const items = bundlesQuery.data?.items ?? [];
    if (!items.length) {
      if (selectedBundleId) {
        setSelectedBundleId(undefined);
      }
      return;
    }
    if (!selectedBundleId || !items.some((item) => item.bundle_id === selectedBundleId)) {
      setSelectedBundleId(items[0].bundle_id);
    }
  }, [bundlesQuery.data, selectedBundleId]);

  const selectedBundleSummary =
    bundlesQuery.data?.items.find((item) => item.bundle_id === selectedBundleId) ?? undefined;
  const bundle = bundleDetailQuery.data?.bundle;
  const decisions = bundleDetailQuery.data?.decisions ?? [];
  const spanMap = Object.fromEntries((bundle?.spans ?? []).map((item) => [item.span_id, item]));

  return (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={9}>
        <PageSection
          title="证据包列表"
          subtitle="这是 Miner Stage 2 的结果页：按评论文本和状态筛选，查看切分、假设、证据与裁决。"
        >
          <Space direction="vertical" size={12} style={{ width: "100%" }}>
            <Space wrap style={{ width: "100%" }}>
              <Select
                allowClear
                placeholder="状态"
                value={status}
                style={{ minWidth: 160 }}
                options={[
                  { label: "待研判", value: "bundled" },
                  { label: "已研判", value: "researched" },
                ]}
                onChange={(value) => {
                  setStatus(value);
                  setPage(1);
                }}
              />
              <Input.Search
                allowClear
                placeholder="搜索评论、原因或 BVID"
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

            {bundlesQuery.isLoading ? (
              <Spin />
            ) : bundlesQuery.error ? (
              <Alert type="error" message="证据包加载失败" description={String(bundlesQuery.error)} />
            ) : (
              <>
                <List
                  dataSource={bundlesQuery.data?.items ?? []}
                  locale={{ emptyText: <Empty description="暂无证据包" /> }}
                  renderItem={(item) => (
                    <List.Item
                      style={{
                        cursor: "pointer",
                        paddingInline: 12,
                        borderRadius: 12,
                        background:
                          item.bundle_id === selectedBundleId
                            ? "rgba(15, 118, 110, 0.08)"
                            : "transparent",
                      }}
                      onClick={() => setSelectedBundleId(item.bundle_id)}
                    >
                      <List.Item.Meta
                        title={
                          <Space wrap>
                            <Typography.Text strong>{item.comment_text || item.bundle_id}</Typography.Text>
                            <BundleStatusTag status={item.status} />
                            {item.latest_decision ? (
                              <ResearchDecisionTag decision={item.latest_decision} />
                            ) : null}
                          </Space>
                        }
                        description={
                          <Space direction="vertical" size={4} style={{ width: "100%" }}>
                            <Typography.Text type="secondary">
                              {item.reason || item.miner_summary_reason || "暂无说明"}
                            </Typography.Text>
                            <Typography.Text type="secondary">
                              {item.bvid} · 假设 {item.hypothesis_count ?? 0} · 证据 {item.evidence_count ?? 0}
                            </Typography.Text>
                          </Space>
                        }
                      />
                      <Typography.Text>{Number(item.signal_score || 0).toFixed(2)}</Typography.Text>
                    </List.Item>
                  )}
                />
                <Pagination
                  align="end"
                  current={page}
                  pageSize={pageSize}
                  total={bundlesQuery.data?.total ?? 0}
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
          title={bundle?.insight.comment_text || selectedBundleSummary?.comment_text || "证据包详情"}
          subtitle="以评论为核心单元，拆开模板、实体填槽和来源证据。"
        >
          {!selectedBundleId ? (
            <Empty description="左侧选择一个证据包" />
          ) : bundleDetailQuery.isLoading ? (
            <Spin />
          ) : bundleDetailQuery.error ? (
            <Alert type="error" message="证据包详情加载失败" description={String(bundleDetailQuery.error)} />
          ) : !bundle ? (
            <Empty description="证据包不存在" />
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
                            children: <BundleStatusTag status={bundle.insight.status} />,
                          },
                          {
                            key: "score",
                            label: "信号分",
                            children: Number(bundle.insight.signal_score || 0).toFixed(2),
                          },
                          {
                            key: "worth",
                            label: "是否值得深挖",
                            children: bundle.insight.worth_investigating ? "是" : "否",
                          },
                          {
                            key: "bvid",
                            label: "BVID",
                            children: bundle.insight.bvid,
                          },
                          {
                            key: "detected_at",
                            label: "采集日期",
                            children: formatDateTime(bundle.insight.collected_date),
                          },
                          {
                            key: "recommended",
                            label: "Miner 推荐假设",
                            children: bundle.miner_summary.recommended_hypothesis_id || "--",
                          },
                          {
                            key: "hypotheses",
                            label: "假设数",
                            children: String(bundle.hypotheses.length),
                          },
                          {
                            key: "evidences",
                            label: "证据数",
                            children: String(bundle.evidences.length),
                          },
                        ]}
                      />

                      <Alert
                        type="info"
                        showIcon
                        message="Miner 摘要"
                        description={bundle.miner_summary.reason || bundle.insight.reason || "暂无摘要"}
                      />

                      <Space wrap>
                        {bundle.video_refs.map((video) => (
                          <Tag key={`${video.bvid}-${video.collected_date}`} color="processing">
                            {video.partition || "未知分区"} · {video.bvid}
                          </Tag>
                        ))}
                      </Space>
                    </Space>
                  ),
                },
                {
                  key: "spans",
                  label: "Spans",
                  children: (
                    <List
                      dataSource={bundle.spans}
                      locale={{ emptyText: <Empty description="暂无切分结果" /> }}
                      renderItem={(item) => (
                        <List.Item>
                          <List.Item.Meta
                            title={
                              <Space wrap>
                                <Tag color={item.is_primary ? "gold" : "default"}>{item.span_type}</Tag>
                                <Typography.Text strong>{item.raw_text}</Typography.Text>
                                <Typography.Text type="secondary">
                                  {item.char_start ?? "-"} ~ {item.char_end ?? "-"}
                                </Typography.Text>
                              </Space>
                            }
                            description={
                              <Space direction="vertical" size={4}>
                                <Typography.Text type="secondary">
                                  归一化：{item.normalized_text}
                                </Typography.Text>
                                <Typography.Text type="secondary">
                                  置信度 {Number(item.confidence || 0).toFixed(2)} · 查询优先级 {item.query_priority}
                                </Typography.Text>
                                <Typography.Text>{item.reason || "暂无说明"}</Typography.Text>
                              </Space>
                            }
                          />
                        </List.Item>
                      )}
                    />
                  ),
                },
                {
                  key: "hypotheses",
                  label: "Hypotheses",
                  children: (
                    <List
                      dataSource={bundle.hypotheses}
                      locale={{ emptyText: <Empty description="暂无假设" /> }}
                      renderItem={(item) => {
                        const relatedSpanIds = (bundle.hypothesis_spans ?? [])
                          .filter((link) => link.hypothesis_id === item.hypothesis_id)
                          .map((link) => link.span_id);
                        return (
                          <List.Item>
                            <List.Item.Meta
                              title={
                                <Space wrap>
                                  <Typography.Text strong>{item.candidate_title}</Typography.Text>
                                  <Tag>{item.hypothesis_type}</Tag>
                                  <HypothesisStatusTag status={item.status} />
                                </Space>
                              }
                              description={
                                <Space direction="vertical" size={4}>
                                  <Typography.Text>{item.miner_opinion || "暂无 Miner 意见"}</Typography.Text>
                                  <Typography.Text type="secondary">
                                    support {Number(item.support_score || 0).toFixed(2)} · counter{" "}
                                    {Number(item.counter_score || 0).toFixed(2)} · uncertainty{" "}
                                    {Number(item.uncertainty_score || 0).toFixed(2)}
                                  </Typography.Text>
                                  <Typography.Text type="secondary">
                                    建议动作：{item.suggested_action}
                                  </Typography.Text>
                                  <Space wrap>
                                    {relatedSpanIds.map((spanId) => (
                                      <Tag key={spanId} color="blue">
                                        {spanMap[spanId]?.raw_text || spanId}
                                      </Tag>
                                    ))}
                                  </Space>
                                </Space>
                              }
                            />
                          </List.Item>
                        );
                      }}
                    />
                  ),
                },
                {
                  key: "evidences",
                  label: "Evidences",
                  children: (
                    <List
                      dataSource={bundle.evidences}
                      locale={{ emptyText: <Empty description="暂无证据" /> }}
                      renderItem={(item) => (
                        <List.Item>
                          <List.Item.Meta
                            title={
                              <Space wrap>
                                <Tag>{item.query_mode}</Tag>
                                <Tag color="purple">{item.evidence_direction}</Tag>
                                <Typography.Text strong>{item.query}</Typography.Text>
                              </Space>
                            }
                            description={
                              <Space direction="vertical" size={4}>
                                <Typography.Link href={item.source_url} target="_blank">
                                  {item.source_title || item.source_url || "未命名来源"}
                                </Typography.Link>
                                <Typography.Text>{item.snippet || "暂无摘要"}</Typography.Text>
                                <Typography.Text type="secondary">
                                  来源 {item.source_kind} · 强度 {Number(item.evidence_strength || 0).toFixed(2)}
                                </Typography.Text>
                              </Space>
                            }
                          />
                        </List.Item>
                      )}
                    />
                  ),
                },
                {
                  key: "decisions",
                  label: "Decisions",
                  children: decisions.length ? (
                    <Space direction="vertical" size={12} style={{ width: "100%" }}>
                      {decisions.map((item) => (
                        <PageSection
                          key={item.decision_id}
                          title={item.final_title || item.target_record_id || item.decision_id}
                          subtitle={item.reason || "暂无裁决说明"}
                        >
                          <Descriptions
                            size="small"
                            bordered
                            column={{ xs: 1, sm: 2 }}
                            items={[
                              {
                                key: "decision",
                                label: "结论",
                                children: <ResearchDecisionTag decision={item.decision} />,
                              },
                              {
                                key: "confidence",
                                label: "置信度",
                                children: Number(item.confidence || 0).toFixed(2),
                              },
                              {
                                key: "record",
                                label: "目标词条 ID",
                                children: item.target_record_id || "--",
                              },
                              {
                                key: "created_at",
                                label: "裁决时间",
                                children: item.created_at || "--",
                              },
                            ]}
                          />
                        </PageSection>
                      ))}
                    </Space>
                  ) : (
                    <Empty description="暂无裁决结果" />
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
