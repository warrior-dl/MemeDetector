import {
  Alert,
  App,
  Button,
  Descriptions,
  Drawer,
  Empty,
  Image,
  Input,
  List,
  Pagination,
  Popconfirm,
  Select,
  Space,
  Table,
  Tabs,
  Tag,
  Typography,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import { useDeferredValue, useEffect, useMemo, useState } from "react";
import type {
  ScoutMediaAsset,
  ScoutRawVideoSummary,
} from "../data/types";
import {
  useScoutRawVideoDetail,
  useScoutRawVideosPage,
  useUpdateScoutRawVideoStage,
} from "../features/scout/hooks";
import { PageSection } from "../ui/PageSection";
import { formatOptionalDateTime } from "../utils/format";

export function ScoutPage() {
  const { message } = App.useApp();
  const [researchStatus, setResearchStatus] = useState<string>();
  const [partition, setPartition] = useState("");
  const [keywordInput, setKeywordInput] = useState("");
  const deferredKeyword = useDeferredValue(keywordInput.trim());
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [selectedVideo, setSelectedVideo] = useState<ScoutRawVideoSummary>();
  const [stageDraft, setStageDraft] = useState<string>();

  const videosQuery = useScoutRawVideosPage({
    researchStatus,
    partition: partition.trim() || undefined,
    keyword: deferredKeyword || undefined,
    limit: pageSize,
    offset: (page - 1) * pageSize,
  });
  const detailQuery = useScoutRawVideoDetail(selectedVideo?.bvid, selectedVideo?.collected_date);
  const updateStageMutation = useUpdateScoutRawVideoStage();
  const detail = detailQuery.data;

  useEffect(() => {
    const items = videosQuery.data?.items ?? [];
    if (!selectedVideo) {
      return;
    }

    if (
      !items.some(
        (item) =>
          item.bvid === selectedVideo.bvid && item.collected_date === selectedVideo.collected_date,
      )
    ) {
      setSelectedVideo(undefined);
    }
  }, [selectedVideo, videosQuery.data]);

  useEffect(() => {
    if (!detail) {
      setStageDraft(undefined);
      return;
    }
    setStageDraft(detail.pipeline_stage);
  }, [detail]);

  const columns = useMemo<ColumnsType<ScoutRawVideoSummary>>(
    () => [
      {
        title: "视频",
        dataIndex: "title",
        render: (_, record) => (
          <div>
            <Typography.Text strong>{record.title || record.bvid}</Typography.Text>
            <div style={{ color: "#6b7280", marginTop: 4 }}>
              {record.partition || "--"} · {record.bvid}
            </div>
          </div>
        ),
      },
      {
        title: "阶段",
        dataIndex: "pipeline_stage",
        width: 140,
        render: (value?: string) => <PipelineStageTag stage={value} />,
      },
      {
        title: "评论",
        dataIndex: "comment_count",
        width: 90,
        render: (value?: number) => value ?? 0,
      },
      {
        title: "高价值",
        dataIndex: "high_value_comment_count",
        width: 96,
        render: (value?: number) => value ?? 0,
      },
      {
        title: "证据包",
        dataIndex: "bundle_count",
        width: 96,
        render: (value?: number) => value ?? 0,
      },
      {
        title: "图片",
        dataIndex: "picture_count",
        width: 90,
        render: (value?: number) => value ?? 0,
      },
      {
        title: "采集时间",
        dataIndex: "updated_at",
        width: 180,
        render: (value?: string) => formatOptionalDateTime(value, "--"),
      },
    ],
    [],
  );

  const canSubmitStageChange =
    Boolean(detail?.bvid && detail?.collected_date && stageDraft) &&
    stageDraft !== detail?.pipeline_stage &&
    !updateStageMutation.isPending;

  return (
    <PageSection
      title="Scout 采集数据"
      subtitle="直接查看原始视频快照、评论样本和图片资产，确认 Scout 采到了什么，再决定是否继续排查 Miner。"
      extra={
        <Space wrap>
          <Select
            allowClear
            placeholder="Research 状态"
            value={researchStatus}
            style={{ width: 140 }}
            options={[
              { label: "待提取", value: "pending" },
              { label: "已提取", value: "processed" },
            ]}
            onChange={(value) => {
              setResearchStatus(value);
              setPage(1);
            }}
          />
          <Input
            allowClear
            placeholder="分区过滤，如鬼畜"
            value={partition}
            style={{ width: 160 }}
            onChange={(event) => {
              setPartition(event.target.value);
              setPage(1);
            }}
          />
          <Input.Search
            allowClear
            placeholder="搜索标题、描述或 BVID"
            value={keywordInput}
            style={{ width: 260 }}
            onChange={(event) => {
              setKeywordInput(event.target.value);
              setPage(1);
            }}
            onSearch={(value) => {
              setKeywordInput(value);
              setPage(1);
            }}
          />
          <Button onClick={() => videosQuery.refetch()} loading={videosQuery.isFetching}>
            刷新
          </Button>
        </Space>
      }
    >
      {videosQuery.error ? (
        <Alert type="error" message="Scout 列表加载失败" description={String(videosQuery.error)} />
      ) : (
        <>
          <Table
            rowKey={(record) => `${record.bvid}:${record.collected_date}`}
            loading={videosQuery.isLoading}
            columns={columns}
            dataSource={videosQuery.data?.items ?? []}
            locale={{ emptyText: <Empty description="暂无 Scout 采集数据" /> }}
            pagination={false}
            onRow={(record) => ({
              onClick: () => setSelectedVideo(record),
              style: { cursor: "pointer" },
            })}
          />
          <div style={{ marginTop: 16, display: "flex", justifyContent: "flex-end" }}>
            <Pagination
              current={page}
              pageSize={pageSize}
              total={videosQuery.data?.total ?? 0}
              showSizeChanger
              showTotal={(total) => `共 ${total} 条`}
              onChange={(nextPage, nextPageSize) => {
                setPage(nextPage);
                setPageSize(nextPageSize);
              }}
            />
          </div>
          <Drawer
            title={selectedVideo?.title || selectedVideo?.bvid || "Scout 详情"}
            placement="right"
            width={680}
            open={Boolean(selectedVideo)}
            onClose={() => setSelectedVideo(undefined)}
          >
            {!selectedVideo ? (
              <Empty description="请选择一条 Scout 记录" />
            ) : detailQuery.isLoading ? (
              <Typography.Text>加载中...</Typography.Text>
            ) : detailQuery.error ? (
              <Alert type="error" message="Scout 详情加载失败" description={String(detailQuery.error)} />
            ) : !detail ? (
              <Empty description="详情不存在" />
            ) : (
              <Tabs
                items={[
                  {
                    key: "overview",
                    label: "概览",
                    children: (
                      <Space direction="vertical" size={16} style={{ width: "100%" }}>
                        <Descriptions
                          bordered
                          size="small"
                          column={1}
                          items={[
                            {
                              key: "bvid",
                              label: "BVID",
                              children: detail.bvid,
                            },
                            {
                              key: "partition",
                              label: "分区",
                              children: detail.partition || "--",
                            },
                            {
                              key: "pipeline_stage",
                              label: "当前阶段",
                              children: <PipelineStageTag stage={detail.pipeline_stage} />,
                            },
                            {
                              key: "comment_count",
                              label: "评论数",
                              children: String(detail.comment_count ?? 0),
                            },
                            {
                              key: "high_value_comment_count",
                              label: "高价值评论",
                              children: String(detail.high_value_comment_count ?? 0),
                            },
                            {
                              key: "bundle_count",
                              label: "证据包数",
                              children: String(detail.bundle_count ?? 0),
                            },
                            {
                              key: "picture_count",
                              label: "图片数",
                              children: String(detail.picture_count ?? 0),
                            },
                            {
                              key: "comments_with_pictures",
                              label: "带图评论",
                              children: String(detail.comments_with_pictures ?? 0),
                            },
                            {
                              key: "miner_status",
                              label: "Miner 状态",
                              children: renderPlainStatus(detail.miner_status),
                            },
                            {
                              key: "miner_attempt_count",
                              label: "Miner 尝试次数",
                              children: String(detail.miner_attempt_count ?? 0),
                            },
                            {
                              key: "miner_last_error",
                              label: "Miner 最近错误",
                              children: detail.miner_last_error || "--",
                            },
                            {
                              key: "research_status",
                              label: "Research 状态",
                              children: renderPlainStatus(detail.research_status),
                            },
                            {
                              key: "updated_at",
                              label: "更新时间",
                              children: formatOptionalDateTime(detail.updated_at),
                            },
                          ]}
                        />
                        <div
                          style={{
                            padding: 16,
                            borderRadius: 12,
                            background: "#fffdf5",
                            border: "1px solid #f1e4b8",
                          }}
                        >
                          <Space direction="vertical" size={8} style={{ width: "100%" }}>
                            <Typography.Text strong>Miner 结果解释</Typography.Text>
                            <Space wrap>
                              <PipelineStageTag stage={detail.pipeline_stage} />
                              <ScoutOutcomeTag
                                highValueCommentCount={detail.high_value_comment_count}
                                bundleCount={detail.bundle_count}
                              />
                            </Space>
                            <Typography.Text type="secondary">
                              {buildScoutOutcomeDescription(detail)}
                            </Typography.Text>
                          </Space>
                        </div>
                        <div
                          style={{
                            padding: 16,
                            borderRadius: 12,
                            background: "#f7faf9",
                            border: "1px solid #d7ebe7",
                          }}
                        >
                          <Space direction="vertical" size={12} style={{ width: "100%" }}>
                            <Typography.Text strong>手动调整阶段</Typography.Text>
                            <Typography.Text type="secondary">
                              改到“仅 Scout”会重置该视频当天的 Miner 与 Research 状态。
                              改到“已 Miner”会保留评论初筛完成态，并把高价值评论重新放回待生成证据包。
                            </Typography.Text>
                            <Space wrap>
                              <Select
                                value={stageDraft}
                                style={{ width: 180 }}
                                options={[
                                  { label: "仅 Scout", value: "scouted" },
                                  { label: "已 Miner", value: "mined" },
                                  { label: "已进入 Research", value: "researched" },
                                ]}
                                onChange={setStageDraft}
                              />
                              <Popconfirm
                                title="确认更新阶段"
                                description={`将 ${detail.pipeline_stage || "--"} 改为 ${stageDraft || "--"}，并同步更新该视频当天的下游状态。`}
                                okText="确认"
                                cancelText="取消"
                                disabled={!canSubmitStageChange}
                                onConfirm={async () => {
                                  if (!detail?.bvid || !detail?.collected_date || !stageDraft) {
                                    return;
                                  }
                                  try {
                                    const updated = await updateStageMutation.mutateAsync({
                                      bvid: detail.bvid,
                                      collectedDate: detail.collected_date,
                                      stage: stageDraft as "scouted" | "mined" | "researched",
                                    });
                                    setSelectedVideo((current) =>
                                      current &&
                                      current.bvid === updated.bvid &&
                                      current.collected_date === updated.collected_date
                                        ? {
                                            ...current,
                                            pipeline_stage: updated.pipeline_stage,
                                            miner_status: updated.miner_status,
                                            miner_processed_at: updated.miner_processed_at,
                                            research_status: updated.research_status,
                                            research_started_at: updated.research_started_at,
                                            updated_at: updated.updated_at,
                                          }
                                        : current,
                                    );
                                    message.success(
                                      `阶段已更新，关联评论线索 ${updated.affected_insight_count} 条已同步处理。`,
                                    );
                                  } catch (error) {
                                    message.error(
                                      error instanceof Error ? error.message : "阶段更新失败",
                                    );
                                  }
                                }}
                              >
                                <Button
                                  type="primary"
                                  loading={updateStageMutation.isPending}
                                  disabled={!canSubmitStageChange}
                                >
                                  更新阶段
                                </Button>
                              </Popconfirm>
                            </Space>
                          </Space>
                        </div>
                        <div>
                          <Typography.Text strong>视频链接</Typography.Text>
                          <div style={{ marginTop: 8 }}>
                            {detail.video_url ? (
                              <Typography.Link href={detail.video_url} target="_blank">
                                {detail.video_url}
                              </Typography.Link>
                            ) : (
                              <Typography.Text type="secondary">暂无链接</Typography.Text>
                            )}
                          </div>
                        </div>
                        <div>
                          <Typography.Text strong>标签</Typography.Text>
                          <div style={{ marginTop: 8 }}>
                            {detail.tags?.length ? (
                              detail.tags.map((item) => <Tag key={item}>{item}</Tag>)
                            ) : (
                              <Typography.Text type="secondary">暂无标签</Typography.Text>
                            )}
                          </div>
                        </div>
                        <div>
                          <Typography.Text strong>视频简介</Typography.Text>
                          <Typography.Paragraph style={{ marginTop: 8 }}>
                            {detail.description || "暂无简介"}
                          </Typography.Paragraph>
                        </div>
                        <div>
                          <Typography.Text strong>评论样本</Typography.Text>
                          <List
                            size="small"
                            style={{ marginTop: 8 }}
                            dataSource={detail.comments ?? []}
                            locale={{ emptyText: <Empty description="暂无评论样本" /> }}
                            renderItem={(item) => <List.Item>{item}</List.Item>}
                          />
                        </div>
                      </Space>
                    ),
                  },
                  {
                    key: "comments",
                    label: `评论快照 (${detail.comment_snapshots?.length ?? 0})`,
                    children: (
                      <List
                        itemLayout="vertical"
                        dataSource={detail.comment_snapshots ?? []}
                        locale={{ emptyText: <Empty description="暂无结构化评论快照" /> }}
                        renderItem={(item) => (
                          <List.Item key={String(item.rpid)}>
                            <Space direction="vertical" size={10} style={{ width: "100%" }}>
                              <Space wrap>
                                <Typography.Text strong>{item.uname || "匿名用户"}</Typography.Text>
                                <Tag>赞 {item.like_count ?? 0}</Tag>
                                <Tag>回复 {item.reply_count ?? 0}</Tag>
                                {item.has_pictures ? <Tag color="gold">带图</Tag> : null}
                                <Typography.Text type="secondary">
                                  {formatOptionalDateTime(item.ctime, "--")}
                                </Typography.Text>
                              </Space>
                              <Typography.Paragraph style={{ margin: 0 }}>
                                {item.message || "--"}
                              </Typography.Paragraph>
                              {item.pictures?.length ? <CommentPictures pictures={item.pictures} /> : null}
                            </Space>
                          </List.Item>
                        )}
                      />
                    ),
                  },
                ]}
              />
            )}
          </Drawer>
        </>
      )}
    </PageSection>
  );
}

function PipelineStageTag({ stage }: { stage?: string }) {
  const color =
    stage === "researched"
      ? "purple"
      : stage === "miner_failed"
        ? "error"
        : stage === "mining"
          ? "processing"
          : stage === "mined"
            ? "cyan"
            : stage === "scouted"
              ? "blue"
              : "default";
  const label =
    stage === "researched"
      ? "已进入 Research"
      : stage === "miner_failed"
        ? "Miner 失败"
        : stage === "mining"
          ? "Miner 处理中"
          : stage === "mined"
            ? "已 Miner"
            : stage === "scouted"
              ? "仅 Scout"
              : "--";
  return <Tag color={color}>{label}</Tag>;
}

function renderPlainStatus(value?: string) {
  if (!value) {
    return "--";
  }
  if (value === "processed") {
    return <Tag color="success">processed</Tag>;
  }
  if (value === "pending") {
    return <Tag color="default">pending</Tag>;
  }
  if (value === "processing") {
    return <Tag color="processing">processing</Tag>;
  }
  if (value === "failed") {
    return <Tag color="error">failed</Tag>;
  }
  return <Tag>{value}</Tag>;
}

function ScoutOutcomeTag({
  highValueCommentCount,
  bundleCount,
}: {
  highValueCommentCount?: number;
  bundleCount?: number;
}) {
  const highValue = highValueCommentCount ?? 0;
  const bundles = bundleCount ?? 0;
  if (bundles > 0) {
    return <Tag color="success">已生成证据包</Tag>;
  }
  if (highValue > 0) {
    return <Tag color="warning">有高价值评论但未落证据包</Tag>;
  }
  return <Tag>无高价值评论</Tag>;
}

function buildScoutOutcomeDescription(detail: ScoutRawVideoSummary) {
  const comments = detail.comment_count ?? 0;
  const highValue = detail.high_value_comment_count ?? 0;
  const bundles = detail.bundle_count ?? 0;
  if (bundles > 0) {
    return `Miner 已处理 ${comments} 条评论，其中 ${highValue} 条被判为高价值，已整理出 ${bundles} 个证据包。`;
  }
  if (highValue > 0) {
    return `Miner 已处理 ${comments} 条评论，其中 ${highValue} 条被判为高价值，但当前没有看到对应证据包，建议继续排查 bundle 落库链路。`;
  }
  return `Miner 已处理 ${comments} 条评论，但没有评论进入高价值集合，所以不会生成证据包。`;
}

function CommentPictures({ pictures }: { pictures: ScoutMediaAsset[] }) {
  return (
    <Space wrap size={12}>
      {pictures.map((picture) => (
        <div key={picture.asset_id} style={{ width: 132 }}>
          <Image
            width={132}
            height={132}
            style={{ objectFit: "cover", borderRadius: 10 }}
            src={`/api/v1/media-assets/${encodeURIComponent(picture.asset_id)}/content`}
            fallback={picture.source_url}
          />
          <Typography.Text
            type="secondary"
            style={{ display: "block", marginTop: 6, fontSize: 12 }}
          >
            {formatPictureMeta(picture)}
          </Typography.Text>
        </div>
      ))}
    </Space>
  );
}

function formatPictureMeta(picture: ScoutMediaAsset) {
  const size =
    typeof picture.width === "number" && typeof picture.height === "number"
      ? `${picture.width}×${picture.height}`
      : "尺寸未知";
  const status = picture.download_status || "unknown";
  return `${size} · ${status}`;
}
