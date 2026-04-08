import {
  Alert,
  Button,
  Descriptions,
  Drawer,
  Empty,
  Image,
  Input,
  List,
  Pagination,
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
import { useScoutRawVideoDetail, useScoutRawVideosPage } from "../features/scout/hooks";
import { PageSection } from "../ui/PageSection";
import { formatOptionalDateTime } from "../utils/format";

export function ScoutPage() {
  const [candidateStatus, setCandidateStatus] = useState<string>();
  const [partition, setPartition] = useState("");
  const [keywordInput, setKeywordInput] = useState("");
  const deferredKeyword = useDeferredValue(keywordInput.trim());
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [selectedVideo, setSelectedVideo] = useState<ScoutRawVideoSummary>();

  const videosQuery = useScoutRawVideosPage({
    candidateStatus,
    partition: partition.trim() || undefined,
    keyword: deferredKeyword || undefined,
    limit: pageSize,
    offset: (page - 1) * pageSize,
  });
  const detailQuery = useScoutRawVideoDetail(selectedVideo?.bvid, selectedVideo?.collected_date);

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

  const detail = detailQuery.data;

  return (
    <PageSection
      title="Scout 采集数据"
      subtitle="直接查看原始视频快照、评论样本和图片资产，确认 Scout 采到了什么，再决定是否继续排查 Miner。"
      extra={
        <Space wrap>
          <Select
            allowClear
            placeholder="候选状态"
            value={candidateStatus}
            style={{ width: 140 }}
            options={[
              { label: "待提取", value: "pending" },
              { label: "已提取", value: "processed" },
            ]}
            onChange={(value) => {
              setCandidateStatus(value);
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
                              key: "candidate_status",
                              label: "候选提取状态",
                              children: renderPlainStatus(detail.candidate_status),
                            },
                            {
                              key: "updated_at",
                              label: "更新时间",
                              children: formatOptionalDateTime(detail.updated_at),
                            },
                          ]}
                        />
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
    stage === "researched" ? "purple" : stage === "mined" ? "cyan" : stage === "scouted" ? "blue" : "default";
  const label =
    stage === "researched" ? "已进入 Research" : stage === "mined" ? "已 Miner" : stage === "scouted" ? "仅 Scout" : "--";
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
  return <Tag>{value}</Tag>;
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
