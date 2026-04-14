import {
  Alert,
  Button,
  Descriptions,
  Drawer,
  Empty,
  Input,
  Select,
  Space,
  Table,
  Tag,
  Typography,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import { useDeferredValue, useEffect, useMemo, useState } from "react";
import { useMemeDetail, useMemes, useVerifyMeme } from "../features/library/hooks";
import type { MemeItem } from "../data/types";
import { PageSection } from "../ui/PageSection";
import { formatDateTime } from "../utils/format";

export function LibraryPage() {
  const [query, setQuery] = useState("");
  const [lifecycle, setLifecycle] = useState<string>();
  const [selectedId, setSelectedId] = useState<string>();
  const deferredQuery = useDeferredValue(query);
  const memesQuery = useMemes({ query: deferredQuery.trim(), lifecycle, limit: 100 });
  const memeDetailQuery = useMemeDetail(selectedId);
  const verifyMutation = useVerifyMeme();

  useEffect(() => {
    const items = memesQuery.data?.hits ?? [];
    if (selectedId && !items.some((item) => item.id === selectedId)) {
      setSelectedId(undefined);
    }
  }, [memesQuery.data, selectedId]);

  const columns = useMemo<ColumnsType<MemeItem>>(
    () => [
      {
        title: "词条",
        dataIndex: "title",
        render: (_, record) => (
          <div>
            <Typography.Text strong>{record.title || record.id}</Typography.Text>
            <div style={{ color: "#6b7280" }}>{record.definition || record.origin || "--"}</div>
          </div>
        ),
      },
      {
        title: "分类",
        dataIndex: "category",
        render: (value: string[] | undefined) =>
          Array.isArray(value) && value.length ? value.map((item) => <Tag key={item}>{item}</Tag>) : "--",
      },
      {
        title: "热度",
        dataIndex: "heat_index",
      },
      {
        title: "阶段",
        dataIndex: "lifecycle_stage",
        render: (value?: string) => value || "--",
      },
      {
        title: "更新时间",
        dataIndex: "updated_at",
        width: 180,
        render: (value?: string) => formatDateTime(value),
      },
      {
        title: "校验",
        dataIndex: "human_verified",
        render: (value?: boolean) =>
          value ? <Tag color="success">已人工校验</Tag> : <Tag>未校验</Tag>,
      },
    ],
    [],
  );

  return (
    <PageSection
      title="梗库"
      subtitle="正式入库后的词条集中管理，默认按最新更新时间倒序展示，方便查看最近入库结果。"
      extra={
        <Space wrap>
          <Input.Search
            allowClear
            placeholder="搜索词条"
            value={query}
            style={{ width: 220 }}
            onChange={(event) => setQuery(event.target.value)}
            onSearch={(value) => setQuery(value)}
          />
          <Select
            allowClear
            placeholder="生命周期"
            value={lifecycle}
            style={{ width: 140 }}
            options={[
              { label: "Emerging", value: "emerging" },
              { label: "Peak", value: "peak" },
              { label: "Declining", value: "declining" },
            ]}
            onChange={(value) => setLifecycle(value)}
          />
          <Button onClick={() => memesQuery.refetch()} loading={memesQuery.isFetching}>
            刷新
          </Button>
        </Space>
      }
    >
      {memesQuery.error ? (
        <Alert type="error" message="梗库加载失败" description={String(memesQuery.error)} />
      ) : (
        <>
          <Table
            rowKey="id"
            loading={memesQuery.isLoading}
            columns={columns}
            dataSource={memesQuery.data?.hits ?? []}
            locale={{ emptyText: <Empty description="暂无梗库数据" /> }}
            pagination={false}
            onRow={(record) => ({
              onClick: () => setSelectedId(record.id),
              style: { cursor: "pointer" },
            })}
          />
          <Drawer
            title={memeDetailQuery.data?.title || memeDetailQuery.data?.id || "词条详情"}
            placement="right"
            width={520}
            open={Boolean(selectedId)}
            onClose={() => setSelectedId(undefined)}
          >
            {!selectedId ? (
              <Empty description="请选择词条" />
            ) : memeDetailQuery.isLoading ? (
              <Typography.Text>加载中...</Typography.Text>
            ) : memeDetailQuery.error ? (
              <Alert type="error" message="词条详情加载失败" description={String(memeDetailQuery.error)} />
            ) : !memeDetailQuery.data ? (
              <Empty description="词条不存在" />
            ) : (
              <Space direction="vertical" size={16} style={{ width: "100%" }}>
                <Descriptions
                  bordered
                  size="small"
                  column={1}
                  items={[
                    {
                      key: "id",
                      label: "词条 ID",
                      children: memeDetailQuery.data.id,
                    },
                    {
                      key: "platform",
                      label: "平台",
                      children: memeDetailQuery.data.platform || "--",
                    },
                    {
                      key: "heat_index",
                      label: "热度",
                      children: String(memeDetailQuery.data.heat_index ?? 0),
                    },
                    {
                      key: "lifecycle",
                      label: "阶段",
                      children: memeDetailQuery.data.lifecycle_stage || "--",
                    },
                    {
                      key: "confidence_score",
                      label: "置信度",
                      children:
                        typeof memeDetailQuery.data.confidence_score === "number"
                          ? memeDetailQuery.data.confidence_score.toFixed(2)
                          : "--",
                    },
                    {
                      key: "first_detected_at",
                      label: "首次发现",
                      children: formatDateTime(memeDetailQuery.data.first_detected_at),
                    },
                    {
                      key: "updated_at",
                      label: "最后更新",
                      children: formatDateTime(memeDetailQuery.data.updated_at),
                    },
                    {
                      key: "verified",
                      label: "人工校验",
                      children: memeDetailQuery.data.human_verified ? "已校验" : "未校验",
                    },
                  ]}
                />
                <div>
                  <Typography.Text strong>定义</Typography.Text>
                  <Typography.Paragraph style={{ marginTop: 8 }}>
                    {memeDetailQuery.data.definition || "暂无定义"}
                  </Typography.Paragraph>
                </div>
                <div>
                  <Typography.Text strong>来源</Typography.Text>
                  <Typography.Paragraph style={{ marginTop: 8 }}>
                    {memeDetailQuery.data.origin || "暂无来源说明"}
                  </Typography.Paragraph>
                </div>
                <div>
                  <Typography.Text strong>分类</Typography.Text>
                  <div style={{ marginTop: 8 }}>
                    {memeDetailQuery.data.category?.length
                      ? memeDetailQuery.data.category.map((item) => <Tag key={item}>{item}</Tag>)
                      : "--"}
                  </div>
                </div>
                <div>
                  <Typography.Text strong>别名</Typography.Text>
                  <div style={{ marginTop: 8 }}>
                    {memeDetailQuery.data.alias?.length
                      ? memeDetailQuery.data.alias.map((item) => <Tag key={item}>{item}</Tag>)
                      : "--"}
                  </div>
                </div>
                <div>
                  <Typography.Text strong>来源链接</Typography.Text>
                  <Space direction="vertical" size={6} style={{ width: "100%", marginTop: 8 }}>
                    {memeDetailQuery.data.source_urls?.length ? (
                      memeDetailQuery.data.source_urls.map((url) => (
                        <Typography.Link key={url} href={url} target="_blank">
                          {url}
                        </Typography.Link>
                      ))
                    ) : (
                      <Typography.Text type="secondary">暂无来源链接</Typography.Text>
                    )}
                  </Space>
                </div>
                <Space wrap>
                  <Button
                    type="primary"
                    onClick={() => {
                      if (memeDetailQuery.data) {
                        verifyMutation.mutate({
                          memeId: memeDetailQuery.data.id,
                          verified: !memeDetailQuery.data.human_verified,
                        });
                      }
                    }}
                    loading={verifyMutation.isPending}
                  >
                    {memeDetailQuery.data.human_verified ? "取消人工校验" : "标记为人工校验"}
                  </Button>
                </Space>
                {verifyMutation.error ? (
                  <Alert type="error" message="人工校验更新失败" description={String(verifyMutation.error)} />
                ) : null}
              </Space>
            )}
          </Drawer>
        </>
      )}
    </PageSection>
  );
}
