import {
  Alert,
  Button,
  Card,
  Col,
  Descriptions,
  Empty,
  List,
  Progress,
  Row,
  Select,
  Space,
  Spin,
  Tag,
  Typography,
} from "antd";
import { useEffect, useState } from "react";
import { useJobs, useRunDetail, useRuns, useTriggerJob } from "../features/pipeline/hooks";
import { PageSection } from "../ui/PageSection";
import { JsonPanel } from "../ui/JsonPanel";
import { RunStatusTag } from "../ui/StatusTags";
import { formatDateTime, formatDuration, formatOptionalDateTime } from "../utils/format";

function renderJobProgress(job: {
  is_running?: boolean;
  active_phase?: string;
  active_progress_current?: number;
  active_progress_total?: number;
  active_progress_unit?: string;
  active_progress_message?: string;
}) {
  const current = Number(job.active_progress_current ?? 0);
  const total = Number(job.active_progress_total ?? 0);
  const hasProgress = total > 0;
  const percent = hasProgress ? Math.max(0, Math.min(100, Math.round((current / total) * 100))) : 0;
  const detailText = hasProgress
    ? `${current}/${total}${job.active_progress_unit ? ` ${job.active_progress_unit}` : ""}`
    : job.active_phase || "运行中";

  if (!job.is_running) {
    return (
      <Typography.Text type="secondary">
        当前空闲
      </Typography.Text>
    );
  }

  return (
    <Space direction="vertical" size={6} style={{ width: "100%" }}>
      <Typography.Text>{job.active_progress_message || "任务运行中"}</Typography.Text>
      {hasProgress ? <Progress percent={percent} size="small" status="active" /> : <Progress percent={100} size="small" status="active" showInfo={false} />}
      <Typography.Text type="secondary">
        {detailText}
      </Typography.Text>
    </Space>
  );
}

export function PipelinePage() {
  const [jobFilter, setJobFilter] = useState<string>();
  const [statusFilter, setStatusFilter] = useState<string>();
  const [selectedRunId, setSelectedRunId] = useState<string>();

  const runsQuery = useRuns({
    jobName: jobFilter,
    status: statusFilter,
    limit: 50,
  });
  const jobsQuery = useJobs();
  const runDetailQuery = useRunDetail(selectedRunId);
  const triggerMutation = useTriggerJob();

  useEffect(() => {
    const items = runsQuery.data ?? [];
    if (!items.length) {
      if (selectedRunId) {
        setSelectedRunId(undefined);
      }
      return;
    }
    if (!selectedRunId || !items.some((item) => item.id === selectedRunId)) {
      setSelectedRunId(items[0].id);
    }
  }, [runsQuery.data, selectedRunId]);

  const jobs = jobsQuery.data ?? [];

  return (
    <Space direction="vertical" size={20} style={{ width: "100%" }}>
      {triggerMutation.isSuccess && triggerMutation.data?.message ? (
        <Alert
          type="success"
          showIcon
          message={triggerMutation.data.message}
          description={`任务 ${triggerMutation.data.job_name} 已提交，右侧运行历史会在后台状态变化后更新。`}
        />
      ) : null}
      {triggerMutation.error ? (
        <Alert
          type="error"
          showIcon
          message="任务触发失败"
          description={String(triggerMutation.error)}
        />
      ) : null}

      <Row gutter={[16, 16]}>
        {jobs.map((job) => {
          const jobName = job.job_name || job.id;
          const isLoading = triggerMutation.isPending && triggerMutation.variables === jobName;
          return (
            <Col xs={24} md={8} key={job.id}>
              <Card>
                <Space direction="vertical" size={12} style={{ width: "100%" }}>
                  <Space wrap>
                    <Typography.Title level={4} style={{ margin: 0 }}>
                      {job.name}
                    </Typography.Title>
                    <Tag color={job.is_running ? "processing" : "default"}>
                      {job.is_running ? "运行中" : "空闲"}
                    </Tag>
                  </Space>
                  <Typography.Text type="secondary">
                    {job.job_name || job.id} · 下次运行 {formatOptionalDateTime(job.next_run_time)}
                  </Typography.Text>
                  <Typography.Text type="secondary">触发器: {job.trigger}</Typography.Text>
                  {renderJobProgress(job)}
                  {job.last_error ? (
                    <Alert type="error" message="最近一次运行报错" description={job.last_error} />
                  ) : null}
                  <Space wrap>
                    <Button type="primary" onClick={() => triggerMutation.mutate(jobName)} loading={isLoading}>
                      手动运行
                    </Button>
                    <Button
                      onClick={() => {
                        setJobFilter(jobName);
                        setStatusFilter(undefined);
                      }}
                    >
                      查看运行记录
                    </Button>
                  </Space>
                </Space>
              </Card>
            </Col>
          );
        })}
      </Row>

      <Row gutter={[20, 20]}>
        <Col xs={24} xl={10}>
          <PageSection title="调度计划" subtitle="自动任务状态、最近错误和手动调试入口统一收口。">
            {jobsQuery.error ? (
              <Alert type="error" message="调度计划加载失败" description={String(jobsQuery.error)} />
            ) : (
              <List
                dataSource={jobs}
                locale={{ emptyText: <Empty description="暂无调度任务" /> }}
                renderItem={(item) => (
                  <List.Item>
                    <List.Item.Meta
                      title={
                        <Space wrap>
                          <Typography.Text strong>{item.name}</Typography.Text>
                          <Tag color={item.is_running ? "processing" : "default"}>
                            {item.is_running ? "运行中" : "待机"}
                          </Tag>
                        </Space>
                      }
                      description={
                        <Space direction="vertical" size={4} style={{ width: "100%" }}>
                          <Typography.Text type="secondary">
                            下次运行：{formatOptionalDateTime(item.next_run_time)}
                          </Typography.Text>
                          <Typography.Text type="secondary">
                            最近完成：{formatOptionalDateTime(item.last_finished_at)}
                          </Typography.Text>
                          {renderJobProgress(item)}
                        </Space>
                      }
                    />
                  </List.Item>
                )}
              />
            )}
          </PageSection>
        </Col>
        <Col xs={24} xl={14}>
          <PageSection
            title="运行历史"
            subtitle="失败、卡住、待重试都在这一个界面定位。"
            extra={
              <Space wrap>
                <Select
                  allowClear
                  placeholder="任务"
                  value={jobFilter}
                  style={{ width: 120 }}
                options={[
                    { label: "Scout", value: "scout" },
                    { label: "Miner Stage 1", value: "miner_insights" },
                    { label: "Miner Stage 2", value: "miner_bundles" },
                    { label: "Research", value: "research" },
                  ]}
                  onChange={(value) => setJobFilter(value)}
                />
                <Select
                  allowClear
                  placeholder="状态"
                  value={statusFilter}
                  style={{ width: 120 }}
                  options={[
                    { label: "运行中", value: "running" },
                    { label: "成功", value: "success" },
                    { label: "失败", value: "failed" },
                  ]}
                  onChange={(value) => setStatusFilter(value)}
                />
                <Button onClick={() => runsQuery.refetch()} loading={runsQuery.isFetching}>
                  刷新
                </Button>
              </Space>
            }
          >
            <Row gutter={[16, 16]}>
              <Col xs={24} lg={10}>
                {runsQuery.error ? (
                  <Alert type="error" message="运行记录加载失败" description={String(runsQuery.error)} />
                ) : (
                  <List
                    dataSource={runsQuery.data ?? []}
                    locale={{ emptyText: <Empty description="暂无运行记录" /> }}
                    renderItem={(item) => (
                      <List.Item
                        style={{
                          cursor: "pointer",
                          paddingInline: 12,
                          borderRadius: 12,
                          background: item.id === selectedRunId ? "rgba(59, 130, 246, 0.08)" : "transparent",
                        }}
                        onClick={() => setSelectedRunId(item.id)}
                      >
                        <List.Item.Meta
                          title={
                            <Space wrap>
                              <Typography.Text strong>{item.job_name}</Typography.Text>
                              <RunStatusTag status={item.status} />
                            </Space>
                          }
                          description={
                            <Space direction="vertical" size={4} style={{ width: "100%" }}>
                              <Typography.Text type="secondary">
                                {formatDateTime(item.started_at)} · {item.trigger_mode || "--"}
                              </Typography.Text>
                              <Typography.Text>{item.summary || item.error_message || "暂无摘要"}</Typography.Text>
                            </Space>
                          }
                        />
                      </List.Item>
                    )}
                  />
                )}
              </Col>
              <Col xs={24} lg={14}>
                {!selectedRunId ? (
                  <Empty description="选择一条运行记录查看详情" />
                ) : runDetailQuery.isLoading ? (
                  <Spin />
                ) : runDetailQuery.error ? (
                  <Alert type="error" message="运行详情加载失败" description={String(runDetailQuery.error)} />
                ) : !runDetailQuery.data ? (
                  <Empty description="运行详情不存在" />
                ) : (
                  <Space direction="vertical" size={12} style={{ width: "100%" }}>
                    <Descriptions
                      bordered
                      size="small"
                      column={1}
                      items={[
                        {
                          key: "job_name",
                          label: "任务",
                          children: runDetailQuery.data.job_name,
                        },
                        {
                          key: "status",
                          label: "状态",
                          children: <RunStatusTag status={runDetailQuery.data.status} />,
                        },
                        {
                          key: "trigger_mode",
                          label: "触发方式",
                          children: runDetailQuery.data.trigger_mode || "--",
                        },
                        {
                          key: "result_count",
                          label: "结果数",
                          children: String(runDetailQuery.data.result_count ?? 0),
                        },
                        {
                          key: "duration",
                          label: "耗时",
                          children: formatDuration(runDetailQuery.data.duration_seconds),
                        },
                        {
                          key: "started_at",
                          label: "开始时间",
                          children: formatDateTime(runDetailQuery.data.started_at),
                        },
                        {
                          key: "finished_at",
                          label: "结束时间",
                          children: formatDateTime(runDetailQuery.data.finished_at),
                        },
                      ]}
                    />
                    <div>
                      <Typography.Text strong>摘要</Typography.Text>
                      <Typography.Paragraph style={{ marginTop: 8 }}>
                        {runDetailQuery.data.summary || "暂无摘要"}
                      </Typography.Paragraph>
                    </div>
                    {runDetailQuery.data.error_message ? (
                      <Alert
                        type="error"
                        showIcon
                        message="错误信息"
                        description={runDetailQuery.data.error_message}
                      />
                    ) : null}
                    <JsonPanel title="运行载荷" value={runDetailQuery.data.payload ?? {}} />
                  </Space>
                )}
              </Col>
            </Row>
          </PageSection>
        </Col>
      </Row>
    </Space>
  );
}
