import {
  Alert,
  Card,
  Col,
  Empty,
  List,
  Row,
  Skeleton,
  Space,
  Statistic,
  Tag,
  Typography,
} from "antd";
import { useCandidatesPage, useAgentConversations } from "../features/candidates/hooks";
import { useDashboardStats } from "../features/dashboard/hooks";
import { useMemes } from "../features/library/hooks";
import { useJobs, useRuns } from "../features/pipeline/hooks";
import { useScoutRawVideosPage } from "../features/scout/hooks";
import { PageSection } from "../ui/PageSection";
import { CandidateStatusTag, ConversationStatusTag, RunStatusTag } from "../ui/StatusTags";
import { formatOptionalDateTime } from "../utils/format";

export function DashboardPage() {
  const statsQuery = useDashboardStats();
  const candidatesQuery = useCandidatesPage({ status: "pending", limit: 6 });
  const memesQuery = useMemes({ limit: 6 });
  const runsQuery = useRuns({ limit: 6 });
  const failedRunsQuery = useRuns({ status: "failed", limit: 6 });
  const jobsQuery = useJobs();
  const scoutQuery = useScoutRawVideosPage({ limit: 6, offset: 0 });
  const failedConversationsQuery = useAgentConversations({ status: "failed", limit: 6, offset: 0 });
  const recentConversationsQuery = useAgentConversations({ limit: 6, offset: 0 });

  if (statsQuery.isLoading) {
    return <Skeleton active paragraph={{ rows: 12 }} />;
  }

  if (statsQuery.error) {
    return <Alert type="error" message="Dashboard 加载失败" description={String(statsQuery.error)} />;
  }

  const stats = statsQuery.data;

  return (
    <Space direction="vertical" size={20} style={{ width: "100%" }}>
      <Row gutter={[16, 16]}>
        <Col xs={24} sm={12} xl={6}>
          <Card>
            <Statistic title="待处理候选" value={stats?.candidates.pending ?? 0} />
          </Card>
        </Col>
        <Col xs={24} sm={12} xl={6}>
          <Card>
            <Statistic title="已接受候选" value={stats?.candidates.accepted ?? 0} />
          </Card>
        </Col>
        <Col xs={24} sm={12} xl={6}>
          <Card>
            <Statistic title="已拒绝候选" value={stats?.candidates.rejected ?? 0} />
          </Card>
        </Col>
        <Col xs={24} sm={12} xl={6}>
          <Card>
            <Statistic title="梗库条目" value={stats?.memes_in_library ?? 0} />
          </Card>
        </Col>
        <Col xs={24} sm={12} xl={6}>
          <Card>
            <Statistic title="失败对话" value={failedConversationsQuery.data?.total ?? 0} />
          </Card>
        </Col>
        <Col xs={24} sm={12} xl={6}>
          <Card>
            <Statistic title="近期失败运行" value={(failedRunsQuery.data ?? []).length} />
          </Card>
        </Col>
      </Row>

      <Row gutter={[20, 20]}>
        <Col xs={24} xl={14}>
          <PageSection title="最近运行" subtitle="先看任务状态，再决定去处理哪一段 pipeline。">
            <List
              dataSource={runsQuery.data ?? []}
              locale={{ emptyText: <Empty description="暂无运行记录" /> }}
              renderItem={(item) => (
                <List.Item>
                  <List.Item.Meta
                    title={
                      <Space wrap>
                        <Typography.Text strong>{item.job_name}</Typography.Text>
                        <RunStatusTag status={item.status} />
                      </Space>
                    }
                    description={
                      <Space direction="vertical" size={4} style={{ width: "100%" }}>
                        <Typography.Text>{item.summary || item.error_message || "暂无摘要"}</Typography.Text>
                        <Typography.Text type="secondary">
                          {formatOptionalDateTime(item.started_at, "--")} · {item.trigger_mode || "--"}
                        </Typography.Text>
                      </Space>
                    }
                  />
                </List.Item>
              )}
            />
          </PageSection>
        </Col>
        <Col xs={24} xl={10}>
          <PageSection title="调度任务" subtitle="保留自动调度，但手动调试也在同一界面完成。">
            <List
              dataSource={jobsQuery.data ?? []}
              locale={{ emptyText: <Empty description="暂无调度任务" /> }}
              renderItem={(item) => (
                <List.Item>
                  <List.Item.Meta
                    title={item.name}
                    description={
                      <Space direction="vertical" size={4} style={{ width: "100%" }}>
                        <Typography.Text type="secondary">
                          下次运行：{formatOptionalDateTime(item.next_run_time)}
                        </Typography.Text>
                        <Typography.Text type="secondary">
                          最近完成：{formatOptionalDateTime(item.last_finished_at)}
                        </Typography.Text>
                      </Space>
                    }
                  />
                  <Tag color={item.is_running ? "processing" : "default"}>
                    {item.is_running ? "运行中" : "待机"}
                  </Tag>
                </List.Item>
              )}
            />
          </PageSection>
        </Col>
      </Row>

      <Row gutter={[20, 20]}>
        <Col xs={24} xl={12}>
          <PageSection title="最近 Scout 采集" subtitle="先确认原始视频和评论有没有被采到，再往下排查 Miner。">
            {scoutQuery.error ? (
              <Alert type="error" message="Scout 数据加载失败" description={String(scoutQuery.error)} />
            ) : (
              <List
                dataSource={scoutQuery.data?.items ?? []}
                locale={{ emptyText: <Empty description="暂无 Scout 采集数据" /> }}
                renderItem={(item) => (
                  <List.Item>
                    <List.Item.Meta
                      title={
                        <Space wrap>
                          <Typography.Text strong>{item.title || item.bvid}</Typography.Text>
                          <Tag color={resolveScoutStageColor(item.pipeline_stage)}>
                            {resolveScoutStageLabel(item.pipeline_stage)}
                          </Tag>
                        </Space>
                      }
                      description={
                        <Space direction="vertical" size={4} style={{ width: "100%" }}>
                          <Typography.Text>
                            {item.partition || "--"} · 评论 {item.comment_count ?? 0} · 图片 {item.picture_count ?? 0}
                          </Typography.Text>
                          <Typography.Text type="secondary">
                            {item.first_comment || "暂无评论样本"}
                          </Typography.Text>
                        </Space>
                      }
                    />
                  </List.Item>
                )}
              />
            )}
          </PageSection>
        </Col>
        <Col xs={24} xl={12}>
          <PageSection title="待处理候选" subtitle="最适合收敛成一个候选工作台，不再拆散到多个页面。">
            <List
              dataSource={candidatesQuery.data?.items ?? []}
              locale={{ emptyText: <Empty description="暂无候选" /> }}
              renderItem={(item) => (
                <List.Item>
                  <List.Item.Meta
                    title={
                      <Space wrap>
                        <Typography.Text strong>{item.word}</Typography.Text>
                        <CandidateStatusTag status={item.status} />
                      </Space>
                    }
                    description={item.explanation || item.sample_comments || "暂无解释"}
                  />
                  <Typography.Text>{Number(item.score || 0).toFixed(2)}</Typography.Text>
                </List.Item>
              )}
            />
          </PageSection>
        </Col>
        <Col xs={24} xl={12}>
          <PageSection title="最新入库梗" subtitle="正式词条与候选分开看，但保持同一套导航。">
            <List
              dataSource={memesQuery.data?.hits ?? []}
              locale={{ emptyText: <Empty description="暂无梗库数据" /> }}
              renderItem={(item) => (
                <List.Item>
                  <List.Item.Meta
                    title={item.title || item.id}
                    description={item.definition || item.origin || "暂无描述"}
                  />
                  <Tag color="geekblue">{item.lifecycle_stage || "--"}</Tag>
                </List.Item>
              )}
            />
          </PageSection>
        </Col>
      </Row>

      <Row gutter={[20, 20]}>
        <Col xs={24} xl={12}>
          <PageSection title="最近 Agent 对话" subtitle="快速确认 Researcher / Miner 最近在处理什么。">
            {recentConversationsQuery.error ? (
              <Alert type="error" message="最近对话加载失败" description={String(recentConversationsQuery.error)} />
            ) : (
              <List
                dataSource={recentConversationsQuery.data?.items ?? []}
                locale={{ emptyText: <Empty description="暂无对话记录" /> }}
                renderItem={(item) => (
                  <List.Item>
                    <List.Item.Meta
                      title={
                        <Space wrap>
                          <Typography.Text strong>{item.word}</Typography.Text>
                          <Tag>{item.agent_name}</Tag>
                          <ConversationStatusTag status={item.status} />
                        </Space>
                      }
                      description={
                        <Space direction="vertical" size={4} style={{ width: "100%" }}>
                          <Typography.Text>{item.summary || "暂无摘要"}</Typography.Text>
                          <Typography.Text type="secondary">
                            {formatOptionalDateTime(item.started_at, "--")} · {item.message_count ?? 0} 条消息
                          </Typography.Text>
                        </Space>
                      }
                    />
                  </List.Item>
                )}
              />
            )}
          </PageSection>
        </Col>
        <Col xs={24} xl={12}>
          <PageSection title="异常审计" subtitle="优先处理失败运行和失败对话，减少 pipeline 沉默失败。">
            <Space direction="vertical" size={16} style={{ width: "100%" }}>
              <div>
                <Typography.Text strong>失败运行</Typography.Text>
                <List
                  dataSource={failedRunsQuery.data ?? []}
                  locale={{ emptyText: <Empty description="暂无失败运行" /> }}
                  renderItem={(item) => (
                    <List.Item>
                      <List.Item.Meta
                        title={
                          <Space wrap>
                            <Typography.Text strong>{item.job_name}</Typography.Text>
                            <Tag color="error">失败</Tag>
                          </Space>
                        }
                        description={item.error_message || item.summary || "暂无错误信息"}
                      />
                    </List.Item>
                  )}
                />
              </div>
              <div>
                <Typography.Text strong>失败对话</Typography.Text>
                <List
                  dataSource={failedConversationsQuery.data?.items ?? []}
                  locale={{ emptyText: <Empty description="暂无失败对话" /> }}
                  renderItem={(item) => (
                    <List.Item>
                      <List.Item.Meta
                        title={
                          <Space wrap>
                            <Typography.Text strong>{item.word}</Typography.Text>
                            <Tag>{item.agent_name}</Tag>
                            <Tag color="error">失败</Tag>
                          </Space>
                        }
                        description={item.error_message || item.summary || "暂无错误信息"}
                      />
                    </List.Item>
                  )}
                />
              </div>
            </Space>
          </PageSection>
        </Col>
      </Row>
    </Space>
  );
}

function resolveScoutStageLabel(stage?: string) {
  if (stage === "researched") {
    return "已进入 Research";
  }
  if (stage === "mined") {
    return "已 Miner";
  }
  if (stage === "scouted") {
    return "仅 Scout";
  }
  return "--";
}

function resolveScoutStageColor(stage?: string) {
  if (stage === "researched") {
    return "purple";
  }
  if (stage === "mined") {
    return "cyan";
  }
  if (stage === "scouted") {
    return "blue";
  }
  return "default";
}
