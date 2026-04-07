import {
  ApartmentOutlined,
  DatabaseOutlined,
  DotChartOutlined,
  RadarChartOutlined,
} from "@ant-design/icons";
import { Badge, Button, Layout, Menu, Space, Typography } from "antd";
import { Outlet, useLocation, useNavigate } from "react-router-dom";
import { useDashboardStats } from "../features/dashboard/hooks";

const { Header, Sider, Content } = Layout;

const navigationItems = [
  { key: "/dashboard", icon: <RadarChartOutlined />, label: "Dashboard" },
  { key: "/candidates", icon: <DotChartOutlined />, label: "候选工作台" },
  { key: "/library", icon: <DatabaseOutlined />, label: "梗库" },
  { key: "/pipeline", icon: <ApartmentOutlined />, label: "Pipeline" },
];

export function AppShell() {
  const location = useLocation();
  const navigate = useNavigate();
  const { data } = useDashboardStats();

  return (
    <Layout style={{ minHeight: "100vh", background: "transparent" }}>
      <Sider
        breakpoint="lg"
        collapsedWidth={0}
        width={260}
        style={{
          background: "linear-gradient(180deg, #0f172a 0%, #111827 100%)",
          borderRight: "1px solid rgba(255,255,255,0.06)",
        }}
      >
        <div style={{ padding: 24 }}>
          <Typography.Text
            style={{
              display: "block",
              marginBottom: 6,
              color: "#99f6e4",
              fontSize: 12,
              letterSpacing: 1.4,
              textTransform: "uppercase",
            }}
          >
            MemeDetector
          </Typography.Text>
          <Typography.Title level={3} style={{ margin: 0, color: "#f8fafc" }}>
            管理工作台
          </Typography.Title>
          <Typography.Paragraph style={{ marginTop: 8, color: "#94a3b8" }}>
            把采集、候选、梗库和调度放进同一条工作流里。
          </Typography.Paragraph>
        </div>

        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[location.pathname]}
          items={navigationItems}
          onClick={({ key }) => navigate(key)}
          style={{
            background: "transparent",
            borderInlineEnd: 0,
            paddingInline: 12,
          }}
        />
      </Sider>

      <Layout style={{ background: "transparent" }}>
        <Header
          style={{
            height: "auto",
            padding: "24px 28px 12px",
            background: "transparent",
          }}
        >
          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              alignItems: "center",
              justifyContent: "space-between",
              gap: 16,
            }}
          >
            <div>
              <Typography.Title level={2} style={{ margin: 0, color: "#111827" }}>
                {resolvePageTitle(location.pathname)}
              </Typography.Title>
              <Typography.Text style={{ color: "#4b5563" }}>
                统一查看 pipeline 状态、候选线索和已入库梗。
              </Typography.Text>
            </div>

            <Space size={12} wrap>
              <Badge
                count={`待处理候选 ${data?.candidates?.pending ?? "--"}`}
                style={{ backgroundColor: "#0f766e" }}
              />
              <Badge
                count={`梗库 ${data?.memes_in_library ?? "--"}`}
                style={{ backgroundColor: "#c2410c" }}
              />
              <Button href="/docs" target="_blank">
                API Docs
              </Button>
            </Space>
          </div>
        </Header>

        <Content style={{ padding: "0 28px 28px" }}>
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
}

function resolvePageTitle(pathname: string) {
  if (pathname.startsWith("/candidates")) {
    return "候选工作台";
  }
  if (pathname.startsWith("/library")) {
    return "梗库";
  }
  if (pathname.startsWith("/pipeline")) {
    return "Pipeline";
  }
  return "Dashboard";
}
