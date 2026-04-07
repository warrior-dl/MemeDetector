import { Card, Space, Typography } from "antd";
import type { PropsWithChildren, ReactNode } from "react";

interface PageSectionProps extends PropsWithChildren {
  title: string;
  extra?: ReactNode;
  subtitle?: string;
}

export function PageSection({ title, subtitle, extra, children }: PageSectionProps) {
  return (
    <Card
      styles={{ body: { padding: 20 } }}
      style={{
        border: "1px solid rgba(15, 23, 42, 0.08)",
        boxShadow: "0 18px 38px rgba(15, 23, 42, 0.06)",
      }}
    >
      <Space
        direction="vertical"
        size={16}
        style={{ width: "100%" }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "flex-start",
            justifyContent: "space-between",
            gap: 16,
          }}
        >
          <div>
            <Typography.Title level={4} style={{ margin: 0 }}>
              {title}
            </Typography.Title>
            {subtitle ? (
              <Typography.Text style={{ color: "#6b7280" }}>{subtitle}</Typography.Text>
            ) : null}
          </div>
          {extra}
        </div>
        {children}
      </Space>
    </Card>
  );
}
