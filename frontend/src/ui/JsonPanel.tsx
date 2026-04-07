import { Typography } from "antd";

interface JsonPanelProps {
  title: string;
  value: unknown;
}

export function JsonPanel({ title, value }: JsonPanelProps) {
  return (
    <div>
      <Typography.Text strong>{title}</Typography.Text>
      <pre
        style={{
          marginTop: 8,
          marginBottom: 0,
          padding: 12,
          overflowX: "auto",
          borderRadius: 12,
          background: "#0f172a",
          color: "#e2e8f0",
          whiteSpace: "pre-wrap",
          wordBreak: "break-word",
        }}
      >
        {JSON.stringify(value, null, 2)}
      </pre>
    </div>
  );
}
