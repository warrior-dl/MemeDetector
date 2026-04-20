import { Component, type ErrorInfo, type ReactNode } from "react";
import { Button, Result, Typography } from "antd";

/**
 * 应用级错误边界。裹在路由外面，把任意 render 异常兜底成一个
 * 可见的 fallback 页（Antd ``<Result status="500">``），而不是白屏。
 *
 * React 只能用 class 组件实现 Error Boundary（官方说明：
 * https://react.dev/reference/react/Component#catching-rendering-errors-with-an-error-boundary）。
 *
 * 范围：只接 **渲染期** 抛的异常；event handler / async / setTimeout 里的
 * 错误 React 不会路由到 ErrorBoundary，由 react-query 各自的 ``error`` 状态
 * 处理，或者在调用点自己 try/catch。
 */

interface Props {
  children: ReactNode;
}

interface State {
  error: Error | null;
}

export class AppErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    // 本地开发直接打到 console；生产可以接上报渠道。
    console.error("AppErrorBoundary caught:", error, info);
  }

  private handleReset = (): void => {
    this.setState({ error: null });
  };

  private handleReload = (): void => {
    window.location.reload();
  };

  render(): ReactNode {
    const { error } = this.state;
    if (!error) return this.props.children;

    return (
      <Result
        status="500"
        title="页面出了点意外"
        subTitle="刚才的操作触发了一个未处理的渲染异常，你可以重试，或者刷新页面。"
        extra={[
          <Button type="primary" key="retry" onClick={this.handleReset}>
            重试
          </Button>,
          <Button key="reload" onClick={this.handleReload}>
            刷新页面
          </Button>,
        ]}
      >
        <Typography.Paragraph type="secondary" style={{ textAlign: "left" }}>
          <Typography.Text code>{error.message}</Typography.Text>
        </Typography.Paragraph>
      </Result>
    );
  }
}
