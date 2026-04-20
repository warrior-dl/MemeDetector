import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ConfigProvider, App as AntdApp, theme } from "antd";
import zhCN from "antd/locale/zh_CN";
import { BrowserRouter } from "react-router-dom";
import App from "./app/App";
import { AppErrorBoundary } from "./app/AppErrorBoundary";
import { ApiError } from "./data/api";
import "./index.css";

const MAX_QUERY_RETRIES = 2;
const MAX_MUTATION_RETRIES = 0;

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      staleTime: 15_000,
      // 4xx 视为业务错误不重试；5xx / 超时 / 网络错才重试，并且有次数上限。
      retry: (failureCount, error) => {
        if (error instanceof ApiError && !error.retryable) return false;
        return failureCount < MAX_QUERY_RETRIES;
      },
      retryDelay: (attempt) => Math.min(1_000 * 2 ** attempt, 8_000),
    },
    mutations: {
      // 写操作默认不重试，避免重复触发 job / 误点 verify 等副作用。
      retry: MAX_MUTATION_RETRIES,
    },
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ConfigProvider
      locale={zhCN}
      theme={{
        algorithm: theme.defaultAlgorithm,
        token: {
          colorPrimary: "#0f766e",
          borderRadius: 14,
          fontFamily: "'IBM Plex Sans', 'Noto Sans SC', sans-serif",
          colorBgLayout: "#f4f5ef",
        },
      }}
    >
      <AntdApp>
        <QueryClientProvider client={queryClient}>
          <BrowserRouter>
            <AppErrorBoundary>
              <App />
            </AppErrorBoundary>
          </BrowserRouter>
        </QueryClientProvider>
      </AntdApp>
    </ConfigProvider>
  </StrictMode>,
);
