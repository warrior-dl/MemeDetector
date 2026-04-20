/**
 * 统一的 HTTP 客户端。
 *
 * 相对原先只有 ``fetch(url)`` 的 wrapper，做三件事：
 *
 * 1. **超时**：默认 15s（可在每次调用覆盖），使用 ``AbortSignal.timeout``；
 * 2. **错误分类**：把失败统一包成 ``ApiError``，带 ``status`` / ``kind``
 *    三种：`network`（离线 / CORS / 中断）、`client`（4xx）、`server`（5xx）
 *    + ``retryable`` 标志，上层可据此决定是否重试或展示不同文案；
 * 3. **兼容旧调用**：保留 ``fetchJson<T>(url, init?)`` 签名，业务代码无需改动。
 *
 * 注意：这里**不负责 react-query 的 retry 策略**，由 QueryClient 的默认
 * ``retry`` 回调读 ``ApiError.retryable`` 决定，见 ``main.tsx``。
 */

export type ApiErrorKind = "network" | "timeout" | "client" | "server";

export class ApiError extends Error {
  readonly status: number;
  readonly kind: ApiErrorKind;
  readonly retryable: boolean;
  readonly body: string;

  constructor(
    message: string,
    options: { status: number; kind: ApiErrorKind; body?: string },
  ) {
    super(message);
    this.name = "ApiError";
    this.status = options.status;
    this.kind = options.kind;
    this.body = options.body ?? "";
    // 4xx 基本是业务错，不重试；5xx 和网络错值得重试一次。
    this.retryable = options.kind === "server" || options.kind === "network" || options.kind === "timeout";
  }
}

const DEFAULT_TIMEOUT_MS = 15_000;

export interface FetchJsonInit extends Omit<RequestInit, "signal"> {
  /** 单次请求超时（毫秒）。默认 15s，填 0 或负数表示关闭超时。 */
  timeoutMs?: number;
  /** 外部可再叠加一个 abort 信号，和内部的 timeout signal 组合。 */
  signal?: AbortSignal;
}

/** 把外部 signal 和 timeout signal 合并成一个。 */
function combineSignals(signals: (AbortSignal | undefined)[]): AbortSignal | undefined {
  const valid = signals.filter((s): s is AbortSignal => Boolean(s));
  if (valid.length === 0) return undefined;
  if (valid.length === 1) return valid[0];
  // `AbortSignal.any` 在 Safari 17+ / Chrome 116+ / Firefox 124+ 可用，
  // 我们已经依赖 React 19 + 现代浏览器，这里直接用。
  return AbortSignal.any(valid);
}

export async function fetchJson<T>(url: string, init?: FetchJsonInit): Promise<T> {
  const timeoutMs = init?.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const timeoutSignal = timeoutMs > 0 ? AbortSignal.timeout(timeoutMs) : undefined;
  const signal = combineSignals([timeoutSignal, init?.signal]);

  let response: Response;
  try {
    response = await fetch(url, {
      ...init,
      signal,
      headers: {
        "Content-Type": "application/json",
        ...(init?.headers ?? {}),
      },
    });
  } catch (err) {
    // AbortError.name 在超时和手动取消都是 "AbortError"，再看 signal.reason 区分
    if (err instanceof DOMException && err.name === "AbortError") {
      if (timeoutSignal?.aborted) {
        throw new ApiError(`请求超时（${timeoutMs}ms）：${url}`, {
          status: 0,
          kind: "timeout",
        });
      }
      throw new ApiError(`请求已取消：${url}`, { status: 0, kind: "network" });
    }
    throw new ApiError(
      err instanceof Error ? `网络错误：${err.message}` : `网络错误：${String(err)}`,
      { status: 0, kind: "network" },
    );
  }

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    const kind: ApiErrorKind = response.status >= 500 ? "server" : "client";
    throw new ApiError(
      body || `${response.status} ${response.statusText}`,
      { status: response.status, kind, body },
    );
  }

  // 204 No Content 等空响应返回 undefined，避免 .json() 报错
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}
