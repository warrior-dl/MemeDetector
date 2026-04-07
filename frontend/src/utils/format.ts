export function formatDateTime(value?: string | null) {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date);
}

export function formatOptionalDateTime(value?: string | null, fallback = "未设置") {
  if (!value) {
    return fallback;
  }
  return formatDateTime(value);
}

export function formatDuration(value?: number) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "--";
  }
  if (value < 60) {
    return `${value.toFixed(1)} 秒`;
  }
  return `${Math.floor(value / 60)} 分 ${Math.round(value % 60)} 秒`;
}

export function shortId(value?: string, maxLength = 12) {
  if (!value) {
    return "--";
  }
  return value.length > maxLength ? `${value.slice(0, maxLength)}...` : value;
}
