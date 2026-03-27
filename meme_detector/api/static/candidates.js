const state = {
  limit: 20,
  offset: 0,
  total: 0,
  status: "",
};

const elements = {
  refreshButton: document.getElementById("refreshButton"),
  statusFilter: document.getElementById("statusFilter"),
  pageSize: document.getElementById("pageSize"),
  prevPage: document.getElementById("prevPage"),
  nextPage: document.getElementById("nextPage"),
  deleteAllButton: document.getElementById("deleteAllButton"),
  candidateRows: document.getElementById("candidateRows"),
  pageSummary: document.getElementById("pageSummary"),
  lastUpdated: document.getElementById("lastUpdated"),
};

elements.refreshButton.addEventListener("click", () => {
  loadCandidates();
});

elements.statusFilter.addEventListener("change", () => {
  state.status = elements.statusFilter.value;
  state.offset = 0;
  loadCandidates();
});

elements.pageSize.addEventListener("change", () => {
  state.limit = Number(elements.pageSize.value);
  state.offset = 0;
  loadCandidates();
});

elements.prevPage.addEventListener("click", () => {
  if (state.offset === 0) {
    return;
  }
  state.offset = Math.max(0, state.offset - state.limit);
  loadCandidates();
});

elements.nextPage.addEventListener("click", () => {
  if (state.offset + state.limit >= state.total) {
    return;
  }
  state.offset += state.limit;
  loadCandidates();
});

elements.deleteAllButton.addEventListener("click", async () => {
  const confirmed = window.confirm("确认删除所有候选梗吗？该操作不可撤销。");
  if (!confirmed) {
    return;
  }

  elements.deleteAllButton.disabled = true;
  try {
    const response = await fetch("/api/v1/candidates", { method: "DELETE" });
    if (!response.ok) {
      throw new Error(`删除失败: ${response.status}`);
    }
    state.offset = 0;
    await loadCandidates();
  } catch (error) {
    window.alert(error.message || String(error));
  } finally {
    elements.deleteAllButton.disabled = false;
  }
});

loadCandidates();

async function loadCandidates() {
  try {
    const params = new URLSearchParams({
      limit: String(state.limit),
      offset: String(state.offset),
    });
    if (state.status) {
      params.set("status", state.status);
    }

    const data = await fetchJson(`/api/v1/candidates/page?${params.toString()}`);
    state.total = data.total || 0;
    renderCandidates(data.items || []);
    renderPager();
    setLastUpdated();
  } catch (error) {
    elements.candidateRows.innerHTML = `
      <tr><td colspan="7">${escapeHtml(error.message || String(error))}</td></tr>
    `;
    elements.pageSummary.textContent = "加载失败";
  }
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`请求失败: ${response.status}`);
  }
  return response.json();
}

function renderCandidates(items) {
  if (!items.length) {
    elements.candidateRows.innerHTML = '<tr><td colspan="7">暂无候选数据</td></tr>';
    return;
  }

  elements.candidateRows.innerHTML = items
    .map(
      (item) => `
        <tr>
          <td>${escapeHtml(item.word)}</td>
          <td>${statusBadge(item.status)}</td>
          <td>${item.is_new_word ? "是" : "否"}</td>
          <td>${Number(item.score || 0).toFixed(2)}</td>
          <td>${formatDateTime(item.detected_at)}</td>
          <td class="long-text">${escapeHtml(item.explanation || "--")}</td>
          <td class="long-text">${escapeHtml(item.sample_comments || "--")}</td>
        </tr>
      `,
    )
    .join("");
}

function renderPager() {
  if (state.total === 0) {
    elements.pageSummary.textContent = "共 0 条";
    elements.prevPage.disabled = true;
    elements.nextPage.disabled = true;
    return;
  }

  const start = state.offset + 1;
  const end = Math.min(state.offset + state.limit, state.total);
  const page = Math.floor(state.offset / state.limit) + 1;
  const totalPages = Math.max(1, Math.ceil(state.total / state.limit));

  elements.pageSummary.textContent = `第 ${page} / ${totalPages} 页，显示 ${start}-${end}，共 ${state.total} 条`;
  elements.prevPage.disabled = state.offset === 0;
  elements.nextPage.disabled = state.offset + state.limit >= state.total;
}

function statusBadge(status) {
  const normalized = String(status || "pending").toLowerCase();
  const labelMap = {
    pending: "待处理",
    accepted: "已接受",
    rejected: "已拒绝",
  };
  return `<span class="badge ${normalized}">${labelMap[normalized] || escapeHtml(normalized)}</span>`;
}

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return escapeHtml(String(value));
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

function setLastUpdated() {
  elements.lastUpdated.textContent = `最近刷新：${formatDateTime(new Date().toISOString())}`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
