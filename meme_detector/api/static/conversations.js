const state = {
  limit: 20,
  offset: 0,
  total: 0,
  selectedId: null,
  items: [],
};

const elements = {
  refreshButton: document.getElementById("refreshButton"),
  applyFilter: document.getElementById("applyFilter"),
  prevPage: document.getElementById("prevPage"),
  nextPage: document.getElementById("nextPage"),
  agentFilter: document.getElementById("agentFilter"),
  statusFilter: document.getElementById("statusFilter"),
  runIdFilter: document.getElementById("runIdFilter"),
  wordFilter: document.getElementById("wordFilter"),
  conversationList: document.getElementById("conversationList"),
  conversationDetail: document.getElementById("conversationDetail"),
  pageSummary: document.getElementById("pageSummary"),
  lastUpdated: document.getElementById("lastUpdated"),
};

elements.refreshButton.addEventListener("click", () => {
  loadConversations();
});

elements.applyFilter.addEventListener("click", () => {
  state.offset = 0;
  loadConversations();
});

elements.prevPage.addEventListener("click", () => {
  if (state.offset === 0) {
    return;
  }
  state.offset = Math.max(0, state.offset - state.limit);
  loadConversations();
});

elements.nextPage.addEventListener("click", () => {
  if (state.offset + state.limit >= state.total) {
    return;
  }
  state.offset += state.limit;
  loadConversations();
});

loadConversations();

async function loadConversations() {
  try {
    const params = new URLSearchParams({
      limit: String(state.limit),
      offset: String(state.offset),
    });
    const runId = elements.runIdFilter.value.trim();
    const word = elements.wordFilter.value.trim();
    const status = elements.statusFilter.value;
    const agentName = elements.agentFilter.value;
    if (agentName) {
      params.set("agent_name", agentName);
    }
    if (runId) {
      params.set("run_id", runId);
    }
    if (word) {
      params.set("word", word);
    }
    if (status) {
      params.set("status", status);
    }

    const response = await fetchJson(`/api/v1/agent-conversations?${params.toString()}`);
    state.total = response.total || 0;
    state.items = response.items || [];
    if (!state.items.length) {
      state.selectedId = null;
    } else if (!state.items.some((item) => item.id === state.selectedId)) {
      state.selectedId = state.items[0].id;
    }
    renderList();
    renderPager();
    setLastUpdated();
    await loadSelectedDetail();
  } catch (error) {
    elements.conversationList.innerHTML = `<div class="empty-state">${escapeHtml(error.message || String(error))}</div>`;
    elements.conversationDetail.className = "conversation-detail";
    elements.conversationDetail.innerHTML = `<div class="error-box mono">${escapeHtml(error.message || String(error))}</div>`;
  }
}

async function loadSelectedDetail() {
  if (!state.selectedId) {
    elements.conversationDetail.className = "conversation-detail empty-state";
    elements.conversationDetail.textContent = "当前筛选条件下暂无对话记录";
    return;
  }

  try {
    const detail = await fetchJson(`/api/v1/agent-conversations/${state.selectedId}`);
    renderDetail(detail);
  } catch (error) {
    elements.conversationDetail.className = "conversation-detail";
    elements.conversationDetail.innerHTML = `<div class="error-box mono">${escapeHtml(error.message || String(error))}</div>`;
  }
}

function renderList() {
  if (!state.items.length) {
    elements.conversationList.innerHTML = '<div class="empty-state">暂无对话记录</div>';
    return;
  }

  elements.conversationList.innerHTML = state.items
    .map((item) => {
      const activeClass = item.id === state.selectedId ? "active" : "";
      return `
        <article class="conversation-item ${activeClass}" data-id="${item.id}">
          <div class="run-header">
            <h3 class="run-title">${escapeHtml(item.word)}</h3>
            ${statusBadge(item.status)}
          </div>
          <p class="run-meta">agent: <span class="mono">${escapeHtml(item.agent_name || "--")}</span></p>
          <p class="run-meta">run: <span class="mono">${escapeHtml(shortId(item.run_id))}</span></p>
          <p class="run-meta">${formatDateTime(item.started_at)} · ${item.message_count || 0} 条消息</p>
          <p class="run-meta">${escapeHtml(item.summary || "暂无摘要")}</p>
        </article>
      `;
    })
    .join("");

  elements.conversationList.querySelectorAll(".conversation-item").forEach((item) => {
    item.addEventListener("click", async () => {
      state.selectedId = item.dataset.id;
      renderList();
      await loadSelectedDetail();
    });
  });
}

function renderDetail(detail) {
  elements.conversationDetail.className = "conversation-detail";
  elements.conversationDetail.innerHTML = `
    <div class="detail-card">
      <div class="detail-top">
        <div>
          <p class="section-kicker">${escapeHtml(detail.agent_name || "agent")}</p>
          <h3>${escapeHtml(detail.word)}</h3>
        </div>
        ${statusBadge(detail.status)}
      </div>
      <div class="detail-grid">
        <div class="metric">
          <span class="section-kicker">运行记录</span>
          <strong class="mono">${escapeHtml(shortId(detail.run_id))}</strong>
        </div>
        <div class="metric">
          <span class="section-kicker">消息数</span>
          <strong>${detail.message_count || 0}</strong>
        </div>
        <div class="metric">
          <span class="section-kicker">结束时间</span>
          <strong>${escapeHtml(formatDateTime(detail.finished_at))}</strong>
        </div>
      </div>
      <p class="detail-meta">开始：${formatDateTime(detail.started_at)}</p>
      <p class="detail-meta">摘要：${escapeHtml(detail.summary || "--")}</p>
      ${detail.error_message ? `<div class="error-box mono">${escapeHtml(detail.error_message)}</div>` : ""}
      <div class="detail-block">
        <h4>最终输出</h4>
        <pre>${escapeHtml(JSON.stringify(detail.output || {}, null, 2))}</pre>
      </div>
      <div class="detail-block">
        <h4>完整消息</h4>
        <pre>${escapeHtml(JSON.stringify(detail.messages || [], null, 2))}</pre>
      </div>
    </div>
  `;
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
  const currentPage = Math.floor(state.offset / state.limit) + 1;
  const totalPages = Math.max(1, Math.ceil(state.total / state.limit));
  elements.pageSummary.textContent = `第 ${currentPage} / ${totalPages} 页，显示 ${start}-${end}，共 ${state.total} 条`;
  elements.prevPage.disabled = state.offset === 0;
  elements.nextPage.disabled = state.offset + state.limit >= state.total;
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`请求失败: ${response.status}`);
  }
  return response.json();
}

function statusBadge(status) {
  const normalized = String(status || "running").toLowerCase();
  const labelMap = {
    running: "运行中",
    success: "成功",
    failed: "失败",
  };
  return `<span class="badge ${normalized}">${labelMap[normalized] || escapeHtml(normalized)}</span>`;
}

function shortId(value) {
  if (!value) {
    return "--";
  }
  return value.length > 12 ? `${value.slice(0, 12)}...` : value;
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
