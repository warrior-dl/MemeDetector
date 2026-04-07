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
  statusFilter: document.getElementById("statusFilter"),
  bvidFilter: document.getElementById("bvidFilter"),
  keywordFilter: document.getElementById("keywordFilter"),
  onlyMeme: document.getElementById("onlyMeme"),
  onlyInsider: document.getElementById("onlyInsider"),
  insightList: document.getElementById("insightList"),
  insightDetail: document.getElementById("insightDetail"),
  pageSummary: document.getElementById("pageSummary"),
  lastUpdated: document.getElementById("lastUpdated"),
};

elements.refreshButton.addEventListener("click", () => {
  loadInsights();
});

elements.applyFilter.addEventListener("click", () => {
  state.offset = 0;
  loadInsights();
});

elements.prevPage.addEventListener("click", () => {
  if (state.offset === 0) {
    return;
  }
  state.offset = Math.max(0, state.offset - state.limit);
  loadInsights();
});

elements.nextPage.addEventListener("click", () => {
  if (state.offset + state.limit >= state.total) {
    return;
  }
  state.offset += state.limit;
  loadInsights();
});

loadInsights();

async function loadInsights() {
  try {
    const params = new URLSearchParams({
      limit: String(state.limit),
      offset: String(state.offset),
    });
    const status = elements.statusFilter.value;
    const bvid = elements.bvidFilter.value.trim();
    const keyword = elements.keywordFilter.value.trim();

    if (status) {
      params.set("status", status);
    }
    if (bvid) {
      params.set("bvid", bvid);
    }
    if (keyword) {
      params.set("keyword", keyword);
    }
    if (elements.onlyMeme.checked) {
      params.set("only_meme_candidates", "true");
    }
    if (elements.onlyInsider.checked) {
      params.set("only_insider_knowledge", "true");
    }

    const response = await fetchJson(`/api/v1/miner/comment-insights?${params.toString()}`);
    state.total = response.total || 0;
    state.items = response.items || [];

    if (!state.items.length) {
      state.selectedId = null;
    } else if (!state.items.some((item) => item.insight_id === state.selectedId)) {
      state.selectedId = state.items[0].insight_id;
    }

    renderList();
    renderPager();
    setLastUpdated();
    await loadSelectedDetail();
  } catch (error) {
    elements.insightList.innerHTML = `<div class="empty-state">${escapeHtml(error.message || String(error))}</div>`;
    elements.insightDetail.className = "conversation-detail";
    elements.insightDetail.innerHTML = `<div class="error-box mono">${escapeHtml(error.message || String(error))}</div>`;
  }
}

async function loadSelectedDetail() {
  if (!state.selectedId) {
    elements.insightDetail.className = "conversation-detail empty-state";
    elements.insightDetail.textContent = "当前筛选条件下暂无评论线索";
    return;
  }

  try {
    const detail = await fetchJson(`/api/v1/miner/comment-insights/${encodeURIComponent(state.selectedId)}`);
    renderDetail(detail);
  } catch (error) {
    elements.insightDetail.className = "conversation-detail";
    elements.insightDetail.innerHTML = `<div class="error-box mono">${escapeHtml(error.message || String(error))}</div>`;
  }
}

function renderList() {
  if (!state.items.length) {
    elements.insightList.innerHTML = '<div class="empty-state">暂无 Miner 评论线索</div>';
    return;
  }

  elements.insightList.innerHTML = state.items
    .map((item) => {
      const activeClass = item.insight_id === state.selectedId ? "active" : "";
      const badges = [
        item.is_meme_candidate ? '<span class="badge accepted">潜在梗</span>' : '',
        item.is_insider_knowledge ? '<span class="badge success">圈内知识</span>' : '',
        statusBadge(item.status),
      ].join(' ');
      return `
        <article class="conversation-item ${activeClass}" data-id="${escapeHtml(item.insight_id)}">
          <div class="run-header">
            <h3 class="run-title">${escapeHtml(item.title || item.bvid || '--')}</h3>
            <div class="chips">${badges}</div>
          </div>
          <p class="run-meta"><span class="mono">${escapeHtml(item.bvid || '--')}</span> · ${escapeHtml(item.partition || '--')} · 置信度 ${formatConfidence(item.confidence)}</p>
          <p class="run-meta">${escapeHtml(item.comment_text || '--')}</p>
          <p class="run-meta">${escapeHtml(item.reason || '无理由')}</p>
        </article>
      `;
    })
    .join('');

  elements.insightList.querySelectorAll('.conversation-item').forEach((item) => {
    item.addEventListener('click', async () => {
      state.selectedId = item.dataset.id;
      renderList();
      await loadSelectedDetail();
    });
  });
}

function renderDetail(detail) {
  const tags = Array.isArray(detail.tags) ? detail.tags : [];
  const context = detail.video_context || {};

  elements.insightDetail.className = 'conversation-detail';
  elements.insightDetail.innerHTML = `
    <div class="detail-card">
      <div class="detail-top">
        <div>
          <p class="section-kicker">${escapeHtml(detail.partition || '--')}</p>
          <h3>${escapeHtml(detail.title || detail.bvid || '--')}</h3>
        </div>
        ${statusBadge(detail.status)}
      </div>
      <div class="detail-grid">
        <div class="metric">
          <span class="section-kicker">BVID</span>
          <strong class="mono">${escapeHtml(detail.bvid || '--')}</strong>
        </div>
        <div class="metric">
          <span class="section-kicker">采集日期</span>
          <strong>${escapeHtml(formatDateTime(detail.collected_date))}</strong>
        </div>
        <div class="metric">
          <span class="section-kicker">置信度</span>
          <strong>${formatConfidence(detail.confidence)}</strong>
        </div>
        <div class="metric">
          <span class="section-kicker">分类</span>
          <strong>${formatInsightKinds(detail)}</strong>
        </div>
      </div>
      <p class="detail-meta">写入时间：${formatDateTime(detail.created_at)} · 更新时间：${formatDateTime(detail.updated_at)}</p>
      <div class="detail-block">
        <h4>视频链接</h4>
        <p><a class="inline-link" href="${escapeAttribute(detail.video_url || '#')}" target="_blank" rel="noreferrer">${escapeHtml(detail.video_url || '--')}</a></p>
      </div>
      <div class="detail-block">
        <h4>视频简介</h4>
        <p>${escapeHtml(detail.description || '--')}</p>
      </div>
      <div class="detail-block">
        <h4>视频标签</h4>
        ${tags.length
          ? `<div class="chips">${tags.map((tag) => `<span class="chip">${escapeHtml(tag)}</span>`).join('')}</div>`
          : '<p class="detail-meta">当前线索没有标签数据。</p>'}
      </div>
      <div class="detail-block">
        <h4>评论文本</h4>
        <p class="comment-copy">${escapeHtml(detail.comment_text || '--')}</p>
      </div>
      <div class="detail-block">
        <h4>打分理由</h4>
        <p>${escapeHtml(detail.reason || '--')}</p>
      </div>
      <div class="detail-block">
        <h4>视频内容摘要</h4>
        <p>${escapeHtml(context.summary || '--')}</p>
      </div>
      <div class="detail-block">
        <h4>视频内容正文</h4>
        <pre>${escapeHtml(context.content_text || '--')}</pre>
      </div>
      <div class="detail-block">
        <h4>字幕摘录</h4>
        <pre>${escapeHtml(context.transcript_excerpt || '--')}</pre>
      </div>
    </div>
  `;
}

function renderPager() {
  if (state.total === 0) {
    elements.pageSummary.textContent = '共 0 条';
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
  const normalized = String(status || 'pending').toLowerCase();
  const labelMap = {
    pending: '待消费',
    processed: '已消费',
    accepted: '潜在梗',
    success: '圈内知识',
  };
  return `<span class="badge ${normalized}">${labelMap[normalized] || escapeHtml(normalized)}</span>`;
}

function formatDateTime(value) {
  if (!value) {
    return '--';
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(String(value))) {
    return escapeHtml(String(value));
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return escapeHtml(String(value));
  }
  return new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(date);
}

function formatConfidence(value) {
  return Number(value || 0).toFixed(2);
}

function formatInsightKinds(item) {
  const kinds = [];
  if (item.is_meme_candidate) {
    kinds.push('潜在梗');
  }
  if (item.is_insider_knowledge) {
    kinds.push('圈内知识');
  }
  return escapeHtml(kinds.join(' / ') || '普通评论');
}

function setLastUpdated() {
  elements.lastUpdated.textContent = `最近刷新：${formatDateTime(new Date().toISOString())}`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function escapeAttribute(value) {
  return escapeHtml(value).replaceAll('`', '&#96;');
}
