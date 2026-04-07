const state = {
  limit: 20,
  offset: 0,
  total: 0,
  selectedKey: null,
  items: [],
};

const elements = {
  refreshButton: document.getElementById("refreshButton"),
  applyFilter: document.getElementById("applyFilter"),
  prevPage: document.getElementById("prevPage"),
  nextPage: document.getElementById("nextPage"),
  statusFilter: document.getElementById("statusFilter"),
  partitionFilter: document.getElementById("partitionFilter"),
  keywordFilter: document.getElementById("keywordFilter"),
  snapshotList: document.getElementById("snapshotList"),
  snapshotDetail: document.getElementById("snapshotDetail"),
  pageSummary: document.getElementById("pageSummary"),
  lastUpdated: document.getElementById("lastUpdated"),
};

elements.refreshButton.addEventListener("click", () => {
  loadSnapshots();
});

elements.applyFilter.addEventListener("click", () => {
  state.offset = 0;
  loadSnapshots();
});

elements.prevPage.addEventListener("click", () => {
  if (state.offset === 0) {
    return;
  }
  state.offset = Math.max(0, state.offset - state.limit);
  loadSnapshots();
});

elements.nextPage.addEventListener("click", () => {
  if (state.offset + state.limit >= state.total) {
    return;
  }
  state.offset += state.limit;
  loadSnapshots();
});

loadSnapshots();

async function loadSnapshots() {
  try {
    const params = new URLSearchParams({
      limit: String(state.limit),
      offset: String(state.offset),
    });
    const candidateStatus = elements.statusFilter.value;
    const partition = elements.partitionFilter.value.trim();
    const keyword = elements.keywordFilter.value.trim();

    if (candidateStatus) {
      params.set("candidate_status", candidateStatus);
    }
    if (partition) {
      params.set("partition", partition);
    }
    if (keyword) {
      params.set("keyword", keyword);
    }

    const response = await fetchJson(`/api/v1/scout/raw-videos?${params.toString()}`);
    state.total = response.total || 0;
    state.items = response.items || [];

    if (!state.items.length) {
      state.selectedKey = null;
    } else if (!state.items.some((item) => itemKey(item) === state.selectedKey)) {
      state.selectedKey = itemKey(state.items[0]);
    }

    renderList();
    renderPager();
    setLastUpdated();
    await loadSelectedDetail();
  } catch (error) {
    elements.snapshotList.innerHTML = `<div class="empty-state">${escapeHtml(error.message || String(error))}</div>`;
    elements.snapshotDetail.className = "conversation-detail";
    elements.snapshotDetail.innerHTML = `<div class="error-box mono">${escapeHtml(error.message || String(error))}</div>`;
  }
}

async function loadSelectedDetail() {
  const selectedItem = state.items.find((item) => itemKey(item) === state.selectedKey);
  if (!selectedItem) {
    elements.snapshotDetail.className = "conversation-detail empty-state";
    elements.snapshotDetail.textContent = "当前筛选条件下暂无原始快照";
    return;
  }

  try {
    const params = new URLSearchParams({ collected_date: selectedItem.collected_date });
    const detail = await fetchJson(`/api/v1/scout/raw-videos/${encodeURIComponent(selectedItem.bvid)}?${params.toString()}`);
    renderDetail(detail);
  } catch (error) {
    elements.snapshotDetail.className = "conversation-detail";
    elements.snapshotDetail.innerHTML = `<div class="error-box mono">${escapeHtml(error.message || String(error))}</div>`;
  }
}

function renderList() {
  if (!state.items.length) {
    elements.snapshotList.innerHTML = '<div class="empty-state">暂无 Scout 原始快照</div>';
    return;
  }

  elements.snapshotList.innerHTML = state.items
    .map((item) => {
      const activeClass = itemKey(item) === state.selectedKey ? "active" : "";
      const title = item.title || item.bvid || "--";
      return `
        <article class="conversation-item ${activeClass}" data-key="${escapeHtml(itemKey(item))}">
          <div class="run-header">
            <h3 class="run-title">${escapeHtml(title)}</h3>
            ${statusBadge(item.candidate_status)}
          </div>
          <p class="run-meta">${escapeHtml(item.partition || "--")} · ${(item.tags || []).length} 个标签 · ${item.comment_count ?? 0} 条评论 · ${item.picture_count ?? 0} 张图</p>
          <p class="run-meta">${formatDateTime(item.collected_date)} · <span class="mono">${escapeHtml(item.bvid)}</span></p>
          <p class="run-meta">${escapeHtml(item.first_comment || "暂无评论预览")}</p>
        </article>
      `;
    })
    .join("");

  elements.snapshotList.querySelectorAll(".conversation-item").forEach((item) => {
    item.addEventListener("click", async () => {
      state.selectedKey = item.dataset.key;
      renderList();
      await loadSelectedDetail();
    });
  });
}

function renderDetail(detail) {
  const commentSnapshots = Array.isArray(detail.comment_snapshots) ? detail.comment_snapshots : [];
  const comments = Array.isArray(detail.comments) ? detail.comments : [];
  const tags = Array.isArray(detail.tags) ? detail.tags : [];
  elements.snapshotDetail.className = "conversation-detail";
  elements.snapshotDetail.innerHTML = `
    <div class="detail-card">
      <div class="detail-top">
        <div>
          <p class="section-kicker">${escapeHtml(detail.partition || "--")}</p>
          <h3>${escapeHtml(detail.title || detail.bvid || "--")}</h3>
        </div>
        ${statusBadge(detail.candidate_status)}
      </div>
      <div class="detail-grid">
        <div class="metric">
          <span class="section-kicker">BVID</span>
          <strong class="mono">${escapeHtml(detail.bvid || "--")}</strong>
        </div>
        <div class="metric">
          <span class="section-kicker">采集日期</span>
          <strong>${escapeHtml(formatDateTime(detail.collected_date))}</strong>
        </div>
        <div class="metric">
          <span class="section-kicker">评论数</span>
          <strong>${detail.comment_count ?? 0}</strong>
        </div>
        <div class="metric">
          <span class="section-kicker">评论图片</span>
          <strong>${detail.picture_count ?? 0}</strong>
        </div>
        <div class="metric">
          <span class="section-kicker">提取完成时间</span>
          <strong>${escapeHtml(formatDateTime(detail.candidate_extracted_at))}</strong>
        </div>
      </div>
      <p class="detail-meta">写入时间：${formatDateTime(detail.created_at)} · 更新时间：${formatDateTime(detail.updated_at)}</p>
      <div class="detail-block">
        <h4>视频链接</h4>
        <p><a class="inline-link" href="${escapeAttribute(detail.video_url || "#")}" target="_blank" rel="noreferrer">${escapeHtml(detail.video_url || "--")}</a></p>
      </div>
      <div class="detail-block">
        <h4>视频描述</h4>
        <p>${escapeHtml(detail.description || "--")}</p>
      </div>
      <div class="detail-block">
        <h4>视频标签</h4>
        ${tags.length
          ? `<div class="chips">${tags.map((tag) => `<span class="chip">${escapeHtml(tag)}</span>`).join("")}</div>`
          : '<p class="detail-meta">当前快照没有标签数据。</p>'}
      </div>
      <div class="detail-block">
        <h4>评论快照</h4>
        ${commentSnapshots.length
          ? `<div class="comment-thread">${commentSnapshots.map((snapshot) => renderCommentSnapshot(snapshot)).join("")}</div>`
          : comments.length
            ? `<ol class="comment-list">${comments.map((comment) => `<li class="comment-item">${escapeHtml(comment)}</li>`).join("")}</ol>`
            : '<p class="detail-meta">当前快照没有可展示的评论。</p>'}
      </div>
    </div>
  `;
}

function renderCommentSnapshot(snapshot) {
  const pictures = Array.isArray(snapshot.pictures) ? snapshot.pictures : [];
  const pictureSummary = pictures.length ? `${pictures.length} 张图` : "无图";
  return `
    <article class="comment-card">
      <div class="run-header">
        <strong>${escapeHtml(snapshot.uname || "匿名用户")}</strong>
        <span class="badge pending">${escapeHtml(pictureSummary)}</span>
      </div>
      <p class="run-meta">rpid: <span class="mono">${escapeHtml(String(snapshot.rpid || "--"))}</span> · 点赞 ${snapshot.like_count ?? 0} · 回复 ${snapshot.reply_count ?? 0}</p>
      <p class="comment-copy">${escapeHtml(snapshot.message || "--")}</p>
      ${pictures.length ? `<div class="image-grid">${pictures.map((picture) => renderPicture(picture)).join("")}</div>` : ""}
    </article>
  `;
}

function renderPicture(picture) {
  const localUrl = picture.asset_id ? `/api/v1/media-assets/${encodeURIComponent(picture.asset_id)}/content` : "";
  const imageUrl = picture.download_status === "success" && localUrl ? localUrl : picture.source_url || "";
  const meta = [];
  if (picture.width && picture.height) {
    meta.push(`${picture.width}x${picture.height}`);
  }
  if (picture.byte_size) {
    meta.push(formatBytes(picture.byte_size));
  }
  meta.push(picture.download_status === "success" ? "已落盘" : "仅源链");
  return `
    <a class="image-tile" href="${escapeAttribute(imageUrl || "#")}" target="_blank" rel="noreferrer">
      ${imageUrl ? `<img src="${escapeAttribute(imageUrl)}" alt="comment image" loading="lazy" />` : '<div class="image-empty">无图片</div>'}
      <span class="image-meta">${escapeHtml(meta.join(" · "))}</span>
    </a>
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

function itemKey(item) {
  return `${item.bvid || ""}::${item.collected_date || ""}`;
}

function statusBadge(status) {
  const normalized = String(status || "pending").toLowerCase();
  const labelMap = {
    pending: "待提取",
    processed: "已提取",
  };
  return `<span class="badge ${normalized}">${labelMap[normalized] || escapeHtml(normalized)}</span>`;
}

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(String(value))) {
    return escapeHtml(String(value));
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

function formatBytes(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num) || num <= 0) {
    return "--";
  }
  if (num >= 1024 * 1024) {
    return `${(num / (1024 * 1024)).toFixed(1)} MB`;
  }
  if (num >= 1024) {
    return `${(num / 1024).toFixed(1)} KB`;
  }
  return `${num} B`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttribute(value) {
  return escapeHtml(value).replaceAll("`", "&#96;");
}
