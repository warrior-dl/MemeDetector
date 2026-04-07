const elements = {
  refreshButton: document.getElementById("refreshButton"),
  pageSubtitle: document.getElementById("pageSubtitle"),
  lastUpdated: document.getElementById("lastUpdated"),
  candidateSummary: document.getElementById("candidateSummary"),
  videoRefList: document.getElementById("videoRefList"),
  sourceInsightList: document.getElementById("sourceInsightList"),
};

const params = new URLSearchParams(window.location.search);
const word = params.get("word") || "";

elements.refreshButton.addEventListener("click", () => {
  loadSources();
});

loadSources();

async function loadSources() {
  if (!word) {
    renderError("缺少候选词参数 word");
    return;
  }

  try {
    const data = await fetchJson(`/api/v1/candidates/${encodeURIComponent(word)}/sources`);
    renderCandidate(data.candidate || {});
    renderVideoRefs(data.video_refs || []);
    renderSourceInsights(data.source_insights || []);
    elements.pageSubtitle.textContent = `当前查看候选词「${word}」的来源视频和评论线索。`;
    elements.lastUpdated.textContent = `最近刷新：${formatDateTime(new Date().toISOString())}`;
  } catch (error) {
    renderError(error.message || String(error));
  }
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`请求失败: ${response.status}`);
  }
  return response.json();
}

function renderCandidate(candidate) {
  const refs = Array.isArray(candidate.video_refs) ? candidate.video_refs : [];
  elements.candidateSummary.className = "conversation-detail";
  elements.candidateSummary.innerHTML = `
    <div class="detail-card">
      <div class="detail-top">
        <div>
          <p class="section-kicker">Candidate</p>
          <h3>${escapeHtml(candidate.word || "--")}</h3>
        </div>
        ${statusBadge(candidate.status)}
      </div>
      <div class="detail-grid">
        <div class="metric">
          <span class="section-kicker">分数</span>
          <strong>${Number(candidate.score || 0).toFixed(2)}</strong>
        </div>
        <div class="metric">
          <span class="section-kicker">新词</span>
          <strong>${candidate.is_new_word ? "是" : "否"}</strong>
        </div>
        <div class="metric">
          <span class="section-kicker">关联视频</span>
          <strong>${refs.length}</strong>
        </div>
      </div>
      <p class="detail-meta">发现时间：${formatDateTime(candidate.detected_at)}</p>
      <div class="detail-block">
        <h4>说明</h4>
        <p>${escapeHtml(candidate.explanation || "--")}</p>
      </div>
      <div class="detail-block">
        <h4>评论样本</h4>
        <pre>${escapeHtml(candidate.sample_comments || "--")}</pre>
      </div>
    </div>
  `;
}

function renderVideoRefs(items) {
  if (!items.length) {
    elements.videoRefList.innerHTML = '<div class="empty-state">没有关联视频</div>';
    return;
  }

  elements.videoRefList.innerHTML = items
    .map((item) => {
      const tags = Array.isArray(item.tags) ? item.tags : [];
      const matchedComments = Array.isArray(item.matched_comments) ? item.matched_comments : [];
      return `
        <article class="conversation-item">
          <div class="run-header">
            <h3 class="run-title">${escapeHtml(item.title || item.bvid || "--")}</h3>
            <span class="badge pending">匹配 ${Number(item.matched_comment_count || 0)}</span>
          </div>
          <p class="run-meta"><span class="mono">${escapeHtml(item.bvid || "--")}</span> · ${escapeHtml(item.partition || "--")}</p>
          <p class="run-meta"><a class="inline-link" href="${escapeAttribute(item.url || "#")}" target="_blank" rel="noreferrer">${escapeHtml(item.url || "--")}</a></p>
          ${tags.length ? `<div class="chips">${tags.map((tag) => `<span class="chip">${escapeHtml(tag)}</span>`).join("")}</div>` : ""}
          <div class="detail-block">
            <h4>匹配评论</h4>
            ${matchedComments.length
              ? `<ul class="comment-list">${matchedComments.map((comment) => `<li class="comment-item">${escapeHtml(comment)}</li>`).join("")}</ul>`
              : '<p class="detail-meta">该视频是通过标题、简介或标签关联上的。</p>'}
          </div>
        </article>
      `;
    })
    .join("");
}

function renderSourceInsights(items) {
  if (!items.length) {
    elements.sourceInsightList.innerHTML = '<div class="empty-state">没有匹配到来源评论线索</div>';
    return;
  }

  elements.sourceInsightList.innerHTML = items
    .map((item) => {
      const badges = [
        item.is_meme_candidate ? '<span class="badge accepted">潜在梗</span>' : "",
        item.is_insider_knowledge ? '<span class="badge success">圈内知识</span>' : "",
        item.matched_by_video_ref_comments ? '<span class="badge processed">评论命中</span>' : "",
        item.matched_by_candidate_word ? '<span class="badge pending">词条命中</span>' : "",
      ].filter(Boolean).join(" ");
      return `
        <article class="conversation-item">
          <div class="run-header">
            <h3 class="run-title">${escapeHtml(item.title || item.bvid || "--")}</h3>
            <div class="chips">${badges}</div>
          </div>
          <p class="run-meta"><span class="mono">${escapeHtml(item.bvid || "--")}</span> · 置信度 ${formatConfidence(item.confidence)}</p>
          <p class="run-meta">${escapeHtml(item.reason || "--")}</p>
          <p class="comment-copy">${escapeHtml(item.comment_text || "--")}</p>
          <p class="detail-meta"><a class="inline-link" href="/admin/miner" target="_blank" rel="noreferrer">在 Miner 页面继续查看</a></p>
        </article>
      `;
    })
    .join("");
}

function renderError(message) {
  elements.candidateSummary.className = "conversation-detail";
  elements.candidateSummary.innerHTML = `<div class="error-box mono">${escapeHtml(message)}</div>`;
  elements.videoRefList.innerHTML = '<div class="empty-state">--</div>';
  elements.sourceInsightList.innerHTML = '<div class="empty-state">--</div>';
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

function formatConfidence(value) {
  return Number(value || 0).toFixed(2);
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

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttribute(value) {
  return escapeHtml(value);
}
