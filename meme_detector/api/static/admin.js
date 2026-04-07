const state = {
  runs: [],
  selectedRunId: null,
  triggeringJobs: new Set(),
};

const endpoints = {
  stats: "/api/v1/stats",
  jobs: "/api/v1/jobs",
  runs: "/api/v1/runs",
  rawVideos: "/api/v1/scout/raw-videos?limit=10",
  candidates: "/api/v1/candidates?limit=20",
  memes: "/api/v1/memes?limit=20&sort_by=updated_at:desc",
};

document.getElementById("refreshButton").addEventListener("click", () => {
  loadDashboard();
});

document.getElementById("jobFilter").addEventListener("change", () => {
  loadRuns();
});

document.getElementById("statusFilter").addEventListener("change", () => {
  loadRuns();
});

document.getElementById("runScoutButton").addEventListener("click", () => {
  triggerJob("scout");
});

document.getElementById("runMinerButton").addEventListener("click", () => {
  triggerJob("miner");
});

document.getElementById("runResearchButton").addEventListener("click", () => {
  triggerJob("research");
});

loadDashboard();

async function loadDashboard() {
  try {
    const [statsResponse, jobsResponse, rawVideosResponse, candidatesResponse, memesResponse] = await Promise.all([
      fetchJson(endpoints.stats),
      fetchJson(endpoints.jobs),
      fetchJson(endpoints.rawVideos),
      fetchJson(endpoints.candidates),
      fetchJson(endpoints.memes),
    ]);

    renderStats(statsResponse);
    renderJobs(jobsResponse);
    renderRawVideos(rawVideosResponse.items || []);
    renderCandidates(candidatesResponse);
    renderMemes(memesResponse.hits || []);
    await loadRuns();
    setLastUpdated();
  } catch (error) {
    renderFatalError(error);
  }
}

async function loadRuns() {
  const params = new URLSearchParams({ limit: "50" });
  const jobName = document.getElementById("jobFilter").value;
  const status = document.getElementById("statusFilter").value;
  if (jobName) {
    params.set("job_name", jobName);
  }
  if (status) {
    params.set("status", status);
  }

  const runs = await fetchJson(`${endpoints.runs}?${params.toString()}`);
  state.runs = runs;
  if (!runs.length) {
    state.selectedRunId = null;
  } else if (!runs.some((item) => item.id === state.selectedRunId)) {
    state.selectedRunId = runs[0].id;
  }
  renderRuns(runs);
  renderSelectedRun();
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`请求失败: ${response.status}`);
  }
  return response.json();
}

function renderStats(data) {
  const candidates = data.candidates || {};
  setText("pendingCount", candidates.pending ?? "--");
  setText("acceptedCount", candidates.accepted ?? "--");
  setText("rejectedCount", candidates.rejected ?? "--");
  setText("memeCount", data.memes_in_library ?? "--");
}

function renderJobs(jobs) {
  const container = document.getElementById("jobsGrid");
  if (!jobs.length) {
    container.innerHTML = '<div class="empty-state">调度器未注册任务</div>';
    return;
  }

  container.innerHTML = jobs
    .map(
      (job) => `
        <article class="job-card">
          <p class="section-kicker">${escapeHtml(job.id)}</p>
          <h3>${escapeHtml(job.name)}</h3>
          <p>下一次运行：${formatDateTime(job.next_run_time)}</p>
          <p>当前状态：${job.is_running ? "运行中" : "空闲"}</p>
          <p class="mono">${escapeHtml(job.trigger || "--")}</p>
          ${job.last_error ? `<p class="run-meta">${escapeHtml(job.last_error)}</p>` : ""}
        </article>
      `,
    )
    .join("");
}

function renderRuns(runs) {
  const container = document.getElementById("runsList");
  if (!runs.length) {
    container.innerHTML = '<div class="empty-state">暂无运行记录</div>';
    return;
  }

  container.innerHTML = runs
    .map((run) => {
      const activeClass = run.id === state.selectedRunId ? "active" : "";
      return `
        <article class="run-item ${activeClass}" data-run-id="${run.id}">
          <div class="run-header">
            <h3 class="run-title">${formatJobName(run.job_name)}</h3>
            ${statusBadge(run.status)}
          </div>
          <p class="run-meta">${formatDateTime(run.started_at)} · ${formatTrigger(run.trigger_mode)}</p>
          <p class="run-meta">${escapeHtml(run.summary || "暂无摘要")}</p>
        </article>
      `;
    })
    .join("");

  container.querySelectorAll(".run-item").forEach((item) => {
    item.addEventListener("click", () => {
      state.selectedRunId = item.dataset.runId;
      renderRuns(state.runs);
      renderSelectedRun();
    });
  });
}

function renderSelectedRun() {
  const run = state.runs.find((item) => item.id === state.selectedRunId);
  const container = document.getElementById("runDetail");

  if (!run) {
    container.className = "run-detail empty-state";
    container.textContent = "选择左侧运行记录查看详情";
    return;
  }

  container.className = "run-detail";
  container.innerHTML = `
    <div class="detail-card">
      <div class="detail-top">
        <div>
          <p class="section-kicker">${formatJobName(run.job_name)}</p>
          <h3>${escapeHtml(run.summary || "本次运行")}</h3>
        </div>
        ${statusBadge(run.status)}
      </div>
      <div class="detail-grid">
        <div class="metric">
          <span class="section-kicker">触发方式</span>
          <strong>${escapeHtml(formatTrigger(run.trigger_mode))}</strong>
        </div>
        <div class="metric">
          <span class="section-kicker">结果数量</span>
          <strong>${run.result_count ?? 0}</strong>
        </div>
        <div class="metric">
          <span class="section-kicker">耗时</span>
          <strong>${formatDuration(run.duration_seconds)}</strong>
        </div>
      </div>
      <div class="detail-strip">
        <p class="detail-meta">开始：${formatDateTime(run.started_at)}</p>
        <p class="detail-meta">结束：${formatDateTime(run.finished_at)}</p>
      </div>
      ${renderRunPayload(run)}
    </div>
  `;
}

function renderRunPayload(run) {
  const payload = run.payload || {};

  if (run.error_message) {
    return `<div class="error-box mono">${escapeHtml(run.error_message)}</div>`;
  }

  if (run.job_name === "scout") {
    const targetDate = payload.target_date || "--";
    const videoCount = payload.video_count ?? 0;
    const commentCount = payload.comment_count ?? 0;
    return `
      <div class="detail-summary">
        <p>本次 Scout 于 ${escapeHtml(targetDate)} 写入 ${videoCount} 个原始视频快照，采集 ${commentCount} 条评论。</p>
        <div class="chips">
          <span class="chip">采集日期 ${escapeHtml(targetDate)}</span>
          <span class="chip">视频 ${videoCount}</span>
          <span class="chip">评论 ${commentCount}</span>
        </div>
      </div>
    `;
  }

  if (run.job_name === "miner") {
    const targetDate = payload.target_date || "--";
    const videoCount = payload.video_count ?? 0;
    const insightCount = payload.insight_count ?? 0;
    const highValueCount = payload.high_value_count ?? 0;
    return `
      <div class="detail-summary">
        <p>本次 Miner 处理 ${videoCount} 个视频，写入 ${insightCount} 条评论线索，其中 ${highValueCount} 条达到高价值阈值。</p>
        <div class="chips">
          <span class="chip">采集日期 ${escapeHtml(targetDate)}</span>
          <span class="chip">视频 ${videoCount}</span>
          <span class="chip">线索 ${insightCount}</span>
          <span class="chip">高价值 ${highValueCount}</span>
        </div>
      </div>
    `;
  }

  if (run.job_name === "research") {
    const accepted = payload.accepted_records || [];
    const rejected = payload.rejected_words || [];
    const failed = payload.failed_words || [];
    const screenFailed = payload.screen_failed_words || [];
    return `
      <div class="detail-summary">
        <p>筛选 ${payload.screened_count || 0} 个候选，深度分析 ${payload.deep_analysis_count || 0} 个。</p>
        <div class="detail-list">
          ${accepted.map((item) => `<span class="chip">${escapeHtml(item.title || item.id)}</span>`).join("") || '<span class="chip muted">本次无新增入库</span>'}
        </div>
        <p class="detail-meta">拒绝候选：${rejected.length ? escapeHtml(rejected.join("、")) : "无"}</p>
        <p class="detail-meta">待重试候选：${screenFailed.length ? escapeHtml(screenFailed.join("、")) : "无"}</p>
        <p class="detail-meta">分析失败：${failed.length ? escapeHtml(failed.join("、")) : "无"}</p>
      </div>
    `;
  }

  return `<pre>${escapeHtml(JSON.stringify(payload, null, 2))}</pre>`;
}

function renderRawVideos(items) {
  const tbody = document.getElementById("rawVideoRows");
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="4">暂无 Scout 原始数据</td></tr>';
    return;
  }

  tbody.innerHTML = items
    .map(
      (item) => `
        <tr>
          <td>${escapeHtml(item.title || item.bvid || "--")}</td>
          <td>${escapeHtml(item.partition || "--")}</td>
          <td>${item.comment_count ?? 0}</td>
          <td>${stageBadge(item.pipeline_stage, item.miner_status, item.candidate_status)}</td>
        </tr>
      `,
    )
    .join("");
}

async function triggerJob(jobName) {
  if (state.triggeringJobs.has(jobName)) {
    return;
  }

  state.triggeringJobs.add(jobName);
  updateTriggerButtons();
  setText("jobActionFeedback", `${formatJobName(jobName)} 请求发送中...`);
  try {
    const response = await fetch(`/api/v1/jobs/${encodeURIComponent(jobName)}/run`, {
      method: "POST",
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || `触发失败: ${response.status}`);
    }
    setText("jobActionFeedback", payload.message || `${formatJobName(jobName)} 已触发`);
    await loadDashboard();
  } catch (error) {
    setText("jobActionFeedback", error.message || String(error));
  } finally {
    state.triggeringJobs.delete(jobName);
    updateTriggerButtons();
  }
}

function updateTriggerButtons() {
  document.getElementById("runScoutButton").disabled = state.triggeringJobs.has("scout");
  document.getElementById("runMinerButton").disabled = state.triggeringJobs.has("miner");
  document.getElementById("runResearchButton").disabled = state.triggeringJobs.has("research");
}

function renderCandidates(candidates) {
  const tbody = document.getElementById("candidateRows");
  if (!candidates.length) {
    tbody.innerHTML = '<tr><td colspan="4">暂无候选数据</td></tr>';
    return;
  }

  tbody.innerHTML = candidates
    .map(
      (item) => `
        <tr>
          <td>${escapeHtml(item.word)}</td>
          <td>${statusBadge(item.status)}</td>
          <td>${Number(item.score || 0).toFixed(2)}</td>
          <td>${formatDateTime(item.detected_at)}</td>
        </tr>
      `,
    )
    .join("");
}

function renderMemes(memes) {
  const tbody = document.getElementById("memeRows");
  if (!memes.length) {
    tbody.innerHTML = '<tr><td colspan="4">暂无梗库数据</td></tr>';
    return;
  }

  tbody.innerHTML = memes
    .map(
      (item) => `
        <tr>
          <td>${escapeHtml(item.title || item.id || "--")}</td>
          <td>${escapeHtml(Array.isArray(item.category) ? item.category.join(" / ") : "--")}</td>
          <td>${item.heat_index ?? "--"}</td>
          <td>${escapeHtml(item.lifecycle_stage || "--")}</td>
        </tr>
      `,
    )
    .join("");
}

function renderFatalError(error) {
  document.getElementById("runDetail").innerHTML = `
    <div class="error-box mono">${escapeHtml(error.message || String(error))}</div>
  `;
}

function statusBadge(status) {
  const normalized = String(status || "pending").toLowerCase();
  const labelMap = {
    running: "运行中",
    success: "成功",
    failed: "失败",
    pending: "待处理",
    processed: "已提取",
    accepted: "已接受",
    rejected: "已拒绝",
  };
  return `<span class="badge ${normalized}">${labelMap[normalized] || escapeHtml(normalized)}</span>`;
}

function stageBadge(stage, minerStatus, candidateStatus) {
  const normalized = String(stage || "").toLowerCase();
  if (normalized === "researched") {
    return `<span class="badge success">Research 已消费</span>`;
  }
  if (normalized === "mined") {
    return `<span class="badge processed">Miner 已完成</span>`;
  }
  if (normalized === "scouted") {
    return `<span class="badge pending">待 Miner</span>`;
  }
  return `${statusBadge(minerStatus)} ${statusBadge(candidateStatus)}`;
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

function formatDuration(value) {
  if (value === null || value === undefined) {
    return "--";
  }
  return `${Number(value).toFixed(1)}s`;
}

function formatJobName(name) {
  if (name === "scout") {
    return "Scout";
  }
  if (name === "miner") {
    return "Miner";
  }
  if (name === "research") {
    return "Researcher";
  }
  return name || "--";
}

function formatTrigger(triggerMode) {
  if (triggerMode === "scheduled") {
    return "定时触发";
  }
  if (triggerMode === "manual") {
    return "手动触发";
  }
  return triggerMode || "--";
}

function setLastUpdated() {
  setText("lastUpdated", `最近刷新：${formatDateTime(new Date().toISOString())}`);
}

function setText(id, value) {
  document.getElementById(id).textContent = String(value);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
