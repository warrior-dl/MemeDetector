export interface StatsResponse {
  bundles: {
    total: number;
    bundled: number;
    researched: number;
    ready: number;
  };
  blockers: {
    pending_miner_videos: number;
    failed_miner_videos: number;
  };
  memes_in_library: number;
}

export interface ResearchBundleSummary {
  bundle_id: string;
  insight_id: string;
  bvid: string;
  collected_date?: string;
  comment_text?: string;
  worth_investigating?: boolean;
  signal_score?: number;
  reason?: string;
  status?: string;
  video_refs?: VideoRef[];
  recommended_hypothesis_id?: string;
  miner_summary_reason?: string;
  hypothesis_count?: number;
  queued_hypothesis_count?: number;
  accepted_hypothesis_count?: number;
  evidence_count?: number;
  latest_decision?: string;
}

export interface ResearchBundlesPageResponse {
  total: number;
  limit: number;
  offset: number;
  items: ResearchBundleSummary[];
}

export interface ResearchBundlesPageParams {
  status?: string;
  queuedOnly?: boolean;
  keyword?: string;
  limit?: number;
  offset?: number;
}

export interface BundleDecisionSummary {
  decision_id: string;
  decision: string;
  final_title?: string;
  target_record_id?: string;
  confidence?: number;
  reason?: string;
  created_at?: string;
}

export interface BundleInsight {
  insight_id: string;
  bvid: string;
  collected_date?: string;
  comment_text?: string;
  worth_investigating?: boolean;
  signal_score?: number;
  reason?: string;
  status?: string;
}

export interface BundleSpan {
  span_id: string;
  insight_id: string;
  raw_text: string;
  normalized_text: string;
  span_type: string;
  char_start?: number | null;
  char_end?: number | null;
  confidence?: number;
  is_primary?: boolean;
  query_priority?: string;
  reason?: string;
}

export interface BundleHypothesis {
  hypothesis_id: string;
  insight_id: string;
  candidate_title: string;
  hypothesis_type: string;
  miner_opinion?: string;
  support_score?: number;
  counter_score?: number;
  uncertainty_score?: number;
  suggested_action?: string;
  status?: string;
}

export interface BundleHypothesisSpanLink {
  hypothesis_id: string;
  span_id: string;
  role: string;
}

export interface BundleEvidence {
  evidence_id: string;
  hypothesis_id: string;
  span_id?: string | null;
  query: string;
  query_mode: string;
  source_kind: string;
  source_title?: string;
  source_url?: string;
  snippet?: string;
  evidence_direction: string;
  evidence_strength?: number;
}

export interface BundleMinerSummary {
  recommended_hypothesis_id?: string | null;
  should_queue_for_research?: boolean;
  reason?: string;
}

export interface BundleDetail {
  bundle_id: string;
  insight: BundleInsight;
  video_refs: VideoRef[];
  spans: BundleSpan[];
  hypotheses: BundleHypothesis[];
  hypothesis_spans: BundleHypothesisSpanLink[];
  evidences: BundleEvidence[];
  miner_summary: BundleMinerSummary;
}

export interface BundleDetailResponse {
  bundle: BundleDetail;
  decisions: BundleDecisionSummary[];
}

export interface VideoRef {
  bvid: string;
  title: string;
  partition?: string;
  url?: string;
  collected_date?: string;
  matched_comment_count?: number;
  matched_comments?: string[];
}

export interface AgentConversationSummary {
  id: string;
  run_id: string;
  agent_name: string;
  word: string;
  status: string;
  summary?: string;
  started_at?: string;
  finished_at?: string;
  message_count?: number;
  error_message?: string;
}

export interface AgentConversationListResponse {
  items: AgentConversationSummary[];
  total: number;
  limit: number;
  offset: number;
}

export interface AgentConversationDetail extends AgentConversationSummary {
  messages: unknown[];
  output: unknown;
}

export interface AgentConversationListParams {
  runId?: string;
  agentName?: string;
  word?: string;
  status?: string;
  limit?: number;
  offset?: number;
}

export interface MemeItem {
  id: string;
  title?: string;
  alias?: string[];
  category?: string[];
  definition?: string;
  origin?: string;
  platform?: string;
  heat_index?: number;
  lifecycle_stage?: string;
  first_detected_at?: string;
  source_urls?: string[];
  confidence_score?: number;
  updated_at?: string;
  human_verified?: boolean;
  source_word?: string;
  meili_doc_id?: string;
}

export interface MemeSearchResponse {
  hits: MemeItem[];
  estimatedTotalHits: number;
  limit: number;
  offset: number;
}

export interface MemeSearchParams {
  query?: string;
  verifiedOnly?: boolean;
  category?: string;
  lifecycle?: string;
  limit?: number;
  offset?: number;
}

export interface RunItem {
  id: string;
  job_name: string;
  trigger_mode?: string;
  status: string;
  duration_seconds?: number;
  result_count?: number;
  summary?: string;
  error_message?: string;
  started_at?: string;
  finished_at?: string;
  payload?: unknown;
}

export interface RunListParams {
  jobName?: string;
  status?: string;
  limit?: number;
}

export interface JobItem {
  id: string;
  name: string;
  job_name?: string;
  next_run_time?: string | null;
  trigger: string;
  is_running?: boolean;
  active_trigger_mode?: string;
  active_started_at?: string | null;
  last_finished_at?: string | null;
  last_error?: string;
  active_phase?: string;
  active_progress_current?: number;
  active_progress_total?: number;
  active_progress_unit?: string;
  active_progress_message?: string;
  active_updated_at?: string | null;
}

export interface MinerCommentInsightItem {
  insight_id: string;
  bvid: string;
  collected_date?: string;
  partition?: string;
  title?: string;
  description?: string;
  video_url?: string;
  url?: string;
  tags?: string[];
  comment_text?: string;
  confidence?: number;
  is_meme_candidate?: boolean;
  is_insider_knowledge?: boolean;
  reason?: string;
  video_context?: Record<string, unknown>;
  status?: string;
  bundle_id?: string;
  bundle_status?: string;
  created_at?: string;
  updated_at?: string;
}

export interface MinerCommentInsightsPageParams {
  status?: string;
  keyword?: string;
  bvid?: string;
  onlyMemeCandidates?: boolean;
  onlyInsiderKnowledge?: boolean;
  limit?: number;
  offset?: number;
  enabled?: boolean;
}

export interface MinerCommentInsightsPageResponse {
  items: MinerCommentInsightItem[];
  total: number;
  limit: number;
  offset: number;
}

export interface ScoutRawVideoSummary {
  bvid: string;
  collected_date: string;
  partition?: string;
  title?: string;
  video_url?: string;
  tags?: string[];
  comment_count?: number;
  miner_status?: string;
  miner_started_at?: string | null;
  miner_processed_at?: string | null;
  miner_failed_at?: string | null;
  miner_last_error?: string;
  miner_attempt_count?: number;
  research_status?: string;
  research_started_at?: string | null;
  created_at?: string;
  updated_at?: string;
  first_comment?: string;
  picture_count?: number;
  high_value_comment_count?: number;
  bundle_count?: number;
  pipeline_stage?: string;
}

export interface ScoutRawVideosPageParams {
  researchStatus?: string;
  partition?: string;
  keyword?: string;
  limit?: number;
  offset?: number;
}

export interface ScoutRawVideosPageResponse {
  items: ScoutRawVideoSummary[];
  total: number;
  limit: number;
  offset: number;
}

export interface ScoutMediaAsset {
  asset_id: string;
  source_url?: string;
  storage_path?: string;
  width?: number | null;
  height?: number | null;
  byte_size?: number | null;
  download_status?: string;
  mime_type?: string;
  file_ext?: string;
  image_index?: number;
}

export interface ScoutCommentSnapshot {
  rpid: number;
  root_rpid?: number | null;
  parent_rpid?: number | null;
  mid?: number | null;
  uname?: string;
  message?: string;
  like_count?: number;
  reply_count?: number;
  ctime?: string | null;
  picture_count?: number;
  has_pictures?: boolean;
  content?: unknown;
  raw_reply?: unknown;
  created_at?: string;
  updated_at?: string;
  pictures?: ScoutMediaAsset[];
}

export interface ScoutRawVideoDetail extends ScoutRawVideoSummary {
  description?: string;
  comments?: string[];
  comment_snapshots?: ScoutCommentSnapshot[];
  comments_with_pictures?: number;
}

export interface ScoutRawVideoStageUpdateResponse extends ScoutRawVideoDetail {
  requested_stage: string;
  affected_insight_count: number;
}

export interface TriggerJobResponse {
  job_name: string;
  started: boolean;
  message?: string;
  runtime_state?: {
    running?: boolean;
    trigger_mode?: string;
    started_at?: string | null;
    last_started_at?: string | null;
    last_finished_at?: string | null;
    last_error?: string;
  };
}
