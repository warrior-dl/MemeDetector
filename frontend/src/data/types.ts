export interface StatsResponse {
  candidates: {
    pending: number;
    accepted: number;
    rejected: number;
  };
  memes_in_library: number;
}

export interface CandidateItem {
  word: string;
  score: number;
  status: string;
  is_new_word?: boolean;
  explanation?: string;
  sample_comments?: string;
  detected_at?: string;
  video_refs?: VideoRef[];
}

export interface CandidatesPageResponse {
  total: number;
  limit: number;
  offset: number;
  items: CandidateItem[];
}

export interface CandidatesPageParams {
  status?: string;
  keyword?: string;
  limit?: number;
  offset?: number;
}

export interface VideoRef {
  bvid: string;
  title: string;
  partition?: string;
  url?: string;
  matched_comment_count?: number;
  matched_comments?: string[];
}

export interface SourceInsight {
  insight_id: string;
  bvid: string;
  title?: string;
  comment_text?: string;
  confidence?: number;
  is_meme_candidate?: boolean;
  is_insider_knowledge?: boolean;
  matched_by_candidate_word?: boolean;
  matched_by_video_ref_comments?: boolean;
  reason?: string;
}

export interface CandidateSourcesResponse {
  candidate: CandidateItem;
  video_refs: VideoRef[];
  source_insights: SourceInsight[];
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
}

export interface CandidateVerifyResponse {
  word: string;
  status: string;
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
  miner_processed_at?: string | null;
  candidate_status?: string;
  candidate_extracted_at?: string | null;
  created_at?: string;
  updated_at?: string;
  first_comment?: string;
  picture_count?: number;
  pipeline_stage?: string;
}

export interface ScoutRawVideosPageParams {
  candidateStatus?: string;
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
