from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── B站 Cookie ──
    bilibili_sessdata: str = ""
    bilibili_bili_jct: str = ""
    bilibili_buvid3: str = ""

    # ── LLM ──
    deepseek_api_key: str = ""
    deepseek_base_url: str = "https://api.deepseek.com"
    deepseek_model: str = "deepseek-chat"

    # ── Web 搜索（火山引擎联网搜索） ──
    web_search_api_key: str = ""
    web_search_timeout_seconds: float = 30.0

    # ── BibiGPT ──
    bibigpt_api_token: str = ""
    bibigpt_base_url: str = "https://api.bibigpt.co/api"
    bibigpt_max_duration_seconds: int = 900
    bibigpt_request_timeout_seconds: float = 60.0
    bibigpt_request_retries: int = 2

    # ── Meilisearch ──
    meili_url: str = "http://localhost:7700"
    meili_master_key: str = "dev-master-key"
    meili_index_name: str = "memes"

    # ── 采集参数 ──
    scout_top_n_videos: int = 20
    scout_comments_per_video: int = 20
    scout_score_threshold: float = 5.0
    scout_new_word_min_docs: int = 3
    scout_delay_min: float = 0.8
    scout_delay_max: float = 2.5
    scout_comment_retry_times: int = 2
    scout_comment_backoff_base: float = 3.0
    scout_risk_cooldown_seconds: float = 20.0
    scout_risk_skip_threshold: int = 2
    scout_request_timeout: float = 15.0
    scout_proxy_url: str = ""

    # ── AI 参数 ──
    ai_batch_size: int = 50
    ai_confidence_threshold: float = 0.65
    miner_comment_confidence_threshold: float = 0.6
    miner_comments_batch_size: int = 8
    miner_llm_timeout_seconds: float = 90.0
    miner_llm_max_retries: int = 1

    # ── 路径 ──
    duckdb_path: str = "data/duckdb/freq.db"
    userdict_path: str = "data/dicts/userdict.txt"
    media_asset_root: str = "data/assets"


# 全局单例
settings = Settings()
