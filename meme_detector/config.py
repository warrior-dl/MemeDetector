from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


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

    # ── Web 搜索 ──
    serper_api_key: str = ""

    # ── Meilisearch ──
    meili_url: str = "http://localhost:7700"
    meili_master_key: str = "dev-master-key"
    meili_index_name: str = "memes"

    # ── 采集参数 ──
    scout_top_n_videos: int = 20
    scout_comments_per_video: int = 500
    scout_score_threshold: float = 5.0
    scout_new_word_min_docs: int = 3
    scout_delay_min: float = 0.8
    scout_delay_max: float = 2.5
    scout_proxy_url: str = ""

    # ── AI 参数 ──
    ai_batch_size: int = 50
    ai_confidence_threshold: float = 0.65

    # ── 路径 ──
    duckdb_path: str = "data/duckdb/freq.db"
    userdict_path: str = "data/dicts/userdict.txt"


# 全局单例
settings = Settings()
