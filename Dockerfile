FROM python:3.12-slim

WORKDIR /app

# 安装系统依赖（curl_cffi 需要）
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 安装 uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# 安装 Python 依赖
COPY pyproject.toml .
RUN uv pip install --system -e ".[dev]"

# 复制源码
COPY meme_detector/ ./meme_detector/
COPY data/dicts/ ./data/dicts/

CMD ["python", "-m", "meme_detector", "serve"]
