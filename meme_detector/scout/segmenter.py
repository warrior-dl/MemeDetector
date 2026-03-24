"""
分词与词频统计模块。
使用 Jieba 对评论/弹幕文本进行分词，输出词频统计。
"""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import jieba
import jieba.analyse

from meme_detector.config import settings

# 过滤规则
_MIN_WORD_LEN = 2           # 最短词长
_MAX_WORD_LEN = 10          # 最长词长（过滤超长无意义字符串）
_PURE_NUMBER = re.compile(r"^\d+$")
_PURE_PUNCT = re.compile(r"^[^\w\u4e00-\u9fff]+$")

# 扩展停用词（常见无意义词）
_STOPWORDS = {
    "一个", "这个", "那个", "什么", "没有", "可以", "就是", "真的", "感觉",
    "一下", "一样", "还是", "但是", "因为", "所以", "然后", "而且", "虽然",
    "哈哈", "哈哈哈", "哈哈哈哈", "呵呵", "嗯嗯", "好的", "谢谢",
    "确实", "感觉", "一下", "真的", "一样", "还是",
    "up主", "视频", "评论", "弹幕", "点赞", "投币", "收藏",
    "the", "and", "for", "that", "this", "with",
}


def _load_userdict() -> None:
    """加载自定义词典（梗词库），允许文件不存在。"""
    path = Path(settings.userdict_path)
    if path.exists():
        jieba.load_userdict(str(path))


def _is_valid_word(word: str) -> bool:
    if len(word) < _MIN_WORD_LEN or len(word) > _MAX_WORD_LEN:
        return False
    if _PURE_NUMBER.match(word):
        return False
    if _PURE_PUNCT.match(word):
        return False
    if word in _STOPWORDS:
        return False
    return True


# 模块加载时初始化一次
_load_userdict()
jieba.setLogLevel("WARNING")


def tokenize(text: str) -> list[str]:
    """对单条文本分词，返回有效词列表。"""
    words = jieba.cut(text, cut_all=False)
    return [w for w in words if _is_valid_word(w)]


def compute_word_freq(
    texts: list[str],
) -> list[dict]:
    """
    对一批文本进行分词并统计词频。

    返回:
        [{"word": str, "freq": int, "doc_count": int}, ...]
        - freq: 词在所有文本中出现的总次数
        - doc_count: 词出现在多少条文本中
    """
    total_freq: Counter[str] = Counter()
    doc_freq: Counter[str] = Counter()

    for text in texts:
        words = tokenize(text)
        total_freq.update(words)
        # doc_count：每条文本中去重统计
        doc_freq.update(set(words))

    return [
        {"word": word, "freq": total_freq[word], "doc_count": doc_freq[word]}
        for word in total_freq
    ]


def extract_sample_comments(
    word: str,
    texts: list[str],
    max_samples: int = 5,
) -> str:
    """
    提取包含指定词的样本评论（供 AI 分析上下文用）。
    返回拼接的字符串，最多 max_samples 条。
    """
    samples = []
    for text in texts:
        if word in text and len(text) > len(word) + 2:
            samples.append(text.strip())
            if len(samples) >= max_samples:
                break
    return "\n".join(f"- {s}" for s in samples)
