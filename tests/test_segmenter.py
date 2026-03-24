"""
词频统计与候选词发现算法测试。
"""

import pytest
from datetime import date, timedelta

from meme_detector.scout.segmenter import tokenize, compute_word_freq, extract_sample_comments


class TestTokenize:
    def test_basic_segmentation(self):
        words = tokenize("依托答辩，这个视频太绷不住了")
        assert "依托答辩" in words
        assert "绷不住" in words

    def test_filter_stopwords(self):
        words = tokenize("哈哈哈哈真的确实感觉一下")
        assert "哈哈哈哈" not in words
        assert "真的" not in words
        assert "确实" not in words

    def test_filter_single_chars(self):
        words = tokenize("我 你 他 好 的 了")
        assert all(len(w) >= 2 for w in words)

    def test_filter_pure_numbers(self):
        words = tokenize("2024年第123期")
        assert "123" not in words
        assert "2024" not in words

    def test_custom_dict_word(self):
        """自定义词典中的词应被正确识别（不被切分）。"""
        words = tokenize("这波依托答辩真的太逆天了")
        assert "依托答辩" in words


class TestComputeWordFreq:
    def test_freq_count(self):
        texts = ["依托答辩", "依托答辩好笑", "这个依托答辩"]
        records = compute_word_freq(texts)
        freq_map = {r["word"]: r for r in records}
        assert "依托答辩" in freq_map
        assert freq_map["依托答辩"]["freq"] == 3
        assert freq_map["依托答辩"]["doc_count"] == 3

    def test_doc_count_dedup(self):
        """同一文本中出现多次，doc_count 只算 1。"""
        texts = ["笑死笑死笑死", "笑死"]
        records = compute_word_freq(texts)
        freq_map = {r["word"]: r for r in records}
        assert freq_map["笑死"]["freq"] == 4
        assert freq_map["笑死"]["doc_count"] == 2

    def test_empty_input(self):
        assert compute_word_freq([]) == []


class TestExtractSampleComments:
    def test_extracts_relevant_comments(self):
        texts = [
            "这个视频真好看",
            "依托答辩，笑死了",
            "怎么这么依托答辩",
            "无关评论",
        ]
        result = extract_sample_comments("依托答辩", texts)
        assert "依托答辩" in result
        assert result.count("- ") == 2

    def test_max_samples(self):
        texts = [f"依托答辩第{i}条" for i in range(10)]
        result = extract_sample_comments("依托答辩", texts, max_samples=3)
        assert result.count("- ") == 3
