import pytest

from meme_detector.researcher.screening import extract_screen_results


def test_extract_screen_results_supports_variant_fields_and_index_mapping():
    raw = """
    {
      "items": [
        {
          "index": 0,
          "meme": true,
          "score": 0.91,
          "category": "游戏",
          "explanation": "符合游戏梗语境"
        },
        {
          "term": "放蚊山",
          "isMeme": "true",
          "confidence": "0.88",
          "candidateCategory": "抽象",
          "why": "抽象表达"
        }
      ]
    }
    """
    candidates = [
        {"word": "两张R卡还想合出SSR"},
        {"word": "放蚊山"},
    ]

    results = extract_screen_results(raw, candidates)

    assert [item.word for item in results] == ["两张R卡还想合出SSR", "放蚊山"]
    assert results[0].is_meme is True
    assert results[0].candidate_category == "游戏"
    assert results[1].confidence == pytest.approx(0.88)


def test_extract_screen_results_supports_word_keyed_mapping():
    raw = """
    {
      "两张R卡还想合出SSR": {
        "is_meme": true,
        "confidence": 0.92,
        "candidate_category": "游戏",
        "reason": "借抽卡术语表达代际期待"
      }
    }
    """

    results = extract_screen_results(raw, [{"word": "两张R卡还想合出SSR"}])

    assert len(results) == 1
    assert results[0].word == "两张R卡还想合出SSR"


def test_extract_screen_results_raises_when_no_valid_items():
    with pytest.raises(RuntimeError, match="未解析到任何有效结果"):
        extract_screen_results('{"results": []}', [{"word": "放蚊山"}])
