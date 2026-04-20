from meme_detector.researcher.taxonomy import (
    CATEGORIES,
    DEFAULT_CATEGORY,
    DEFAULT_LIFECYCLE_STAGE,
    LIFECYCLE_STAGES,
    normalize_category,
    normalize_lifecycle_stage,
)


def test_normalize_category_handles_aliases_and_delimiters():
    assert normalize_category("谐音梗/鬼畜梗") == ["谐音", "其他"]
    assert normalize_category(["动漫", "音乐梗"]) == ["二次元", "音乐"]
    assert normalize_category("社会 ， 抽象梗；游戏") == [
        "社会现象",
        "抽象",
        "游戏",
    ]


def test_normalize_category_deduplicates_and_preserves_order():
    assert normalize_category(["动画", "动漫", "二次元"]) == ["二次元"]


def test_normalize_category_empty_falls_back_to_default():
    assert normalize_category("") == [DEFAULT_CATEGORY]
    assert normalize_category(None) == [DEFAULT_CATEGORY]
    assert normalize_category("   不存在的分类   ") == [DEFAULT_CATEGORY]


def test_normalize_category_all_canonical_values_are_identity():
    for category in CATEGORIES:
        assert normalize_category(category) == [category]


def test_normalize_lifecycle_stage_handles_chinese_and_english():
    assert normalize_lifecycle_stage("新兴期") == "emerging"
    assert normalize_lifecycle_stage("高峰") == "peak"
    assert normalize_lifecycle_stage("下降期") == "declining"
    assert normalize_lifecycle_stage("PEAK") == "peak"
    assert normalize_lifecycle_stage("emerging") == "emerging"


def test_normalize_lifecycle_stage_unknown_falls_back_to_default():
    assert normalize_lifecycle_stage("") == DEFAULT_LIFECYCLE_STAGE
    assert normalize_lifecycle_stage(None) == DEFAULT_LIFECYCLE_STAGE
    assert normalize_lifecycle_stage("未知状态") == DEFAULT_LIFECYCLE_STAGE


def test_normalize_lifecycle_stage_all_canonical_values_are_identity():
    for stage in LIFECYCLE_STAGES:
        assert normalize_lifecycle_stage(stage) == stage
