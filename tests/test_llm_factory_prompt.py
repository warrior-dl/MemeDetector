from meme_detector.llm_factory import (
    _JSON_PROMPT_ONLY_REMINDER,
    build_prompt_only_json_messages,
)


def test_appends_reminder_to_first_system_message_when_missing():
    messages = [
        {"role": "system", "content": "你是一个助手。"},
        {"role": "user", "content": "hi"},
    ]
    result = build_prompt_only_json_messages(messages)

    assert result[0]["role"] == "system"
    assert result[0]["content"].startswith("你是一个助手。")
    assert _JSON_PROMPT_ONLY_REMINDER in result[0]["content"]
    assert result[1] == {"role": "user", "content": "hi"}
    # 原 dict 不应被修改
    assert messages[0]["content"] == "你是一个助手。"


def test_leaves_first_system_message_alone_when_reminder_already_present():
    original_content = f"你是一个助手。\n\n{_JSON_PROMPT_ONLY_REMINDER}"
    messages = [{"role": "system", "content": original_content}]

    result = build_prompt_only_json_messages(messages)

    assert result[0]["content"] == original_content
    # 不应该出现重复 reminder
    assert result[0]["content"].count(_JSON_PROMPT_ONLY_REMINDER) == 1


def test_prepends_system_message_when_no_system_present():
    messages = [{"role": "user", "content": "hi"}]
    result = build_prompt_only_json_messages(messages)

    assert len(result) == 2
    assert result[0] == {"role": "system", "content": _JSON_PROMPT_ONLY_REMINDER}
    assert result[1] == {"role": "user", "content": "hi"}


def test_only_patches_first_system_message():
    messages = [
        {"role": "system", "content": "first"},
        {"role": "user", "content": "hi"},
        {"role": "system", "content": "second"},
    ]
    result = build_prompt_only_json_messages(messages)

    assert _JSON_PROMPT_ONLY_REMINDER in result[0]["content"]
    assert result[2]["content"] == "second"


def test_empty_first_system_message_is_replaced_with_reminder_only():
    messages = [{"role": "system", "content": ""}]
    result = build_prompt_only_json_messages(messages)
    assert result[0]["content"] == _JSON_PROMPT_ONLY_REMINDER
