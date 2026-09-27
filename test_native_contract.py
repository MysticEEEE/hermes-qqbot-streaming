import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest

import qqbot_streaming_plugin as plugin
from gateway.platforms.base import SendResult
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig


_PROGRESS_GETTER = GatewayStreamConsumer.accepts_tool_progress.fget
_CORE_NATIVE_PROGRESS_OPT_OUT = "SUPPORTS_NATIVE_TOOL_PROGRESS" in getattr(
    getattr(_PROGRESS_GETTER, "__code__", None), "co_consts", ()
)
_CORE_NATIVE_ABANDON = "_send_frame" in GatewayStreamConsumer._abandon_native_stream.__code__.co_names


def make_adapter():
    adapter = object.__new__(plugin.QQStreamingAdapter)
    adapter.config = SimpleNamespace(extra={})
    adapter._running = True
    adapter._ws = SimpleNamespace(closed=False)
    adapter._last_msg_id = {}
    adapter._chat_type_map = {"user-1": "c2c", "same-user": "c2c"}
    adapter._api_request = AsyncMock(return_value={"id": "stream-1"})
    adapter.send = AsyncMock(
        return_value=SendResult(success=True, message_id="ordinary-1")
    )
    return adapter


def make_http_adapter(handler):
    adapter = make_adapter()
    del adapter._api_request
    adapter._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter._auth_headers = AsyncMock(return_value={"Authorization": "QQBot token"})
    return adapter


def test_native_capability_is_c2c_only_and_never_claims_generic_editing():
    adapter = make_adapter()

    assert adapter.SUPPORTS_MESSAGE_EDITING is False
    assert adapter.SUPPORTS_NATIVE_STREAMING is True
    assert adapter.SUPPORTS_NATIVE_TOOL_PROGRESS is False
    assert adapter.supports_native_streaming(chat_type="dm") is True
    assert adapter.supports_native_streaming(chat_type="c2c") is True
    assert adapter.supports_native_streaming(chat_type="group") is False
    assert adapter.supports_native_streaming(chat_type="guild") is False


@pytest.mark.asyncio
async def test_guild_dm_never_uses_c2c_stream_endpoint():
    adapter = make_adapter()
    adapter._chat_type_map["guild-dm-1"] = "dm"
    consumer = GatewayStreamConsumer(
        adapter,
        "guild-dm-1",
        StreamConsumerConfig(
            edit_interval=0,
            buffer_threshold=1,
            transport="draft",
            chat_type="dm",
            cursor="",
        ),
        initial_reply_to_id="guild-msg-1",
    )

    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0)
    assert consumer._resolve_native_streaming() is True
    consumer.on_delta("guild answer")
    await asyncio.sleep(0.08)
    consumer.finish("guild answer complete")
    await asyncio.wait_for(task, timeout=1)

    adapter._api_request.assert_not_awaited()
    adapter.send.assert_awaited_once()


def test_plugin_exposes_native_transport_without_legacy_draft_or_edit_state_machine():
    own_methods = plugin.QQStreamingAdapter.__dict__

    assert "send_draft" not in own_methods
    assert "edit_message" not in own_methods
    assert "supports_draft_streaming" not in own_methods
    assert "_handle_c2c_message" not in own_methods
    assert "_clear_stream_state" not in own_methods


@pytest.mark.asyncio
async def test_native_stream_uses_turn_scoped_reply_identity_before_first_ack():
    adapter = make_adapter()

    assert await adapter.send_stream_frame(
        "", chat_id="same-user", reply_to="msg-old", turn_id="turn-old"
    ) is True
    assert await adapter.send_stream_frame(
        "", chat_id="same-user", reply_to="msg-new", turn_id="turn-new"
    ) is True
    assert await adapter.send_stream_frame(
        "旧回复的第一段足够长", chat_id="same-user", turn_id="turn-old"
    ) is True
    assert await adapter.send_stream_frame(
        "新回复的第一段足够长", chat_id="same-user", turn_id="turn-new"
    ) is True

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert [body["msg_id"] for body in bodies] == ["msg-old", "msg-new"]
    assert "event_id" not in bodies[0]
    assert "event_id" not in bodies[1]
    assert bodies[0]["msg_seq"] != bodies[1]["msg_seq"]
    assert bodies[0]["index"] == bodies[1]["index"] == 0


@pytest.mark.asyncio
async def test_overlapping_first_frames_remain_turn_scoped_while_old_ack_is_pending():
    adapter = make_adapter()
    old_entered = asyncio.Event()
    release_old = asyncio.Event()

    async def request(_method, _path, body):
        if body["msg_id"] == "msg-old":
            old_entered.set()
            await release_old.wait()
        return {"id": f"stream-{body['msg_id']}"}

    adapter._api_request.side_effect = request
    old = asyncio.create_task(
        adapter.send_stream_frame(
            "old answer", chat_id="same-user", reply_to="msg-old", turn_id="turn-old"
        )
    )
    await asyncio.wait_for(old_entered.wait(), timeout=1)
    new = asyncio.create_task(
        adapter.send_stream_frame(
            "new answer", chat_id="same-user", reply_to="msg-new", turn_id="turn-new"
        )
    )
    assert await asyncio.wait_for(new, timeout=1) is True
    release_old.set()
    assert await asyncio.wait_for(old, timeout=1) is True

    states = adapter._native_states()
    assert states["turn-old"].reply_to == "msg-old"
    assert states["turn-old"].stream_msg_id == "stream-msg-old"
    assert states["turn-new"].reply_to == "msg-new"
    assert states["turn-new"].stream_msg_id == "stream-msg-new"


@pytest.mark.asyncio
async def test_progress_overlay_is_never_committed_and_diverged_final_closes_same_stream():
    adapter = make_adapter()

    await adapter.send_stream_frame(
        "", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
    )
    await adapter.send_stream_frame(
        '💻 Running terminal: "date" ▉', chat_id="user-1", turn_id="turn-1"
    )
    assert adapter._api_request.await_count == 0

    await adapter.send_stream_frame(
        '开始回答的第一段\n\n---\n💻 Running terminal: "date" ▉',
        chat_id="user-1",
        turn_id="turn-1",
    )
    await adapter.send_stream_frame(
        "开始回答的第一段以及结尾",
        chat_id="user-1",
        turn_id="turn-1",
        finalize=True,
    )

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert len(bodies) == 2
    assert bodies[0]["content_raw"] == "开始回答的第一段"
    assert bodies[1]["content_raw"] == "开始回答的第一段以及结尾"
    assert bodies[1]["stream_msg_id"] == "stream-1"
    assert bodies[1]["input_state"] == adapter._STREAM_INPUT_STATE_DONE
    assert "turn-1" not in adapter._native_states()


def test_real_markdown_horizontal_rule_is_not_mistaken_for_tool_overlay():
    frames = [
        "上节正文\n\n---\n\n下节正文 ▉",
        "上节正文\n\n---\n下节正文 ▉",
    ]

    assert [
        plugin.QQStreamingAdapter._native_frame_text(frame, finalize=False)
        for frame in frames
    ] == [
        "上节正文\n\n---\n\n下节正文",
        "上节正文\n\n---\n下节正文",
    ]


def test_emoji_leading_answer_is_not_mistaken_for_tool_progress():
    adapter = make_adapter()

    assert adapter._looks_like_tool_progress("✅ 已完成，这是最终答案") is False
    assert adapter._looks_like_tool_progress("© 注意事项") is False
    assert adapter._looks_like_tool_progress('💻 Running terminal: "date"') is True
    assert adapter._looks_like_tool_progress("⚙️ terminal\n```\ndate\n```") is True


@pytest.mark.asyncio
async def test_multiline_verbose_tool_overlay_is_never_opened_as_qq_stream():
    adapter = make_adapter()
    await adapter.send_stream_frame(
        "", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
    )

    assert await adapter.send_stream_frame(
        "⚙️ terminal\n```\ndate\n```", chat_id="user-1", turn_id="turn-1"
    ) is True
    adapter._api_request.assert_not_awaited()


@pytest.mark.asyncio
async def test_true_authoritative_prefix_rewrite_closes_with_acknowledged_text_only():
    adapter = make_adapter()
    assert await adapter.send_stream_frame(
        "开始回答的第一段", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
    ) is True

    assert await adapter.send_stream_frame(
        "回答的第一段以及结尾",
        finalize=True,
        chat_id="user-1",
        turn_id="turn-1",
    ) is False
    assert adapter._api_request.await_count == 1
    assert adapter._native_states()["turn-1"].last_text == "开始回答的第一段"

    assert await adapter.send_stream_frame(
        "回答的第一段以及结尾",
        finalize=True,
        chat_id="user-1",
        turn_id="turn-1",
    ) is False
    close_body = adapter._api_request.await_args_list[-1].args[2]
    assert close_body["content_raw"] == "开始回答的第一段"
    assert close_body["input_state"] == adapter._STREAM_INPUT_STATE_DONE
    assert "turn-1" not in adapter._native_states()


@pytest.mark.xfail(
    not _CORE_NATIVE_PROGRESS_OPT_OUT,
    strict=True,
    reason="Hermes core does not yet honor native adapter tool-progress opt-out",
)
@pytest.mark.asyncio
async def test_real_consumer_keeps_tool_progress_on_ordinary_lane_and_owns_final():
    adapter = make_adapter()
    consumer = GatewayStreamConsumer(
        adapter,
        "user-1",
        StreamConsumerConfig(
            edit_interval=0,
            buffer_threshold=1,
            transport="draft",
            chat_type="dm",
            cursor=" ▉",
        ),
        initial_reply_to_id="msg-1",
    )

    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0)
    assert consumer.accepts_tool_progress is False
    assert adapter._api_request.await_count == 0

    consumer.on_delta("开始回答的第一段")
    await asyncio.sleep(0.08)
    assert adapter._api_request.await_count == 1
    consumer.finish("开始回答的第一段以及结尾")
    await asyncio.wait_for(task, timeout=1)

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert [body["input_state"] for body in bodies] == [1, 10]
    assert bodies[0]["content_raw"] == "开始回答的第一段"
    assert bodies[1]["content_raw"] == "开始回答的第一段以及结尾"
    assert bodies[1]["stream_msg_id"] == "stream-1"
    adapter.send.assert_not_awaited()
    assert consumer.final_response_sent is True
    assert consumer.delivered_final_matches("开始回答的第一段以及结尾") is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        "QQ Bot API error [429] /stream_messages: rate limited",
        "QQ Bot API error [500] /stream_messages: code=50002 频率限制",
        "QQ Bot API error [500] /stream_messages: code=50001 服务内部错误",
        "QQ Bot API error [500] /stream_messages: 频率限制",
        "QQ Bot API error [500] /stream_messages: 服务内部错误",
    ],
)
async def test_retryable_qq_stream_errors_use_bounded_backoff(error):
    adapter = make_adapter()
    adapter._api_request.side_effect = [
        RuntimeError(error),
        RuntimeError(error),
        {"id": "stream-after-retry"},
    ]

    with patch("qqbot_streaming_plugin.asyncio.sleep", new=AsyncMock()) as sleep:
        ok = await adapter.send_stream_frame(
            "answer", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
        )

    assert ok is True
    assert adapter._api_request.await_count == 3
    assert sleep.await_count == 2
    assert [call.args[2]["index"] for call in adapter._api_request.await_args_list] == [0, 0, 0]


@pytest.mark.asyncio
async def test_native_stream_retries_structured_50001_from_real_http_response():
    requests = []

    def handler(request):
        requests.append(request)
        if len(requests) < 3:
            return httpx.Response(
                500,
                json={"code": 50001, "message": "opaque temporary failure"},
            )
        return httpx.Response(200, json={"id": "stream-after-retry"})

    adapter = make_http_adapter(handler)
    try:
        with patch("qqbot_streaming_plugin.asyncio.sleep", new=AsyncMock()) as sleep:
            ok = await adapter.send_stream_frame(
                "answer", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
            )
    finally:
        await adapter._http_client.aclose()

    assert ok is True
    assert sleep.await_count == 2
    assert len(requests) == 3
    assert requests[0].method == "POST"
    assert requests[0].url.path == "/v2/users/user-1/stream_messages"
    assert json.loads(requests[0].content)["index"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "payload"),
    [
        (503, {"code": "50002", "message": "opaque overload"}),
        (429, {"message": "opaque rejection"}),
    ],
)
async def test_native_stream_retries_structured_rate_limits(status, payload):
    requests = []

    def handler(request):
        requests.append(request)
        if len(requests) < 3:
            return httpx.Response(status, json=payload)
        return httpx.Response(200, json={"id": "stream-after-retry"})

    adapter = make_http_adapter(handler)
    try:
        with patch("qqbot_streaming_plugin.asyncio.sleep", new=AsyncMock()) as sleep:
            ok = await adapter.send_stream_frame(
                "answer", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
            )
    finally:
        await adapter._http_client.aclose()

    assert ok is True
    assert sleep.await_count == 2
    assert len(requests) == 3
    assert [json.loads(request.content)["index"] for request in requests] == [0, 0, 0]


@pytest.mark.asyncio
async def test_native_stream_classifies_structured_40007_without_retry():
    requests = []

    def handler(request):
        body = json.loads(request.content)
        requests.append(body)
        if len(requests) == 1:
            return httpx.Response(200, json={"id": "stream-1"})
        if len(requests) == 2:
            return httpx.Response(
                400,
                json={"code": 40007, "message": "opaque invalid update"},
            )
        return httpx.Response(200, json={"id": "stream-closed"})

    adapter = make_http_adapter(handler)
    try:
        assert await adapter.send_stream_frame(
            "answer", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
        ) is True
        assert await adapter.send_stream_frame(
            "answer extended", chat_id="user-1", turn_id="turn-1"
        ) is False
        assert len(requests) == 2
        assert await adapter.send_stream_frame(
            "answer complete", finalize=True, chat_id="user-1", turn_id="turn-1"
        ) is False
    finally:
        await adapter._http_client.aclose()

    assert len(requests) == 3
    assert requests[-1]["content_raw"] == "answer"
    assert requests[-1]["input_state"] == adapter._STREAM_INPUT_STATE_DONE


@pytest.mark.asyncio
async def test_native_stream_timeout_from_real_client_is_ambiguous_and_not_retried():
    requests = []

    def handler(request):
        requests.append(request)
        raise httpx.ReadTimeout("write outcome unknown", request=request)

    adapter = make_http_adapter(handler)
    try:
        assert await adapter.send_stream_frame(
            "answer", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
        ) is False
        assert await adapter.send_stream_frame(
            "answer complete", finalize=True, chat_id="user-1", turn_id="turn-1"
        ) is False
    finally:
        await adapter._http_client.aclose()

    assert len(requests) == 1


@pytest.mark.asyncio
async def test_stream_http_exception_preserves_status_and_qq_code():
    def handler(_request):
        return httpx.Response(
            503,
            json={"code": "50002", "message": "opaque overload"},
        )

    adapter = make_http_adapter(handler)
    try:
        with pytest.raises(plugin._StreamAPIError) as raised:
            await adapter._api_request(
                "POST",
                "/v2/users/user-1/stream_messages",
                {"content_raw": "answer"},
            )
    finally:
        await adapter._http_client.aclose()

    assert raised.value.http_status == 503
    assert raised.value.qq_code == 50002
    policy = adapter._classify_stream_error(raised.value)
    assert policy.http_status == 503
    assert policy.code == 50002
    assert policy.retryable is True


@pytest.mark.asyncio
async def test_consumer_closes_open_stream_with_acked_prefix_before_send_fallback():
    adapter = make_adapter()
    del adapter.send
    operations = []
    stream_calls = 0

    async def request(_method, _path, body):
        nonlocal stream_calls
        stream_calls += 1
        operations.append(("stream", dict(body)))
        if stream_calls == 1:
            return {"id": "stream-1"}
        if stream_calls == 2:
            raise RuntimeError("QQ Bot API error [400] /stream_messages: bad frame")
        return {"id": "stream-closed"}

    async def send(chat_id, content, reply_to=None, metadata=None):
        del chat_id, reply_to, metadata
        operations.append(("send", content))
        return SendResult(success=True, message_id="ordinary-1")

    adapter._api_request.side_effect = request
    consumer = GatewayStreamConsumer(
        adapter,
        "user-1",
        StreamConsumerConfig(edit_interval=0, buffer_threshold=1, chat_type="dm", cursor=""),
        initial_reply_to_id="msg-1",
    )

    with patch.object(plugin.QQAdapter, "send", new=AsyncMock(side_effect=send)) as ordinary_send:
        task = asyncio.create_task(consumer.run())
        await asyncio.sleep(0.01)
        consumer.on_delta("acknowledged prefix")
        await asyncio.sleep(0.08)
        consumer.on_delta(" and final tail")
        await asyncio.sleep(0.08)
        consumer.finish("acknowledged prefix and final tail")
        await asyncio.wait_for(task, timeout=1)

    stream_bodies = [value for kind, value in operations if kind == "stream"]
    assert [body["input_state"] for body in stream_bodies] == [1, 1, 10]
    assert [body["content_raw"] for body in stream_bodies] == [
        "acknowledged prefix",
        "acknowledged prefix and final tail",
        "acknowledged prefix",
    ]
    assert [body["index"] for body in stream_bodies] == [0, 1, 1]
    assert [kind for kind, _value in operations] == ["stream", "stream", "stream", "send"]
    assert [value for kind, value in operations if kind == "send"] == [" and final tail"]
    ordinary_send.assert_awaited_once()
    assert not adapter._native_states()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        "QQ Bot API error [400] /stream_messages: code=40007 已下发内容前缀不可修改",
        "QQ Bot API error [400] /stream_messages: 已下发内容前缀不可修改",
        "QQ Bot API error [404] /stream_messages: stream_msg_id not found",
    ],
)
async def test_invalid_stream_errors_close_acknowledged_prefix_without_retry(error):
    adapter = make_adapter()
    adapter._api_request.side_effect = [
        {"id": "stream-1"},
        RuntimeError(error),
        {"id": "stream-closed"},
    ]
    await adapter.send_stream_frame(
        "answer", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
    )

    assert await adapter.send_stream_frame(
        "answer extended", chat_id="user-1", turn_id="turn-1"
    ) is False
    calls_after_failure = adapter._api_request.await_count
    assert await adapter.send_stream_frame(
        "answer complete", finalize=True, chat_id="user-1", turn_id="turn-1"
    ) is False
    assert calls_after_failure == 2
    assert adapter._api_request.await_count == 3
    close_body = adapter._api_request.await_args_list[-1].args[2]
    assert close_body["content_raw"] == "answer"
    assert close_body["input_state"] == adapter._STREAM_INPUT_STATE_DONE
    assert "turn-1" not in adapter._native_states()


@pytest.mark.asyncio
async def test_opening_timeout_never_claims_delivery_or_suppresses_fallback():
    adapter = make_adapter()
    adapter._api_request.side_effect = TimeoutError("opening write timed out")

    assert await adapter.send_stream_frame(
        "answer", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
    ) is False
    assert await adapter.send_stream_frame(
        "answer complete", finalize=True, chat_id="user-1", turn_id="turn-1"
    ) is False
    assert adapter._api_request.await_count == 1


@pytest.mark.asyncio
async def test_intermediate_timeout_keeps_known_stream_closable():
    adapter = make_adapter()
    adapter._api_request.side_effect = [
        {"id": "stream-1"},
        TimeoutError("update timed out after write"),
        {"id": "stream-final"},
    ]
    await adapter.send_stream_frame(
        "answer", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
    )

    assert await adapter.send_stream_frame(
        "answer extended", chat_id="user-1", turn_id="turn-1"
    ) is False
    assert await adapter.send_stream_frame(
        "answer extended complete",
        finalize=True,
        chat_id="user-1",
        turn_id="turn-1",
    ) is False

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert [body["index"] for body in bodies] == [0, 1, 1]
    assert bodies[-1]["stream_msg_id"] == "stream-1"
    assert bodies[-1]["input_state"] == adapter._STREAM_INPUT_STATE_DONE
    # The timed-out frame may already have reached QQ. Closing with the
    # attempted cumulative body is prefix-safe whether it did or did not.
    assert bodies[-1]["content_raw"] == "answer extended"


@pytest.mark.asyncio
async def test_timeout_is_possibly_delivered_and_never_blindly_retried():
    adapter = make_adapter()
    adapter._api_request.side_effect = [
        {"id": "stream-1"},
        TimeoutError("stream request timed out after write"),
        {"id": "stream-closed"},
    ]
    await adapter.send_stream_frame(
        "answer", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
    )

    assert await adapter.send_stream_frame(
        "answer complete", finalize=True, chat_id="user-1", turn_id="turn-1"
    ) is False
    assert await adapter.send_stream_frame(
        "answer complete", finalize=True, chat_id="user-1", turn_id="turn-1"
    ) is False
    assert adapter._api_request.await_count == 3
    close_body = adapter._api_request.await_args_list[-1].args[2]
    # Repeating the attempted final body avoids shortening a frame that may
    # already have been accepted before the timeout.
    assert close_body["content_raw"] == "answer complete"
    assert close_body["input_state"] == adapter._STREAM_INPUT_STATE_DONE


@pytest.mark.asyncio
async def test_first_ack_without_stream_id_fails_closed():
    adapter = make_adapter()
    adapter._api_request.return_value = {"remain_msg_len": 29994}

    assert await adapter.send_stream_frame(
        "answer", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
    ) is False
    assert await adapter.send_stream_frame(
        "answer complete", finalize=True, chat_id="user-1", turn_id="turn-1"
    ) is False
    assert adapter._api_request.await_count == 1


@pytest.mark.asyncio
async def test_markdown_autoclose_tail_is_held_out_of_immutable_qq_prefix():
    adapter = make_adapter()
    stable = "稳定正文。" * 40
    first = stable + " `inline`"
    second = stable + " `inline code`"

    assert await adapter.send_stream_frame(
        first, chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
    ) is True
    assert await adapter.send_stream_frame(
        second, chat_id="user-1", turn_id="turn-1"
    ) is True
    assert await adapter.send_stream_frame(
        second, finalize=True, chat_id="user-1", turn_id="turn-1"
    ) is True

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert len(bodies) == 2
    assert first.startswith(bodies[0]["content_raw"])
    assert bodies[1]["content_raw"].startswith(bodies[0]["content_raw"])
    assert bodies[1]["content_raw"] == second
    assert [body["index"] for body in bodies] == [0, 1]


@pytest.mark.asyncio
async def test_fenced_markdown_autoclose_is_not_committed_as_immutable_text():
    adapter = make_adapter()
    first = "intro\n```python\nprint(1)\n```"
    second = "intro\n```python\nprint(1)\nprint(2)\n```"

    assert await adapter.send_stream_frame(
        first, chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
    ) is True
    assert await adapter.send_stream_frame(
        second, chat_id="user-1", turn_id="turn-1"
    ) is True
    assert await adapter.send_stream_frame(
        second, finalize=True, chat_id="user-1", turn_id="turn-1"
    ) is True

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert bodies[0]["content_raw"] == "intro\n```python\nprint(1)"
    assert bodies[1]["content_raw"] == "intro\n```python\nprint(1)\nprint(2)"
    assert bodies[2]["content_raw"] == second


@pytest.mark.asyncio
async def test_clarify_boundary_closes_then_reopens_a_fresh_stream_in_same_turn():
    adapter = make_adapter()
    adapter._api_request.side_effect = [
        {"id": "stream-old"},
        {"id": "stream-old-final"},
        {"id": "stream-new"},
        {"id": "stream-new-final"},
    ]
    consumer = GatewayStreamConsumer(
        adapter,
        "user-1",
        StreamConsumerConfig(edit_interval=0, buffer_threshold=1, chat_type="dm"),
        initial_reply_to_id="msg-1",
    )
    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0.01)
    consumer.on_delta("before clarify")
    await asyncio.sleep(0.08)

    boundary, _cancelled = consumer.close_for_approval_prompt(
        "waiting", reason="Clarify", reopen=True
    )
    assert await asyncio.wait_for(boundary, timeout=1) is True
    consumer.request_reopen_seed()
    await asyncio.sleep(0.08)
    consumer.on_delta("after clarify")
    await asyncio.sleep(0.08)
    consumer.finish("after clarify complete")
    await asyncio.wait_for(task, timeout=1)

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert [body["input_state"] for body in bodies] == [1, 10, 1, 10]
    assert [body["index"] for body in bodies] == [0, 1, 0, 1]
    assert "stream_msg_id" not in bodies[0]
    assert "stream_msg_id" not in bodies[2]
    adapter.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_approval_boundary_closes_native_stream_then_uses_one_buffered_send():
    adapter = make_adapter()
    consumer = GatewayStreamConsumer(
        adapter,
        "user-1",
        StreamConsumerConfig(edit_interval=0, buffer_threshold=1, chat_type="dm"),
        initial_reply_to_id="msg-1",
    )
    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0.01)
    consumer.on_delta("before approval")
    await asyncio.sleep(0.08)

    boundary, _cancelled = consumer.close_for_approval_prompt(reason="Approval")
    assert await asyncio.wait_for(boundary, timeout=1) is True
    consumer.on_delta("approved final")
    consumer.finish("approved final")
    await asyncio.wait_for(task, timeout=1)

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert [body["input_state"] for body in bodies] == [1, 10]
    adapter.send.assert_awaited_once()
    assert adapter.send.await_args.kwargs["content"] == "approved final"


@pytest.mark.xfail(
    not _CORE_NATIVE_ABANDON,
    strict=True,
    reason=(
        "Hermes d0288be _on_cancelled calls _abandon_native_stream(), but that "
        "helper is draft-only and never calls send_stream_frame(finalize=True)"
    ),
)
@pytest.mark.asyncio
async def test_core_blocker_cancel_should_finalize_open_native_stream():
    adapter = make_adapter()
    consumer = GatewayStreamConsumer(
        adapter,
        "user-1",
        StreamConsumerConfig(edit_interval=0, buffer_threshold=1, chat_type="dm"),
        initial_reply_to_id="msg-1",
    )
    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0.01)
    consumer.on_delta("partial")
    await asyncio.sleep(0.08)
    task.cancel()
    await asyncio.wait_for(task, timeout=1)

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert bodies[-1]["input_state"] == 10
    assert not adapter._native_states()


@pytest.mark.xfail(
    not _CORE_NATIVE_ABANDON,
    strict=True,
    reason=(
        "Hermes d0288be stale-run abandonment is draft-only and exposes no "
        "native adapter cleanup hook"
    ),
)
@pytest.mark.asyncio
async def test_core_blocker_stale_run_should_finalize_open_native_stream():
    adapter = make_adapter()
    current = [True]
    consumer = GatewayStreamConsumer(
        adapter,
        "user-1",
        StreamConsumerConfig(edit_interval=0, buffer_threshold=1, chat_type="dm"),
        initial_reply_to_id="msg-1",
        run_still_current=lambda: current[0],
    )
    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0.01)
    consumer.on_delta("partial")
    await asyncio.sleep(0.08)
    current[0] = False
    await asyncio.wait_for(task, timeout=1)

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert bodies[-1]["input_state"] == 10
    assert not adapter._native_states()
