import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

import qqbot_streaming_plugin as plugin
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig


def make_adapter():
    adapter = object.__new__(plugin.QQStreamingAdapter)
    adapter._running = True
    adapter._ws = SimpleNamespace(closed=False)
    adapter._active_streams = {}
    adapter._stream_event_ids = {}
    adapter._stream_msg_ids = {}
    adapter._stream_msg_seqs = {}
    adapter._stream_indices = {}
    adapter._stream_sent_text = {}
    adapter._finalized_streams = {}
    adapter._stream_requests = {}
    adapter._stream_progress = {}
    adapter._stream_aliases = {}
    adapter._api_request = AsyncMock(return_value={"id": "stream-1"})
    return adapter


def test_qq_stream_messages_does_not_use_temporary_native_draft_contract():
    adapter = make_adapter()
    assert adapter.supports_draft_streaming(chat_type="dm") is False
    assert adapter.supports_draft_streaming(chat_type="c2c") is False
    assert adapter.supports_draft_streaming(chat_type="group") is False


def test_register_overrides_builtin_qqbot_adapter():
    ctx = SimpleNamespace(register_platform=Mock())
    plugin.register(ctx)
    ctx.register_platform.assert_called_once()
    kwargs = ctx.register_platform.call_args.kwargs
    assert kwargs["name"] == "qqbot"
    assert kwargs["label"] == "QQ Bot (C2C Streaming)"


def test_only_active_c2c_stream_uses_limit_above_5200_chars():
    """C2C stream_messages must not raise normal group/guild send limits."""
    adapter = make_adapter()
    adapter._stream_msg_ids["user-1"] = "msg-1"
    assert adapter.max_message_length_for_chat("user-1") >= 6000
    assert adapter.max_message_length_for_chat("group-1") == 4000


@pytest.mark.asyncio
async def test_c2c_intake_stores_per_turn_stream_state_before_parent_handler():
    adapter = make_adapter()
    adapter._active_streams["user-1"] = "stale-stream"
    adapter._stream_sent_text["user-1"] = "stale"
    adapter._stream_indices["user-1"] = 9
    adapter._next_msg_seq = lambda _msg_id: 42

    with patch.object(plugin.QQAdapter, "_handle_c2c_message", new=AsyncMock()) as parent:
        await adapter._handle_c2c_message(
            {"event_id": "event-1"},
            "msg-1",
            "hello",
            {"user_openid": "user-1"},
            "2026-08-13T00:00:00+08:00",
        )

    assert adapter._stream_event_ids["user-1"] == "event-1"
    assert adapter._stream_msg_ids["user-1"] == "msg-1"
    assert adapter._stream_msg_seqs["user-1"] == 42
    assert adapter._stream_indices["user-1"] == 0
    assert "user-1" not in adapter._active_streams
    assert "user-1" not in adapter._stream_sent_text
    parent.assert_awaited_once()


@pytest.mark.asyncio
async def test_editable_first_send_creates_persistent_qq_stream_message():
    adapter = make_adapter()
    adapter._stream_event_ids["user-1"] = "event-1"
    adapter._stream_msg_ids["user-1"] = "msg-1"
    adapter._stream_msg_seqs["user-1"] = 7

    result = await adapter.send(
        "user-1", "Hello ▉", metadata={"expect_edits": True}
    )

    assert result.success is True
    _, path, body = adapter._api_request.await_args.args
    assert path == "/v2/users/user-1/stream_messages"
    assert body == {
        "msg_id": "msg-1",
        "msg_seq": 7,
        "index": 0,
        "content_raw": "H",
        "content_type": "markdown",
        "input_mode": "replace",
        "input_state": 1,
    }
    assert adapter._active_streams["user-1"] == "stream-1"
    assert adapter._stream_sent_text["user-1"] == "H"


@pytest.mark.asyncio
async def test_non_streaming_send_keeps_builtin_qq_delivery_path():
    adapter = make_adapter()
    expected = plugin.SendResult(success=True, message_id="normal-1")

    with patch.object(
        plugin.QQAdapter, "send", new=AsyncMock(return_value=expected)
    ) as parent:
        result = await adapter.send(
            "group-1", "final text", metadata={"notify": True}
        )

    assert result is expected
    parent.assert_awaited_once_with(
        "group-1", "final text", reply_to=None, metadata={"notify": True}
    )


@pytest.mark.asyncio
async def test_send_draft_update_preserves_acknowledged_prefix_and_stream_id():
    adapter = make_adapter()
    adapter._stream_event_ids["user-1"] = "event-1"
    adapter._stream_msg_ids["user-1"] = "msg-1"
    adapter._stream_msg_seqs["user-1"] = 7
    adapter._active_streams["user-1"] = "stream-1"
    adapter._stream_sent_text["user-1"] = "H"
    adapter._stream_indices["user-1"] = 1

    cumulative = "Hello world " + ("x" * 140)
    result = await adapter.send_draft("user-1", 123, cumulative + " ▉")

    assert result.success is True
    body = adapter._api_request.await_args.args[2]
    assert cumulative.startswith(body["content_raw"])
    assert body["content_raw"].startswith("H")
    assert body["stream_msg_id"] == "stream-1"
    assert body["index"] == 1


@pytest.mark.asyncio
async def test_send_draft_recovers_rewrite_without_triggering_static_fallback():
    adapter = make_adapter()
    adapter._stream_event_ids["user-1"] = "event-1"
    adapter._stream_msg_ids["user-1"] = "msg-1"
    adapter._stream_msg_seqs["user-1"] = 7
    adapter._active_streams["user-1"] = "stream-1"
    adapter._stream_sent_text["user-1"] = "Hello"

    result = await adapter.send_draft("user-1", 123, "Hallo world")

    assert result.success is True
    # A divergent mutable frame must be held locally, never synthesized into
    # a body that differs from both the acknowledged prefix and model text.
    adapter._api_request.assert_not_awaited()
    assert adapter._stream_sent_text["user-1"] == "Hello"


@pytest.mark.asyncio
async def test_short_markdown_rewrite_never_corrupts_final_text():
    """A <=128-char Markdown tail rewrite must not be silently spliced."""
    adapter = make_adapter()
    adapter._stream_event_ids["user-1"] = "event-1"
    adapter._stream_msg_ids["user-1"] = "msg-1"
    adapter._stream_msg_seqs["user-1"] = 7

    opened = await adapter.send(
        "user-1", "Hello `x`", metadata={"expect_edits": True}
    )
    assert opened.success is True
    first = adapter._api_request.await_args.args[2]["content_raw"]
    assert first == "H"

    finished = await adapter.edit_message(
        "user-1", "stream-1", "Hello `xy`", finalize=True
    )
    assert finished.success is True
    final = adapter._api_request.await_args.args[2]["content_raw"]
    assert final == "Hello `xy`"


@pytest.mark.asyncio
async def test_finalize_marks_done_and_cleans_all_turn_state():
    adapter = make_adapter()
    adapter._stream_event_ids["user-1"] = "event-1"
    adapter._stream_msg_ids["user-1"] = "msg-1"
    adapter._stream_msg_seqs["user-1"] = 7
    adapter._active_streams["user-1"] = "stream-1"
    adapter._stream_sent_text["user-1"] = "Hello"
    adapter._stream_indices["user-1"] = 1

    result = await adapter.edit_message(
        "user-1", "stream-1", "Hello world", finalize=True
    )

    assert result.success is True
    body = adapter._api_request.await_args.args[2]
    assert body["input_state"] == 10
    assert body["stream_msg_id"] == "stream-1"
    assert "user-1" not in adapter._active_streams
    assert adapter._finalized_streams["user-1"] == "stream-1"
    assert "user-1" not in adapter._stream_msg_ids
    assert adapter.max_message_length_for_chat("user-1") == 4000

    # Consumer's redundant second finalize is acknowledged without a second
    # QQ API request, so the same bubble closes exactly once.
    adapter._api_request.reset_mock()
    duplicate = await adapter.edit_message(
        "user-1", "stream-1", "Hello world", finalize=True
    )
    assert duplicate.success is True
    adapter._api_request.assert_not_awaited()


@pytest.mark.asyncio
async def test_each_edit_adopts_latest_response_stream_id():
    """The next fragment must use the id returned by the previous fragment."""
    adapter = make_adapter()
    adapter._stream_msg_ids["user-1"] = "msg-1"
    adapter._stream_msg_seqs["user-1"] = 7
    adapter._active_streams["user-1"] = "stream-1"
    adapter._stream_requests["stream-1"] = ("msg-1", 7)
    adapter._stream_sent_text["user-1"] = "H"
    adapter._stream_indices["user-1"] = 1
    adapter._api_request.side_effect = [
        {"id": "stream-2"},
        {"id": "stream-3"},
    ]
    long_text = "Hello " + ("x" * 140)

    first = await adapter.edit_message(
        "user-1", "stream-1", long_text, finalize=False
    )
    assert first.message_id == "stream-2"
    assert adapter._active_streams["user-1"] == "stream-2"

    second = await adapter.edit_message(
        "user-1", first.message_id, long_text + " done", finalize=True
    )
    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert bodies[0]["stream_msg_id"] == "stream-1"
    assert bodies[1]["stream_msg_id"] == "stream-2"
    assert second.message_id == "stream-3"


@pytest.mark.asyncio
async def test_consumer_old_id_is_resolved_to_latest_response_id():
    adapter = make_adapter()
    adapter._stream_msg_ids["user-1"] = "msg-1"
    adapter._stream_msg_seqs["user-1"] = 7
    adapter._api_request.side_effect = [
        {"id": "stream-1"},
        {"id": "stream-2"},
        {"id": "stream-3"},
    ]
    consumer = GatewayStreamConsumer(
        adapter,
        "user-1",
        StreamConsumerConfig(transport="draft", chat_type="dm", cursor=" ▉"),
    )
    frame1 = "A" * 140
    frame2 = "A" * 160
    final = "A" * 180

    assert await consumer._send_or_edit(frame1 + " ▉") is True
    assert consumer.message_id == "stream-1"
    assert await consumer._send_or_edit(frame2 + " ▉") is True
    # GatewayStreamConsumer retains its original id after a normal edit.
    assert consumer.message_id == "stream-1"
    assert await consumer._send_or_edit(final, finalize=True) is True

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert bodies[1]["stream_msg_id"] == "stream-1"
    assert bodies[2]["stream_msg_id"] == "stream-2"
    calls_after_final = adapter._api_request.await_count
    duplicate = await adapter.edit_message(
        "user-1", "stream-1", final, finalize=True
    )
    assert duplicate.success is True
    assert duplicate.message_id == "stream-3"
    assert adapter._api_request.await_count == calls_after_final


def test_stream_alias_cache_is_bounded_after_insert():
    adapter = make_adapter()
    adapter._stream_aliases = {
        f"old-{index}": f"latest-{index}" for index in range(4096)
    }

    adapter._remember_stream_alias("old-new", "latest-new")

    assert len(adapter._stream_aliases) == 4096
    assert adapter._stream_aliases["old-new"] == "latest-new"
    assert "old-0" not in adapter._stream_aliases


@pytest.mark.asyncio
async def test_old_stream_keeps_request_snapshot_after_new_c2c_intake():
    """A newer inbound message must not retarget an older stream's fragments."""
    adapter = make_adapter()
    adapter._stream_msg_ids["user-1"] = "old-msg"
    adapter._stream_msg_seqs["user-1"] = 3
    adapter._active_streams["user-1"] = "old-stream"
    adapter._stream_requests["old-stream"] = ("old-msg", 3)
    adapter._stream_sent_text["user-1"] = "H"
    adapter._stream_indices["user-1"] = 1
    adapter._next_msg_seq = lambda _msg_id: 8

    with patch.object(plugin.QQAdapter, "_handle_c2c_message", new=AsyncMock()):
        await adapter._handle_c2c_message(
            {"event_id": "new-event"},
            "new-msg",
            "new question",
            {"user_openid": "user-1"},
            "2026-08-13T00:00:00+08:00",
        )

    result = await adapter.edit_message(
        "user-1", "old-stream", "Hello " + ("x" * 140), finalize=False
    )
    assert result.success is True
    body = adapter._api_request.await_args.args[2]
    assert body["msg_id"] == "old-msg"
    assert body["msg_seq"] == 3


@pytest.mark.asyncio
async def test_old_stream_holdback_uses_its_own_acknowledged_prefix():
    adapter = make_adapter()
    adapter._stream_msg_ids["user-1"] = "new-msg"
    adapter._stream_msg_seqs["user-1"] = 8
    adapter._active_streams["user-1"] = "new-stream"
    adapter._stream_requests["old-stream"] = ("old-msg", 3)
    adapter._stream_progress["old-stream"] = ("Old", 2)
    adapter._stream_sent_text["user-1"] = "New"
    adapter._stream_indices["user-1"] = 4
    old_frame = "Old continuation " + ("x" * 140)

    result = await adapter.edit_message(
        "user-1", "old-stream", old_frame, finalize=False
    )

    assert result.success is True
    body = adapter._api_request.await_args.args[2]
    assert body["content_raw"].startswith("Old")
    assert not body["content_raw"].startswith("New")


@pytest.mark.asyncio
async def test_old_stream_finalize_does_not_clear_new_stream_progress():
    adapter = make_adapter()
    adapter._stream_msg_ids["user-1"] = "new-msg"
    adapter._stream_msg_seqs["user-1"] = 8
    adapter._active_streams["user-1"] = "new-stream"
    adapter._stream_requests.update(
        {
            "old-stream": ("old-msg", 3),
            "new-stream": ("new-msg", 8),
        }
    )
    adapter._stream_progress.update(
        {
            "old-stream": ("Old", 2),
            "new-stream": ("New", 4),
        }
    )
    adapter._stream_sent_text["user-1"] = "New"
    adapter._stream_indices["user-1"] = 4
    adapter._api_request.return_value = {"id": "old-stream"}

    result = await adapter.edit_message(
        "user-1", "old-stream", "Old complete", finalize=True
    )

    assert result.success is True
    assert adapter._active_streams["user-1"] == "new-stream"
    assert adapter._stream_sent_text["user-1"] == "New"
    assert adapter._stream_indices["user-1"] == 4
    assert adapter._stream_msg_ids["user-1"] == "new-msg"
    assert adapter._stream_progress["new-stream"] == ("New", 4)


@pytest.mark.asyncio
async def test_gateway_consumer_uses_one_persistent_bubble_and_finalizes_it():
    adapter = make_adapter()
    adapter._stream_event_ids["user-1"] = "event-1"
    adapter._stream_msg_ids["user-1"] = "msg-1"
    adapter._stream_msg_seqs["user-1"] = 7
    adapter._api_request.side_effect = [
        {"id": "stream-1"},
        {"id": "stream-1"},
        {"id": "stream-1"},
    ]
    consumer = GatewayStreamConsumer(
        adapter,
        "user-1",
        StreamConsumerConfig(
            transport="draft", chat_type="dm", cursor=" ▉"
        ),
    )

    assert consumer._resolve_draft_streaming() is False
    assert await consumer._send_or_edit("Hello ▉") is True
    assert consumer.message_id == "stream-1"
    assert await consumer._send_or_edit("Hello world ▉") is True
    assert await consumer._send_or_edit("Hello world", finalize=True) is True

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert [body["input_state"] for body in bodies] == [1, 10]
    assert [body["index"] for body in bodies] == [0, 1]
    assert "stream_msg_id" not in bodies[0]
    assert bodies[1]["stream_msg_id"] == "stream-1"


@pytest.mark.asyncio
async def test_markdown_auto_close_rewrite_stays_on_one_qq_stream():
    """Hermes temporarily closes unfinished inline Markdown on each frame.

    The QQ-acknowledged prefix must not include that unstable tail, otherwise
    the next frame (``...`inline` `` -> ``...`inline code` ``) diverges and
    GatewayStreamConsumer falls back to a second complete bubble.
    """
    adapter = make_adapter()
    adapter._stream_event_ids["user-1"] = "event-1"
    adapter._stream_msg_ids["user-1"] = "msg-1"
    adapter._stream_msg_seqs["user-1"] = 7

    stable = "# 富文本测试\n\n" + ("稳定正文。" * 40)
    first = stable + "- `inline`"  # synthetic closing ` from Hermes
    second = stable + "- `inline code`"

    opened = await adapter.send(
        "user-1", first, metadata={"expect_edits": True}
    )
    assert opened.success is True
    updated = await adapter.edit_message(
        "user-1", "stream-1", second, finalize=False
    )
    assert updated.success is True
    finished = await adapter.edit_message(
        "user-1", "stream-1", second, finalize=True
    )
    assert finished.success is True

    bodies = [call.args[2] for call in adapter._api_request.await_args_list]
    assert len(bodies) == 3
    assert second.startswith(bodies[0]["content_raw"])
    assert second.startswith(bodies[1]["content_raw"])
    assert bodies[2]["content_raw"] == second
    assert [body["input_state"] for body in bodies] == [1, 1, 10]


@pytest.mark.asyncio
async def test_tail_rewrite_recovery_never_inflates_cumulative_reply():
    """Regression for the 1950-char reply that inflated to 24827 chars."""
    adapter = make_adapter()
    adapter._stream_event_ids["user-1"] = "event-1"
    adapter._stream_msg_ids["user-1"] = "msg-1"
    adapter._stream_msg_seqs["user-1"] = 7

    # Reproduce the observed shape: the first 122 chars stay common while
    # cumulative model frames grow.  No outgoing QQ frame may exceed its input.
    frames = [
        ("A" * 122) + (chr(65 + i) * extra)
        for i, extra in enumerate((6, 41, 113, 190, 314, 464, 620, 811))
    ]
    opened = await adapter.send(
        "user-1", frames[0], metadata={"expect_edits": True}
    )
    assert opened.success is True
    for frame in frames[1:]:
        result = await adapter.edit_message(
            "user-1", "stream-1", frame, finalize=False
        )
        assert result.success is True
        sent = adapter._api_request.await_args.args[2]["content_raw"]
        assert len(sent) <= len(frame)

    final = ("A" * 122) + ("Z" * 1830)
    calls_before_final = adapter._api_request.await_count
    result = await adapter.edit_message(
        "user-1", "stream-1", final, finalize=True
    )
    assert result.success is False
    assert "no longer extends" in result.error
    assert result.raw_response["partial_overflow"] is True
    assert result.raw_response["delivered_prefix"] == adapter._stream_sent_text["user-1"]
    assert adapter._api_request.await_count == calls_before_final


@pytest.mark.asyncio
async def test_consumer_tracks_exact_qq_prefix_when_final_edit_fails():
    """Hermes fallback must retain exact QQ delivery state without data loss."""
    adapter = make_adapter()
    adapter._stream_event_ids["user-1"] = "event-1"
    adapter._stream_msg_ids["user-1"] = "msg-1"
    adapter._stream_msg_seqs["user-1"] = 7
    consumer = GatewayStreamConsumer(
        adapter,
        "user-1",
        StreamConsumerConfig(transport="draft", chat_type="dm", cursor=" ▉"),
    )

    assert await consumer._send_or_edit("Hello `x` ▉") is True
    assert adapter._stream_sent_text["user-1"] == "H"

    ok = await consumer._send_or_edit(
        "Jallo final answer", finalize=True, is_turn_final=True
    )
    assert ok is False
    assert consumer._fallback_final_send is True
    assert consumer._fallback_prefix == "H"
    # The final answer no longer starts with QQ's acknowledged H, so Hermes
    # cannot send a tail safely and must fall back to the complete final body.
    assert consumer._fallback_preserve_partial_messages is False


@pytest.mark.asyncio
async def test_post_limit_tail_uses_builtin_send_without_repeating_head():
    """After a sealed stream, overflow tail uses normal QQ delivery."""
    adapter = make_adapter()
    adapter._stream_msg_ids["user-1"] = "msg-1"
    adapter._stream_msg_seqs["user-1"] = 7
    adapter._active_streams["user-1"] = "stream-head"
    adapter._stream_sent_text["user-1"] = "H" * 3879
    adapter._stream_indices["user-1"] = 64

    sealed = await adapter.edit_message(
        "user-1", "stream-head", "H" * 3879, finalize=True
    )
    assert sealed.success is True

    tail = "T" * 254
    expected = plugin.SendResult(success=True, message_id="normal-tail")
    with patch.object(
        plugin.QQAdapter, "send", new=AsyncMock(return_value=expected)
    ) as parent:
        delivered = await adapter.send(
            "user-1", tail, metadata={"expect_edits": True}
        )

    assert delivered is expected
    parent.assert_awaited_once_with(
        "user-1", tail, reply_to=None, metadata={"expect_edits": True}
    )
