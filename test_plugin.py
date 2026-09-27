import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call, patch

import pytest

import qqbot_streaming_plugin as plugin
from gateway.platforms.base import SendResult
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig


def make_adapter(chat_types=None):
    adapter = object.__new__(plugin.QQStreamingAdapter)
    adapter.config = SimpleNamespace(extra={})
    adapter._running = True
    adapter._ws = SimpleNamespace(closed=False)
    adapter._last_msg_id = {}
    adapter._chat_type_map = dict(chat_types or {"user-1": "c2c"})
    adapter._native_stream_states = {}
    adapter._native_finalized_turns = {}
    adapter._api_request = AsyncMock(return_value={"id": "stream-1"})
    return adapter


def test_register_overrides_builtin_qqbot_adapter():
    ctx = SimpleNamespace(register_platform=Mock())

    plugin.register(ctx)

    ctx.register_platform.assert_called_once()
    kwargs = ctx.register_platform.call_args.kwargs
    assert kwargs["name"] == "qqbot"
    assert kwargs["label"] == "QQ Bot (C2C Streaming)"
    assert kwargs["adapter_factory"](SimpleNamespace(extra={})).__class__ is plugin.QQStreamingAdapter


def test_profile_scoped_validator_uses_shared_secret_reader_for_each_profile():
    config = SimpleNamespace(extra={})

    with patch.object(
        plugin,
        "extra_or_secret",
        side_effect=["profile-a-app", "profile-a-secret"],
    ) as scoped:
        assert plugin._validate_qq_config(config) is True
    assert scoped.call_args_list == [
        call({}, "app_id", "QQ_APP_ID", ""),
        call({}, "client_secret", "QQ_CLIENT_SECRET", ""),
    ]

    with patch.object(
        plugin,
        "extra_or_secret",
        side_effect=["profile-b-app", ""],
    ):
        assert plugin._validate_qq_config(config) is False


def test_c2c_has_30000_budget_but_group_guild_and_unknown_keep_4000():
    adapter = make_adapter(
        {"c2c-user": "c2c", "group-1": "group", "guild-1": "guild"}
    )

    assert adapter.max_message_length_for_chat("c2c-user") == 30000
    assert adapter.max_message_length_for_chat("group-1") == 4000
    assert adapter.max_message_length_for_chat("guild-1") == 4000
    assert adapter.max_message_length_for_chat("unknown") == 4000


@pytest.mark.asyncio
async def test_ordinary_send_is_exactly_the_builtin_transport():
    adapter = make_adapter({"group-1": "group"})
    expected = SendResult(success=True, message_id="ordinary-1")

    with patch.object(
        plugin.QQAdapter, "send", new=AsyncMock(return_value=expected)
    ) as parent:
        result = await adapter.send(
            "group-1", "ordinary", reply_to="inbound-1", metadata={"notify": True}
        )

    assert result is expected
    parent.assert_awaited_once_with(
        "group-1", "ordinary", reply_to="inbound-1", metadata={"notify": True}
    )
    adapter._api_request.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("chat_type", ["group", "guild"])
async def test_group_and_guild_consumers_never_treat_ordinary_id_as_stream_id(chat_type):
    adapter = make_adapter({"room-1": chat_type})
    adapter.send = AsyncMock(
        return_value=SendResult(success=True, message_id="ordinary-message-id")
    )
    consumer = GatewayStreamConsumer(
        adapter,
        "room-1",
        StreamConsumerConfig(
            edit_interval=0,
            buffer_threshold=1,
            transport="draft",
            chat_type=chat_type,
            cursor="",
        ),
        initial_reply_to_id="inbound-1",
    )

    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0)
    assert consumer.accepts_tool_progress is False
    consumer.on_tool_progress('💻 Running terminal: "date"')
    consumer.on_delta("complete reply")
    consumer.finish("complete reply")
    await asyncio.wait_for(task, timeout=1)

    adapter.send.assert_awaited_once()
    adapter._api_request.assert_not_awaited()
    assert "edit_message" not in plugin.QQStreamingAdapter.__dict__


@pytest.mark.asyncio
async def test_response_id_rotates_while_index_and_request_identity_stay_stable():
    adapter = make_adapter()
    adapter._next_msg_seq = lambda _msg_id: 77
    adapter._api_request.side_effect = [
        {"id": "stream-1"},
        {"id": "stream-2"},
        {"id": "stream-3"},
    ]

    await adapter.send_stream_frame(
        "one", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
    )
    await adapter.send_stream_frame(
        "one two", chat_id="user-1", turn_id="turn-1"
    )
    await adapter.send_stream_frame(
        "one two three", finalize=True, chat_id="user-1", turn_id="turn-1"
    )

    bodies = [item.args[2] for item in adapter._api_request.await_args_list]
    assert [body["index"] for body in bodies] == [0, 1, 2]
    assert [body["msg_id"] for body in bodies] == ["msg-1"] * 3
    assert [body["msg_seq"] for body in bodies] == [77] * 3
    assert [body.get("stream_msg_id") for body in bodies] == [
        None,
        "stream-1",
        "stream-2",
    ]
    assert all("event_id" not in body for body in bodies)


@pytest.mark.asyncio
async def test_duplicate_finalize_is_idempotent_without_another_api_call():
    adapter = make_adapter()
    await adapter.send_stream_frame(
        "answer", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
    )
    await adapter.send_stream_frame(
        "answer complete", finalize=True, chat_id="user-1", turn_id="turn-1"
    )
    count = adapter._api_request.await_count

    assert await adapter.send_stream_frame(
        "answer complete", finalize=True, chat_id="user-1", turn_id="turn-1"
    ) is True
    assert adapter._api_request.await_count == count == 2


@pytest.mark.asyncio
async def test_stream_rest_call_does_not_depend_on_websocket_connected_state():
    adapter = make_adapter()
    adapter._running = False
    adapter._ws = SimpleNamespace(closed=True)

    assert await adapter.send_stream_frame(
        "answer", chat_id="user-1", reply_to="msg-1", turn_id="turn-1"
    ) is True
    adapter._api_request.assert_awaited_once()


@pytest.mark.asyncio
async def test_opened_stream_overflow_closes_prefix_and_sends_only_tail():
    adapter = make_adapter()
    adapter._C2C_STREAM_MAX_LENGTH = 20
    operations = []

    async def request(_method, _path, body):
        operations.append(("stream", dict(body)))
        return {"id": "stream-1"}

    async def send(chat_id, content, reply_to=None, metadata=None):
        del chat_id, reply_to, metadata
        operations.append(("send", content))
        return SendResult(success=True, message_id="ordinary-tail")

    adapter._api_request.side_effect = request
    consumer = GatewayStreamConsumer(
        adapter,
        "user-1",
        StreamConsumerConfig(edit_interval=0, buffer_threshold=1, chat_type="dm", cursor=""),
        initial_reply_to_id="msg-1",
    )

    with patch.object(plugin.QQAdapter, "send", new=AsyncMock(side_effect=send)):
        task = asyncio.create_task(consumer.run())
        await asyncio.sleep(0.01)
        prefix = "123456789012345"
        final = prefix + "ABCDEFGHIJ"
        consumer.on_delta(prefix)
        await asyncio.sleep(0.08)
        consumer.finish(final)
        await asyncio.wait_for(task, timeout=1)

    stream_bodies = [value for kind, value in operations if kind == "stream"]
    assert [body["input_state"] for body in stream_bodies] == [1, 10]
    assert [body["content_raw"] for body in stream_bodies] == [prefix, prefix]
    assert [value for kind, value in operations if kind == "send"] == ["ABCDEFGHIJ"]
    assert not adapter._native_states()


@pytest.mark.asyncio
async def test_local_30000_budget_rejects_overflow_before_qq_api():
    adapter = make_adapter()

    assert await adapter.send_stream_frame(
        "x" * 30001,
        chat_id="user-1",
        reply_to="msg-1",
        turn_id="turn-1",
    ) is False
    adapter._api_request.assert_not_awaited()
