"""QQ Bot C2C native streaming plugin for Hermes Agent.

This user plugin overrides the built-in ``qqbot`` platform registration while
subclassing its official QQ Bot API v2 adapter.  Only C2C/private conversations
use ``/v2/users/{openid}/stream_messages``; groups, guilds, media, keyboards,
STT, ACLs, and all other QQ behavior remain implemented by the built-in
adapter.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from gateway.platforms.base import SendResult
from gateway.platforms.qqbot.adapter import QQAdapter, check_qq_requirements

logger = logging.getLogger(__name__)


class QQStreamingAdapter(QQAdapter):
    """Built-in QQ adapter enhanced with native C2C stream_messages output."""

    # The gateway only enters the draft transport for eligible DM/C2C chats.
    # The built-in adapter remains responsible for normal sends elsewhere.
    SUPPORTS_MESSAGE_EDITING = True
    # QQ stream_messages is a persistent message, not a disposable preview.
    # The consumer must issue an explicit finalize=True edit so QQ receives
    # input_state=10 and removes its loading animation.
    REQUIRES_EDIT_FINALIZE = True

    _STREAM_INPUT_STATE_GENERATING = 1
    _STREAM_INPUT_STATE_DONE = 10
    _STREAM_INPUT_MODE_REPLACE = "replace"
    _STREAM_CONTENT_TYPE_MARKDOWN = "markdown"
    # QQ's C2C stream_messages accepts substantially more than the built-in
    # adapter's 4000-char normal-send cap.  Keep enough room for long replies
    # to stay on one persistent stream; ordinary group/guild sends retain the
    # inherited 4000 limit via max_message_length_for_chat below.
    _C2C_STREAM_MAX_LENGTH = 30000
    # Hermes balances unfinished Markdown before every intermediate update.
    # Keep that mutable tail out of QQ's immutable SentContent prefix.
    _STREAM_TAIL_HOLDBACK = 128

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._active_streams: Dict[str, str] = {}
        self._stream_event_ids: Dict[str, str] = {}
        self._stream_msg_ids: Dict[str, str] = {}
        self._stream_msg_seqs: Dict[str, int] = {}
        self._stream_indices: Dict[str, int] = {}
        self._stream_sent_text: Dict[str, str] = {}
        self._finalized_streams: Dict[str, str] = {}

    def supports_draft_streaming(
        self,
        chat_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Never use Hermes' disposable native-draft contract.

        QQ ``stream_messages`` creates a persistent chat message.  It must use
        Hermes' normal send→edit→finalize transport or the consumer will send
        a second ordinary final bubble after the stream.
        """
        return False

    def max_message_length_for_chat(self, chat_id: str) -> int:
        """Use the native stream capacity only for an active C2C request."""
        if chat_id in self._stream_msg_ids:
            return self._C2C_STREAM_MAX_LENGTH
        return super().max_message_length_for_chat(chat_id)

    @staticmethod
    def _stable_stream_content(content: str) -> str:
        """Remove Hermes' visual cursor, which cannot be retracted on QQ."""
        if content.endswith(" ▉"):
            return content[:-2]
        if content.endswith("▉"):
            return content[:-1]
        return content

    def _frame_content(
        self, chat_id: str, content: str, *, finalize: bool
    ) -> str:
        """Build an append-only QQ frame while buffering Hermes' mutable tail."""
        content = self._stable_stream_content(content)
        previous = self._stream_sent_text.get(chat_id, "")
        if finalize:
            candidate = content
        else:
            # Keep the mutable tail out of QQ's immutable SentContent even for
            # short replies.  A short answer may therefore reveal mostly on
            # the terminal frame, but can never be silently corrupted by a
            # temporary Markdown closer inserted by Hermes.
            visible_len = max(1, len(content) - self._STREAM_TAIL_HOLDBACK)
            candidate = content[:visible_len]

        if not previous or candidate.startswith(previous):
            return candidate

        # Holdback activation can temporarily make the candidate shorter than
        # a prefix already accepted while the reply grows from <=128 chars to
        # >128 chars.  Keep the accepted frame unchanged until the stable
        # candidate catches up; never append an older prefix again.
        if len(candidate) <= len(previous):
            return previous

        # QQ cannot retract an acknowledged prefix.  Never synthesize a suffix
        # from a divergent cumulative frame: doing so can silently corrupt the
        # answer.  Hold the last accepted body until a later frame catches up;
        # a divergent terminal frame is rejected by edit_message below.
        common_len = 0
        limit = min(len(previous), len(candidate))
        while common_len < limit and previous[common_len] == candidate[common_len]:
            common_len += 1
        logger.warning(
            "[%s] QQ stream divergent frame held: previous_len=%d "
            "incoming_len=%d common_len=%d finalize=%s",
            self._log_tag,
            len(previous),
            len(candidate),
            common_len,
            finalize,
        )
        return candidate if finalize else previous

    def _clear_stream_state(self, chat_id: str, *, keep_request: bool) -> None:
        """Clear response state; optionally retain current inbound request data."""
        self._active_streams.pop(chat_id, None)
        self._stream_sent_text.pop(chat_id, None)
        self._stream_indices.pop(chat_id, None)
        if not keep_request:
            self._stream_event_ids.pop(chat_id, None)
            self._stream_msg_ids.pop(chat_id, None)
            self._stream_msg_seqs.pop(chat_id, None)
            self._finalized_streams.pop(chat_id, None)

    @staticmethod
    def _partial_failure(
        error: str, *, message_id: str, delivered_prefix: str
    ) -> SendResult:
        """Tell Hermes exactly what QQ retained so fallback sends only the tail."""
        return SendResult(
            success=False,
            message_id=message_id,
            error=error,
            raw_response={
                "partial_overflow": True,
                "last_message_id": message_id,
                "delivered_prefix": delivered_prefix,
            },
        )

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Create a persistent C2C stream for editable preview sends.

        ``expect_edits`` is set by GatewayStreamConsumer only for a first
        progressive frame.  Normal final sends, groups, guilds, cron delivery,
        and all non-streaming traffic stay on the built-in QQ adapter.
        """
        if (
            metadata
            and metadata.get("expect_edits") is True
            and chat_id in self._stream_msg_ids
        ):
            return await self.send_draft(chat_id, 0, content, metadata)
        return await super().send(
            chat_id, content, reply_to=reply_to, metadata=metadata
        )

    async def send_draft(
        self,
        chat_id: str,
        draft_id: int,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Create/update one QQ C2C stream using full-text replace frames."""
        if not self.is_connected:
            return SendResult(success=False, error="Not connected", retryable=True)

        msg_id = self._stream_msg_ids.get(chat_id)
        if not msg_id:
            return SendResult(success=False, error="No msg_id for streaming")

        content = self._frame_content(chat_id, content, finalize=False)
        stream_msg_id = self._active_streams.get(chat_id)
        previous = self._stream_sent_text.get(chat_id, "")

        # A mutable Markdown-only change can leave the stable prefix unchanged.
        # Acknowledge it locally instead of burning an index/QPS on a duplicate.
        if stream_msg_id and content == previous:
            return SendResult(success=True, message_id=stream_msg_id)

        # QQ replace mode requires every new full body to preserve the exact
        # prefix already handed to the client.  Failing locally is safer than
        # provoking error 40007 and then treating a partial bubble as complete.
        if previous and not content.startswith(previous):
            logger.warning(
                "[%s] QQ stream frame diverged: previous_len=%d current_len=%d",
                self._log_tag,
                len(previous),
                len(content),
            )
            return SendResult(
                success=False,
                error="QQ streaming frame no longer extends acknowledged prefix",
            )

        current_index = self._stream_indices.get(chat_id, 0)
        body: Dict[str, Any] = {
            "msg_id": msg_id,
            "msg_seq": self._stream_msg_seqs.get(chat_id, 0),
            "index": current_index,
            "content_raw": content,
            "content_type": self._STREAM_CONTENT_TYPE_MARKDOWN,
            "input_mode": self._STREAM_INPUT_MODE_REPLACE,
            "input_state": self._STREAM_INPUT_STATE_GENERATING,
        }
        if stream_msg_id:
            body["stream_msg_id"] = stream_msg_id

        try:
            data = await self._api_request(
                "POST", f"/v2/users/{chat_id}/stream_messages", body
            )
        except Exception as exc:
            logger.warning(
                "[%s] Streaming draft failed, falling back to regular send: %s",
                self._log_tag,
                exc,
            )
            return SendResult(success=False, error=str(exc))

        returned_id = data.get("id") if isinstance(data, dict) else None
        if not returned_id:
            return SendResult(
                success=False, error="QQ stream response missing id", raw_response=data
            )

        self._active_streams[chat_id] = str(returned_id)
        self._stream_sent_text[chat_id] = content
        self._stream_indices[chat_id] = current_index + 1
        logger.info(
            "[%s] QQ stream frame accepted: index=%d state=1 prior_len=%d "
            "current_len=%d response_id=%s remain_msg_len=%s",
            self._log_tag,
            current_index,
            len(previous),
            len(content),
            returned_id,
            data.get("remain_msg_len") if isinstance(data, dict) else None,
        )
        return SendResult(
            success=True, message_id=str(returned_id), raw_response=data
        )

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Update/finalize an active C2C stream through stream_messages."""
        if not self.is_connected:
            return SendResult(success=False, error="Not connected", retryable=True)

        # GatewayStreamConsumer currently makes a second identical finalize
        # call for REQUIRES_EDIT_FINALIZE adapters.  The first one already sent
        # input_state=10 to QQ; acknowledge the redundant call locally instead
        # of posting another terminal fragment.
        if finalize and self._finalized_streams.get(chat_id) == message_id:
            return SendResult(success=True, message_id=message_id)

        msg_id = self._stream_msg_ids.get(chat_id)
        if not msg_id:
            return SendResult(success=False, error="Missing QQ streaming request state")

        content = self._frame_content(chat_id, content, finalize=finalize)
        previous = self._stream_sent_text.get(chat_id, "")

        if previous and not content.startswith(previous):
            logger.error(
                "[%s] QQ stream final frame diverged: previous_len=%d current_len=%d",
                self._log_tag,
                len(previous),
                len(content),
            )
            return self._partial_failure(
                "QQ final frame no longer extends acknowledged prefix",
                message_id=message_id,
                delivered_prefix=previous,
            )

        if not finalize and content == previous:
            return SendResult(success=True, message_id=message_id)

        current_index = self._stream_indices.get(chat_id, 0)
        body = {
            "msg_id": msg_id,
            "msg_seq": self._stream_msg_seqs.get(chat_id, 0),
            "index": current_index,
            "content_raw": content,
            "content_type": self._STREAM_CONTENT_TYPE_MARKDOWN,
            "input_mode": self._STREAM_INPUT_MODE_REPLACE,
            "input_state": (
                self._STREAM_INPUT_STATE_DONE
                if finalize
                else self._STREAM_INPUT_STATE_GENERATING
            ),
            "stream_msg_id": message_id,
        }
        try:
            data = await self._api_request(
                "POST", f"/v2/users/{chat_id}/stream_messages", body
            )
        except Exception as exc:
            logger.error("[%s] edit_message failed: %s", self._log_tag, exc)
            return self._partial_failure(
                str(exc), message_id=message_id, delivered_prefix=previous
            )

        response_id = str(data.get("id") or message_id) if isinstance(data, dict) else message_id
        logger.info(
            "[%s] QQ stream frame accepted: index=%d state=%d prior_len=%d "
            "current_len=%d request_stream_id=%s response_id=%s "
            "remain_msg_len=%s",
            self._log_tag,
            current_index,
            self._STREAM_INPUT_STATE_DONE
            if finalize
            else self._STREAM_INPUT_STATE_GENERATING,
            len(previous),
            len(content),
            message_id,
            response_id,
            data.get("remain_msg_len") if isinstance(data, dict) else None,
        )
        if response_id != message_id:
            logger.warning(
                "[%s] QQ stream response changed id: requested=%s returned=%s "
                "index=%d",
                self._log_tag,
                message_id,
                response_id,
                current_index,
            )
        self._stream_indices[chat_id] = current_index + 1
        self._stream_sent_text[chat_id] = content
        if finalize:
            self._finalized_streams[chat_id] = message_id
            self._active_streams.pop(chat_id, None)
            # Hermes may continue with an overflow tail after sealing a message
            # near the platform limit.  That tail is a new bubble/new stream,
            # so it must start at index 0 with no inherited full-text prefix.
            self._stream_sent_text.pop(chat_id, None)
            self._stream_indices.pop(chat_id, None)
        return SendResult(
            success=True, message_id=message_id, raw_response=data
        )

    def store_stream_event_id(self, chat_id: str, event_id: str) -> None:
        self._stream_event_ids[chat_id] = event_id

    async def _handle_c2c_message(
        self,
        d: Dict[str, Any],
        msg_id: str,
        content: str,
        author: Dict[str, Any],
        timestamp: str,
    ) -> None:
        """Capture request identifiers before delegating normal C2C intake."""
        chat_id = str(author.get("user_openid", ""))
        if chat_id:
            self._clear_stream_state(chat_id, keep_request=False)
            self._stream_event_ids[chat_id] = str(d.get("event_id") or msg_id)
            self._stream_msg_ids[chat_id] = msg_id
            self._stream_msg_seqs[chat_id] = self._next_msg_seq(msg_id)
            self._stream_indices[chat_id] = 0
        await super()._handle_c2c_message(
            d, msg_id, content, author, timestamp
        )


def _validate_qq_config(config: Any) -> bool:
    extra = getattr(config, "extra", {}) or {}
    app_id = str(extra.get("app_id") or os.getenv("QQ_APP_ID", "")).strip()
    secret = str(
        extra.get("client_secret") or os.getenv("QQ_CLIENT_SECRET", "")
    ).strip()
    return bool(app_id and secret)


def register(ctx: Any) -> None:
    """Register this adapter under ``qqbot``, overriding the built-in path."""
    ctx.register_platform(
        name="qqbot",
        label="QQ Bot (C2C Streaming)",
        adapter_factory=lambda cfg: QQStreamingAdapter(cfg),
        check_fn=check_qq_requirements,
        validate_config=_validate_qq_config,
        required_env=["QQ_APP_ID", "QQ_CLIENT_SECRET"],
        install_hint="pip install aiohttp httpx",
        allowed_users_env="QQ_ALLOWED_USERS",
        allow_all_env="QQ_ALLOW_ALL_USERS",
        cron_deliver_env_var="QQBOT_HOME_CHANNEL",
        max_message_length=getattr(QQAdapter, "MAX_MESSAGE_LENGTH", 0),
        emoji="🐧",
        platform_hint=(
            "You are communicating through QQ Bot. Native progressive output "
            "is available only in C2C private chats; group and guild replies "
            "use the built-in QQ delivery behavior."
        ),
    )


__all__ = ["QQStreamingAdapter", "register"]
