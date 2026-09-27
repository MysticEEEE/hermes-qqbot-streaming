"""QQ Bot C2C native streaming plugin for Hermes Agent.

The plugin overrides the built-in ``qqbot`` registration while leaving every
ordinary send, group/guild route, media operation, ACL, and interaction handler
on Hermes' built-in adapter.  Only the native ``send_stream_frame`` contract is
implemented here.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import httpx

from gateway.platforms.base import SendResult
from gateway.platforms._shared import extra_or_secret
from gateway.platforms.qqbot.adapter import QQAdapter, check_qq_requirements
from gateway.platforms.qqbot.constants import API_BASE, DEFAULT_API_TIMEOUT

logger = logging.getLogger(__name__)


@dataclass
class _NativeStreamState:
    """QQ request identity and progress owned by one Hermes turn."""

    chat_id: str
    reply_to: str
    msg_seq: int
    stream_msg_id: str = ""
    index: int = 0
    last_text: str = ""
    close_after_failure: str = ""
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)


@dataclass(frozen=True)
class _StreamErrorPolicy:
    kind: str
    code: Optional[int]
    http_status: Optional[int] = None
    retryable: bool = False
    ambiguous: bool = False


class _StreamAPIError(RuntimeError):
    """A native-stream failure retaining QQ's transport and business codes."""

    def __init__(
        self,
        path: str,
        message: str,
        *,
        http_status: Optional[int] = None,
        qq_code: Optional[int] = None,
        timed_out: bool = False,
    ) -> None:
        self.path = path
        self.http_status = http_status
        self.qq_code = qq_code
        self.timed_out = timed_out
        super().__init__(message)


class QQStreamingAdapter(QQAdapter):
    """Built-in QQ adapter plus persistent native streaming for C2C turns."""

    SUPPORTS_MESSAGE_EDITING = False
    SUPPORTS_NATIVE_STREAMING = True
    # Tool progress must stay on Hermes' ordinary progress lane.  QQ's C2C
    # endpoint creates persistent immutable content, not an ephemeral overlay.
    SUPPORTS_NATIVE_TOOL_PROGRESS = False

    _STREAM_INPUT_STATE_GENERATING = 1
    _STREAM_INPUT_STATE_DONE = 10
    _STREAM_INPUT_MODE_REPLACE = "replace"
    _STREAM_CONTENT_TYPE_MARKDOWN = "markdown"
    _C2C_STREAM_MAX_LENGTH = 30000
    _FINALIZED_TURN_CACHE_SIZE = 4096
    _STREAM_MAX_ATTEMPTS = 3
    _STREAM_RETRY_BASE_SECONDS = 0.25

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._native_stream_states: Dict[str, _NativeStreamState] = {}
        self._native_finalized_turns: Dict[str, str] = {}
        self._native_terminal_turns: Dict[str, str] = {}
        self._native_fallback_prefixes: Dict[tuple[str, str], str] = {}

    def _native_states(self) -> Dict[str, _NativeStreamState]:
        """Lazy storage also keeps object.__new__ test doubles useful."""
        states = getattr(self, "_native_stream_states", None)
        if states is None:
            states = {}
            self._native_stream_states = states
        return states

    def _finalized_turns(self) -> Dict[str, str]:
        finalized = getattr(self, "_native_finalized_turns", None)
        if finalized is None:
            finalized = {}
            self._native_finalized_turns = finalized
        return finalized

    def _terminal_turns(self) -> Dict[str, str]:
        terminal = getattr(self, "_native_terminal_turns", None)
        if terminal is None:
            terminal = {}
            self._native_terminal_turns = terminal
        return terminal

    def _fallback_prefixes(self) -> Dict[tuple[str, str], str]:
        prefixes = getattr(self, "_native_fallback_prefixes", None)
        if prefixes is None:
            prefixes = {}
            self._native_fallback_prefixes = prefixes
        return prefixes

    def _remember_fallback_prefix(self, state: _NativeStreamState) -> None:
        prefixes = self._fallback_prefixes()
        prefixes[(state.chat_id, state.reply_to)] = state.last_text
        while len(prefixes) > self._FINALIZED_TURN_CACHE_SIZE:
            prefixes.pop(next(iter(prefixes)))

    def supports_native_streaming(
        self,
        chat_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Hermes names QQ C2C sources ``dm``; group/guild sources stay false."""
        del metadata
        return str(chat_type or "").strip().lower() in {"dm", "c2c"}

    def max_message_length_for_chat(self, chat_id: str) -> int:
        """Raise only known QQ C2C chats above the ordinary 4000-char limit."""
        if getattr(self, "_chat_type_map", {}).get(chat_id) == "c2c":
            return self._C2C_STREAM_MAX_LENGTH
        return super().max_message_length_for_chat(chat_id)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Deliver only the unsent tail after an opened native stream is closed."""
        inbound_id = str(
            reply_to or (metadata or {}).get("reply_to_message_id") or ""
        )
        prefix = self._fallback_prefixes().pop((str(chat_id), inbound_id), "")
        if prefix and content.startswith(prefix):
            content = content[len(prefix):]
            if not content:
                return SendResult(
                    success=True,
                    raw_response={"already_delivered_by_native_stream": True},
                )
        return await super().send(
            chat_id, content, reply_to=reply_to, metadata=metadata
        )

    @staticmethod
    def _stable_stream_content(content: str) -> str:
        """Remove Hermes' visual cursor, which QQ cannot retract."""
        if content.endswith(" ▉"):
            return content[:-2]
        if content.endswith("▉"):
            return content[:-1]
        return content

    @staticmethod
    def _looks_like_tool_progress(text: str) -> bool:
        """Recognize the native consumer's one-line tool overlay."""
        stripped = text.strip()
        if not stripped:
            return False
        first_line = stripped.splitlines()[0]
        return " Running " in f" {first_line} " or first_line.startswith(
            ("⚙️ terminal", "🔧 terminal", "🔧 execute_code")
        )

    @classmethod
    def _native_frame_text(cls, text: str, *, finalize: bool) -> str:
        """Strip cursor and an ephemeral tool-progress suffix from a frame."""
        text = cls._stable_stream_content(text or "")
        if not finalize:
            separator = "\n\n---\n"
            head, found, tail = text.rpartition(separator)
            # Gateway tool progress follows the separator immediately.  A real
            # Markdown horizontal rule has the required blank line after it.
            if found and head and cls._looks_like_tool_progress(tail):
                text = head
        # GatewayStreamConsumer temporarily closes incomplete Markdown before
        # each push.  QQ cannot retract that synthetic closer, so keep the last
        # backtick span local until the terminal frame supplies authoritative text.
        if not finalize and text.endswith("\n```"):
            text = text[:-4]
        elif not finalize and text.endswith("`"):
            opening = text.rfind("`", 0, len(text) - 1)
            if opening >= 0:
                text = text[:opening].rstrip()
        return text

    @staticmethod
    def _reconcile_prefix(previous: str, incoming: str) -> Optional[str]:
        """Accept only byte-stable prefix extension; QQ cannot rewrite history."""
        return incoming if not previous or incoming.startswith(previous) else None

    def _remember_finalized_turn(self, turn_id: str, stream_msg_id: str) -> None:
        finalized = self._finalized_turns()
        finalized[turn_id] = stream_msg_id
        while len(finalized) > self._FINALIZED_TURN_CACHE_SIZE:
            finalized.pop(next(iter(finalized)))

    def _remember_terminal_turn(self, turn_id: str, kind: str) -> None:
        terminal = self._terminal_turns()
        terminal[turn_id] = kind
        while len(terminal) > self._FINALIZED_TURN_CACHE_SIZE:
            terminal.pop(next(iter(terminal)))

    async def _api_request(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        timeout: float = DEFAULT_API_TIMEOUT,
    ) -> Dict[str, Any]:
        """Preserve structured errors only for this plugin's stream endpoint."""
        if path.endswith("/stream_messages"):
            client = self._require_http_client()
            headers = await self._auth_headers()
            try:
                response = await client.request(
                    method,
                    f"{API_BASE}{path}",
                    headers=headers,
                    json=body,
                    timeout=timeout,
                )
                data = response.json()
            except httpx.TimeoutException as exc:
                raise _StreamAPIError(
                    path,
                    f"QQ Bot API timeout [{path}]: {exc}",
                    timed_out=True,
                ) from exc

            raw_code = data.get("code") if isinstance(data, dict) else None
            try:
                qq_code = int(raw_code) if raw_code is not None else None
            except (TypeError, ValueError):
                qq_code = None
            if response.status_code >= 400 or qq_code not in {None, 0}:
                detail = data.get("message", data) if isinstance(data, dict) else data
                raise _StreamAPIError(
                    path,
                    f"QQ Bot API error [{response.status_code}] {path}: {detail}",
                    http_status=response.status_code,
                    qq_code=qq_code,
                )
            return data
        return await super()._api_request(method, path, body, timeout)

    @staticmethod
    def _classify_stream_error(exc: Exception) -> _StreamErrorPolicy:
        """Map QQ's documented stream failures to an explicit retry policy."""
        if isinstance(exc, _StreamAPIError):
            if exc.timed_out:
                return _StreamErrorPolicy(
                    "timeout", exc.qq_code, exc.http_status, ambiguous=True
                )
            if exc.qq_code == 40007:
                return _StreamErrorPolicy(
                    "invalid_stream", exc.qq_code, exc.http_status
                )
            if exc.qq_code in {50001, 50002} or exc.http_status == 429:
                return _StreamErrorPolicy(
                    "transient",
                    exc.qq_code,
                    exc.http_status,
                    retryable=True,
                )
            return _StreamErrorPolicy("permanent", exc.qq_code, exc.http_status)
        message = str(exc)
        lower = message.lower()
        code_match = re.search(r"(?:code\s*[=:]\s*|\b)(40007|50001|50002)\b", lower)
        code = int(code_match.group(1)) if code_match else None
        if (
            code == 40007
            or "已下发内容前缀不可修改" in message
            or "stream_msg_id not found" in lower
            or "stream id not found" in lower
        ):
            return _StreamErrorPolicy("invalid_stream", code or 40007)
        if (
            code in {50001, 50002}
            or "[429]" in lower
            or "rate limit" in lower
            or "频率限制" in message
            or "服务内部错误" in message
        ):
            return _StreamErrorPolicy("transient", code or (429 if "[429]" in lower else None), retryable=True)
        if "timeout" in lower or "timed out" in lower or "timeout" in type(exc).__name__.lower():
            return _StreamErrorPolicy("timeout", code, ambiguous=True)
        return _StreamErrorPolicy("permanent", code)

    async def _request_stream(
        self, path: str, body: Dict[str, Any]
    ) -> tuple[Optional[Dict[str, Any]], Optional[_StreamErrorPolicy]]:
        """POST one index with bounded retries; never retry ambiguous timeouts."""
        for attempt in range(self._STREAM_MAX_ATTEMPTS):
            try:
                return await self._api_request("POST", path, body), None
            except Exception as exc:
                policy = self._classify_stream_error(exc)
                if policy.retryable and attempt + 1 < self._STREAM_MAX_ATTEMPTS:
                    delay = self._STREAM_RETRY_BASE_SECONDS * (2**attempt)
                    logger.warning(
                        "[%s] QQ native stream retry %d/%d kind=%s code=%s delay=%.2fs",
                        self._log_tag,
                        attempt + 1,
                        self._STREAM_MAX_ATTEMPTS,
                        policy.kind,
                        policy.code,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.error(
                    "[%s] QQ native stream failed kind=%s status=%s code=%s "
                    "retryable=%s ambiguous=%s: %s",
                    self._log_tag,
                    policy.kind,
                    policy.http_status,
                    policy.code,
                    policy.retryable,
                    policy.ambiguous,
                    exc,
                )
                return None, policy
        return None, _StreamErrorPolicy("permanent", None)

    async def send_stream_frame(
        self,
        text: str,
        *,
        finalize: bool = False,
        chat_id: Optional[str] = None,
        reply_to: Optional[str] = None,
        turn_id: Optional[str] = None,
        **kwargs: Any,
    ) -> bool:
        """Create/update/finalize one QQ C2C stream keyed only by Hermes turn."""
        del kwargs
        chat_id = str(chat_id or "")
        turn_key = str(turn_id or "")
        if not chat_id or not turn_key:
            return False
        # Hermes currently probes native capability without chat_id, and both
        # QQ C2C and Guild DM are normalized to chat_type="dm".  For Guild DM,
        # absorb interim frames without opening QQ's C2C endpoint, then return
        # False on finalize so the consumer performs exactly one ordinary send.
        if getattr(self, "_chat_type_map", {}).get(chat_id) != "c2c":
            return not finalize
        states = self._native_states()
        terminal_failure = self._terminal_turns().get(turn_key)
        if terminal_failure:
            return False
        if finalize and turn_key in self._finalized_turns() and turn_key not in states:
            return True

        state = states.get(turn_key)
        if state is None:
            inbound_id = str(reply_to or "")
            if not inbound_id:
                return False
            state = _NativeStreamState(
                chat_id=chat_id,
                reply_to=inbound_id,
                msg_seq=self._next_msg_seq(inbound_id),
            )
            states[turn_key] = state

        async with state.lock:
            if state.close_after_failure:
                if not finalize:
                    return False
                body: Dict[str, Any] = {
                    "msg_id": state.reply_to,
                    "msg_seq": state.msg_seq,
                    "index": state.index,
                    "content_raw": state.last_text,
                    "content_type": self._STREAM_CONTENT_TYPE_MARKDOWN,
                    "input_mode": self._STREAM_INPUT_MODE_REPLACE,
                    "input_state": self._STREAM_INPUT_STATE_DONE,
                    "stream_msg_id": state.stream_msg_id,
                }
                await self._request_stream(
                    f"/v2/users/{state.chat_id}/stream_messages", body
                )
                self._remember_fallback_prefix(state)
                states.pop(turn_key, None)
                self._remember_terminal_turn(turn_key, state.close_after_failure)
                return False

            # The seed is local: QQ has no empty-frame typing primitive.  Tool-only
            # overlays also remain local so they never become immutable chat text.
            if not text and not finalize:
                return True
            frame_text = self._native_frame_text(text, finalize=finalize)
            if not finalize and not state.stream_msg_id and self._looks_like_tool_progress(frame_text):
                return True
            if not frame_text and not finalize:
                return True
            if finalize and not state.stream_msg_id:
                states.pop(turn_key, None)
                return False

            reconciled = self._reconcile_prefix(state.last_text, frame_text)
            if reconciled is None:
                logger.error(
                    "[%s] QQ native stream rejected divergent prefix: turn=%s index=%d",
                    self._log_tag,
                    turn_key,
                    state.index,
                )
                if state.stream_msg_id:
                    state.close_after_failure = "invalid_prefix"
                else:
                    states.pop(turn_key, None)
                    self._remember_terminal_turn(turn_key, "invalid_prefix")
                return False
            frame_text = reconciled
            if not finalize and state.stream_msg_id and frame_text == state.last_text:
                return True
            if len(frame_text) > self._C2C_STREAM_MAX_LENGTH:
                logger.error(
                    "[%s] QQ native stream exceeds local %d-char budget: turn=%s length=%d",
                    self._log_tag,
                    self._C2C_STREAM_MAX_LENGTH,
                    turn_key,
                    len(frame_text),
                )
                if state.stream_msg_id:
                    state.close_after_failure = "overflow"
                else:
                    states.pop(turn_key, None)
                    self._remember_terminal_turn(turn_key, "overflow")
                return False

            body: Dict[str, Any] = {
                "msg_id": state.reply_to,
                "msg_seq": state.msg_seq,
                "index": state.index,
                "content_raw": frame_text,
                "content_type": self._STREAM_CONTENT_TYPE_MARKDOWN,
                "input_mode": self._STREAM_INPUT_MODE_REPLACE,
                "input_state": (
                    self._STREAM_INPUT_STATE_DONE
                    if finalize
                    else self._STREAM_INPUT_STATE_GENERATING
                ),
            }
            if state.stream_msg_id:
                body["stream_msg_id"] = state.stream_msg_id

            data, failure = await self._request_stream(
                f"/v2/users/{state.chat_id}/stream_messages", body
            )
            if failure is not None:
                if failure.ambiguous and state.stream_msg_id:
                    # The timed-out cumulative frame may already be visible.
                    # Preserve that attempted body and let Hermes' best-effort
                    # second finalize repeat the same index with input_state=10;
                    # this is prefix-safe whether QQ accepted the first write.
                    state.last_text = frame_text
                    state.close_after_failure = failure.kind
                    return False
                if state.stream_msg_id:
                    state.close_after_failure = failure.kind
                else:
                    states.pop(turn_key, None)
                    self._remember_terminal_turn(
                        turn_key,
                        "opening_timeout" if failure.ambiguous else failure.kind,
                    )
                return False
            response_id = str((data or {}).get("id") or state.stream_msg_id)
            if not response_id:
                states.pop(turn_key, None)
                self._remember_terminal_turn(turn_key, "missing_id")
                return False
            state.stream_msg_id = response_id
            state.last_text = frame_text
            state.index += 1
            if finalize:
                states.pop(turn_key, None)
                self._remember_finalized_turn(turn_key, response_id)
            return True


def _validate_qq_config(config: Any) -> bool:
    """Use Hermes' profile-scoped env/YAML precedence for both credentials."""
    extra = getattr(config, "extra", {}) or {}
    app_id = str(extra_or_secret(extra, "app_id", "QQ_APP_ID", "") or "").strip()
    secret = str(
        extra_or_secret(extra, "client_secret", "QQ_CLIENT_SECRET", "") or ""
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
