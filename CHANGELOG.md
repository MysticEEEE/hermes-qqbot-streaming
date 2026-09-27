# Changelog

## 0.6.0-beta.3 — 2026-09-27

### Changed

- Migrate persistent C2C delivery from the legacy send/edit shim to Hermes' native `send_stream_frame` contract
- Keep all stream identity and progress state scoped by Hermes `turn_id`; remove chat-level send/edit state
- Leave group, guild, ordinary sends, and tool-progress messages on the built-in QQ adapter paths
- Use Hermes' profile-scoped shared secret reader for configuration validation

### Fixed

- Keep tool-progress overlays out of QQ's immutable stream content and finalize the authoritative answer in the same stream
- Rotate response IDs, preserve monotonic indexes and immutable prefixes, and make duplicate finalize calls idempotent
- Preserve HTTP status and QQ business codes through a stream-only HTTP seam; classify `40007`, missing stream IDs, `50001`, `50002`, HTTP 429, and timeout outcomes with bounded retry/fail-closed behavior
- Keep opened streams closable after timeout, definitive API failure, prefix divergence, and local 30000-character overflow
- Close the acknowledged overflow prefix and deliver only the non-overlapping tail through ordinary send
- Keep real Markdown horizontal rules and symbol/emoji-leading answer text out of tool-overlay filtering
- Keep Guild DM off the C2C endpoint and fall back exactly once at finalization
- Keep REST streaming available during a transient WebSocket disconnect

### Verification

- 47 passing automated tests with the companion Hermes core lifecycle patch, including real `GatewayStreamConsumer.run()` coverage for final delivery, approval, clarify/reopen, Guild DM, failure close, and opened-stream overflow lifecycles
- On unpatched Hermes `d0288be5`, 44 tests pass and three strict xfails document the native tool-progress opt-out plus cancel/stale cleanup gaps
- Verified against Hermes `d0288be5b3330d2442e3907185b8e9d0958297bb`
- Passed a 2026-09-27 desktop QQ C2C visual test: one progressively growing reply bubble, no repeated body, no duplicate final send, and no residual cursor or loading animation

## 0.6.0-beta.2 — 2026-08-13

### Fixed

- Adopt each fragment response's latest `stream_msg_id` for the next request, including when Hermes keeps passing the original ID
- Bind `msg_id`, `msg_seq`, `index`, and acknowledged text to each stream so overlapping C2C turns cannot retarget one another
- Restore the built-in 4000-character budget after a C2C stream completes
- Preserve a newer stream's progress when an older overlapping stream finalizes
- Route post-limit overflow tails through the built-in normal-send path

### Verification

- 21 automated regression and integration tests

## 0.6.0-beta.1 — 2026-08-13

Initial public beta.

### Added

- Persistent QQ C2C streaming through `/v2/users/{openid}/stream_messages`
- Explicit `input_state=10` finalization on the same `stream_msg_id`
- Markdown mutable-tail holdback with fail-closed prefix-divergence handling
- Per-C2C streaming length budget that avoids the normal QQ 4000-character split
- Diagnostic frame logging for index, state, lengths, response ID, and `remain_msg_len`
- 15 automated regression and integration tests
- Exact delivered-prefix metadata for safe Hermes fallback after edit/finalize failures

### Fixed

- Duplicate final bubbles caused by treating QQ streams as disposable Hermes drafts
- Repeated full-answer accumulation that could inflate a 1950-character reply to 24827 characters
- Markdown temporary closing markers causing prefix divergence and fallback sends
- Cursor/loading animation remaining after completion
- Long replies splitting around 3900 characters and delivering the tail as a delayed static bubble
