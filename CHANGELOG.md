# Changelog

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
