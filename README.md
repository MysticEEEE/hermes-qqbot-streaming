# Hermes QQ Bot C2C Streaming

为 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 的 QQ Bot 增加基于腾讯 QQ 官方 C2C `stream_messages` API 的持久单气泡流式输出。

> 当前为 beta 版本。插件依赖 Hermes 的内部 QQ adapter 与 Gateway streaming 接口；Hermes 升级后可能需要同步适配。

## 功能

- QQ C2C 私聊在同一个持久气泡中逐步增长
- 使用 `/v2/users/{openid}/stream_messages`
- 使用 `input_state: 1 → 10` 正常完成流式消息
- 支持 Markdown 与长文本
- 将 Hermes 临时闭合的 Markdown 尾部留在本地，保护 QQ `replace` 模式的不可修改前缀
- 遇到无法无损恢复的前缀分歧时明确失败，绝不静默拼接错误正文
- 防止完成后重复发送普通最终消息
- 群聊、Guild、频道、媒体、ACL 等能力继续复用 Hermes 内置 `QQAdapter`

## 要求

- 一个已经启用 QQ Bot 的 Hermes Agent 安装
- `QQ_APP_ID`
- `QQ_CLIENT_SECRET`
- Python 3.11+

本插件已在 Hermes commit `d0288be5b3330d2442e3907185b8e9d0958297bb` 上验证，并使用其 `SUPPORTS_NATIVE_STREAMING` / `send_stream_frame` contract。它继承 Hermes 内置 `gateway.platforms.qqbot.adapter.QQAdapter`，因此不是独立 QQ Bot SDK。

> **beta.3 兼容性提示：** 完整的“工具进度不进入持久正文、cancel/stale 必定收口”保证还依赖配套的 Hermes core 生命周期补丁。未应用补丁的 `d0288be5` 可以运行插件，但保留 3 项严格 xfail 所记录的缺口；不要把这种组合用于本版最终验收。

## 安装

推荐使用 Hermes 插件安装器：

```bash
hermes plugins install MysticEEEE/hermes-qqbot-streaming --enable
hermes gateway restart
```

也可以手工安装：

```bash
git clone https://github.com/MysticEEEE/hermes-qqbot-streaming.git ~/.hermes/plugins/qqbot-streaming
hermes plugins enable qqbot-streaming
hermes gateway restart
```

如果手工安装的目标目录已经存在，先备份或移走旧目录，不要把仓库克隆进现有插件目录的子目录。

确认加载：

```bash
hermes plugins list
```

应看到：

```text
qqbot-streaming  enabled  0.6.0-beta.3
```

## 配置

将凭据存入 Hermes 的 secret/env 配置，不要提交到 Git：

```text
QQ_APP_ID
QQ_CLIENT_SECRET
```

插件注册平台名为 `qqbot`，通过 Hermes 平台注册表覆盖内置 QQ adapter。只有当前收到 C2C 私聊消息的 chat 才使用扩展的流式长度预算；群聊和其他普通发送仍保留内置 4000 字限制。

## 验证

在 QQ 私聊发送：

```text
请生成约5000字、包含15个不同编号章节的中文长文；第一行输出 BEGIN，最后一行输出 END，不要重复段落。
```

预期：

- 只有一个机器人气泡
- 气泡在原位置持续增长
- `BEGIN` 和 `END` 各出现一次
- 内容不重复
- 完成后无光标或加载动画

运行自动化测试：

```bash
python -m pytest -q
```

## 当前版本自动化验证范围

基于 Hermes `d0288be5b3330d2442e3907185b8e9d0958297bb`：

- 配合独立 Hermes core 生命周期补丁时，47 项自动化测试全部通过
- 直接使用 Hermes `d0288be5b3330d2442e3907185b8e9d0958297bb` 时为 44 项通过、3 项严格 xfail；它们记录 native tool-progress opt-out 与 cancel/stale cleanup 三个 core 缺口
- 覆盖真实 `GatewayStreamConsumer.run()` 的最终交付、工具阶段、approval、clarify/reopen、Guild DM、失败关闭和已打开 stream 超限路径
- 覆盖真实 HTTP request/response seam 下的 `50001`、`50002`、HTTP 429、`40007` 和 timeout 分类
- 每一帧采用上一响应返回的最新 `stream_msg_id`
- 已打开 stream 超限后只普通发送未下发尾部，不重复完整前缀

`0.6.0-beta.3` 已于 2026-09-27 完成电脑版 QQ C2C 视觉验收：约 5000 字回复在单一气泡中持续增长，正文无重复，最终无光标、加载动画或额外完整补发气泡。

## 已知限制

- 仅 QQ C2C 私聊使用原生 `stream_messages`；群聊与频道不启用此路径。
- QQ `replace` 模式要求新帧严格保留已经下发的内容前缀。
- 插件不提交 Hermes 临时闭合的末尾 Markdown span，避免下一帧改写 QQ 已确认前缀。
- C2C 本地流式预算暂设为 30000 字，这是防止 Hermes 在普通消息的 4000 字边界提前分片，**不是腾讯官方公布的最大长度**；当前真实测试覆盖到约 5000 字。
- QQ API 响应中的 `remain_msg_len=0` 在实测中不代表容量耗尽。
- 插件依赖 Hermes 内部 Python API，尚未承诺跨所有 Hermes 版本兼容。
- `40007` / `stream_msg_id not found` 会 fail closed；`50001`、`50002` 和 HTTP 429 仅做有界退避；写入超时按“可能已送达”处理，不作为普通增量盲目重放，而是由 native consumer 以相同 cumulative body 和 index 做一次 `input_state=10` 收口。
- Hermes `d0288be5b3330d2442e3907185b8e9d0958297bb` 的 cancel/stale abandonment 只清理 draft transport，且 native consumer 没有 adapter 级 tool-progress opt-out。三项对应测试保留为严格 xfail；独立 Hermes core 补丁已在本地 worktree 通过 121 项相关回归测试，插件与该补丁组合时 47 项全部通过。补丁合入上游前，插件无法安全伪造这些缺失的生命周期信号。
- Hermes 的 native capability probe 不传 `chat_id`。插件会在帧入口使用 QQ adapter 的真实 wire chat kind 二次校验，因此 Guild DM 不调用 C2C endpoint，并在最终阶段只 fallback 一次。

## 故障排查

日志位置：

```text
~/.hermes/logs/gateway.log
```

重点搜索：

```text
QQ native stream retry
QQ native stream failed
QQ native stream rejected divergent prefix
QQ native stream exceeds local
```

正常流的特征：

- `index` 单调递增
- 中间帧 `state=1`
- 最终帧 `state=10`
- 下一帧的 `stream_msg_id` 等于上一帧响应的 `id`（允许响应 ID 轮换）
- 没有普通 `Sending response (...)` 补发

## 许可

MIT License。详见 [LICENSE](LICENSE)。
