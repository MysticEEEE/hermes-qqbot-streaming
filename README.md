# Hermes QQ Bot C2C Streaming

为 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 的 QQ Bot 增加基于腾讯 QQ 官方 C2C `stream_messages` API 的持久单气泡流式输出。

> 当前为 beta 版本。插件依赖 Hermes 的内部 QQ adapter 与 Gateway streaming 接口；Hermes 升级后可能需要同步适配。

## 功能

- QQ C2C 私聊在同一个持久气泡中逐步增长
- 使用 `/v2/users/{openid}/stream_messages`
- 使用 `input_state: 1 → 10` 正常完成流式消息
- 支持 Markdown 与长文本
- 通过尾部缓冲保护 QQ `replace` 模式的不可修改前缀
- 遇到无法无损恢复的前缀分歧时明确失败，绝不静默拼接错误正文
- 防止完成后重复发送普通最终消息
- 群聊、Guild、频道、媒体、ACL 等能力继续复用 Hermes 内置 `QQAdapter`

## 要求

- 一个已经启用 QQ Bot 的 Hermes Agent 安装
- `QQ_APP_ID`
- `QQ_CLIENT_SECRET`
- Python 3.11+

本插件已在 Hermes commit `a871948` 上验证。它继承 Hermes 内置 `gateway.platforms.qqbot.adapter.QQAdapter`，因此不是独立 QQ Bot SDK。

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
qqbot-streaming  enabled  0.6.0-beta.2
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

## 实测范围

`v0.6.0-beta.2` 已完成：

- 21 项自动化测试
- 约 1000 字 Markdown 富文本单气泡流式
- 约 3200–3500 字纯文本单气泡流式
- 两次约 4900–5000 字 C2C 长文本单气泡流式
- 同一 `stream_msg_id` 从首帧保持到 `input_state=10`
- 无 fallback 普通消息补发

## 已知限制

- 仅 QQ C2C 私聊使用原生 `stream_messages`；群聊与频道不启用此路径。
- QQ `replace` 模式要求新帧严格保留已经下发的内容前缀。
- 插件保留最近 128 个字符作为可变尾部，避免 Hermes 临时闭合 Markdown 时改写 QQ 已确认前缀。
- C2C 本地流式预算暂设为 30000 字，这是防止 Hermes 在普通消息的 4000 字边界提前分片，**不是腾讯官方公布的最大长度**；当前真实测试覆盖到约 5000 字。
- QQ API 响应中的 `remain_msg_len=0` 在实测中不代表容量耗尽。
- 插件依赖 Hermes 内部 Python API，尚未承诺跨所有 Hermes 版本兼容。
- 若 QQ/API 或最终前缀异常导致原地完成失败，插件会把实际已投递前缀交给 Hermes fallback：可安全续发时只补尾部，否则发送完整最终答案；不会把拼接错误的正文标记为完成。

## 故障排查

日志位置：

```text
~/.hermes/logs/gateway.log
```

重点搜索：

```text
QQ stream frame accepted
QQ stream response changed id
QQ stream divergent frame held
QQ stream final frame diverged
Streaming draft failed
edit_message failed
```

正常流的特征：

- `index` 单调递增
- 中间帧 `state=1`
- 最终帧 `state=10`
- 全程 `response_id` 不变
- 没有普通 `Sending response (...)` 补发

## 许可

MIT License。详见 [LICENSE](LICENSE)。
