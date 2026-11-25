# 飞书 IM 统计和监听

在 WSL 中使用飞书官方 Go SDK，以授权用户的身份读取应用权限范围内的私聊和群聊消息。

## 运行

在 Ubuntu WSL 中进入项目目录，使用 Go 1.25 或更新版本编译。

```bash
bash ./build.sh
bash ./run.sh login
bash ./run.sh stats --since 24h
bash ./run.sh watch --interval 15s
```

`run.sh` 会在可执行文件缺失时自动构建，也可以不带参数直接进入监听模式。修改代码后运行 `bash ./build.sh` 更新可执行文件。

`login` 会打印飞书授权链接。请打开链接，并用要同步的飞书账号授权。已有有效授权时可以直接运行 `stats` 或 `watch`。

`stats` 输出指定时段的私聊和群聊数量、消息数量，以及消息最多的十个会话。两种类型都会统计该时段内读到的消息，包括本人发送的消息。读取部分会话或话题失败时，程序会输出已读到的统计结果并返回非零退出码。

可用 `stats --since 1h --until 2026-11-25T10:13:28+08:00` 重跑同一时间段。遇到飞书限频时会暂停并重试。飞书保护模式限制读取的消息不会进入总数。

`watch` 从启动时开始报告新消息，在终端打印消息时间、会话、发送者和文本摘要。富文本会显示标题、正文摘要和链接。独立图片和富文本中的图片会显示需要授权的飞书资源 API 地址，有飞书原消息跳转地址时也会显示。文件等其余类型以消息类型表示。设置 `--interval 5s` 可降低轮询间隔。忙碌会话或限频可能延长实际提示延迟。按 `Ctrl+C` 停止。

每轮跳过本地排除的会话，先查置顶会话，再按飞书返回的活跃顺序检查其他会话，最后从后台队列轮扫。直接 @ 当前账号的消息会显示 `[@我]`。免打扰会话只提示这类消息，包括已跟踪的话题回复。普通消息、@ 其他人和仅 @所有人都跳过。最近活跃阶段遇到一个成功查完、且从上次热点检查时间起没有新消息的会话，就结束该阶段。排在后面的会话仍由后台队列覆盖。

在应用配置目录下创建可选的 `watch-exclude.txt`，每行填一个要跳过的会话 ID（`oc_...`），可用 `#` 写注释。重新启动 `watch` 后读入名单。置顶列表和已查询的免打扰状态分别缓存 5 分钟，查不到新状态时沿用现有缓存并输出提示。尚未确认免打扰状态的群聊也只提示直接 @ 当前账号的消息。

飞书的个人账号消息通过轮询读取，程序会重叠扫描最近 90 秒并去重。话题回复会在已发现的话题中拉取。`watch --thread-lookback 168h` 可将启动时发现群话题的范围扩大到 7 天，默认 24 小时。`stats` 使用指定统计时段发现话题。更早的话题如果未出现在会话查询结果中，其中的新回复可能无法发现。

## 写入 OpenViking

把 OpenViking 的连接参数和群聊名单写到 WSL 的 `~/.config/feishu-im-watch/openviking.json`。文件权限设为 `600`。如需指定托管 Agent，可添加 `agent_id`。

```json
{
  "endpoint": "https://api.vikingdb.cn-beijing.volces.com/openviking",
  "api_key": "your_openviking_api_key",
  "group_ids": ["oc_your_group_id"]
}
```

运行 `bash ./run.sh` 即可开始监听和入库。入库包括监听到的私聊和 `group_ids` 名单中的群聊。空名单只写入私聊。`watch-exclude.txt` 仍适用。免打扰设置控制终端提示，选中的会话仍可入库。配置在重启后生效。

已有的 `OPENVIKING_API_KEY` 和 `OPENVIKING_AGENT_ID` 环境变量会覆盖对应的 JSON 字段。没有 `openviking.json` 时，仍支持原有的环境变量和 `openviking-groups.txt` 配置。

同一飞书账号下，每个 `chat_id` 对应一个固定的 OpenViking Session。话题根消息和回复归入所属会话，记录 `thread_id`、`root_id` 和 `parent_id`。消息保留原始正文和发送时间，图片以资源地址和原正文中的资源标识保存。程序关闭 Session 自动提交，只追加消息。

本地 `openviking.sqlite` 保存待上传消息和已采集会话的游标。后台上传失败会保留待处理记录，重启后继续处理。追加请求结果不明确时会查回 OpenViking 核对，有待核对消息的会话会等到确认后才继续上传。

## 应用配置

项目的 [`template/`](template/) 目录提供飞书和 OpenViking 的脱敏配置。首次部署可以复制所需文件到应用配置目录，填入实际值，再将文件权限设为 `600`。

飞书应用凭证保存在 WSL 的 `~/.config/feishu-im-watch/feishu.json`，也支持 `XDG_CONFIG_HOME`。已有的 `credentials.json` 可以直接改名，程序仍可读取尚未迁移的旧文件。首次在其他环境使用时，把应用 ID 和密钥填入以下格式，并用 `chmod 600` 限制文件权限。

```json
{"app_id":"cli_your_app_id","app_secret":"your_app_secret"}
```

授权完成后，程序会在同目录保存用户会话，并在访问令牌即将过期时自动刷新。应用需要面向该用户开通以下权限并发布生效。

- `im:chat:read`
- `im:message:readonly`
- `im:message.p2p_msg:get_as_user`
- `im:message.group_msg:get_as_user`
- `im:feed.shortcut:read`，用于读取当前用户的置顶会话。已有会话需在应用发布该权限后重新运行 `bash ./run.sh login` 授权。
- `offline_access`，并在应用安全设置中开启用户令牌刷新

参考 [飞书历史消息 API](https://open.feishu.cn/document/server-docs/im-v1/message/list) 和 [用户令牌刷新 API](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/authentication-management/access-token/refresh-user-access-token-v3)。
