# 异步图片任务

本分支为 OpenAI 图片接口增加了统一的异步任务接口。客户端提交后立即拿到
NewAPI 生成的任务 ID，渠道设置决定 NewAPI 如何调用上游。

## 渠道配置

- 渠道 Base URL 可填写 `https://mianyunai.com` 或 `https://mianyunai.com/v1`。
- 客户端请求建议传 `async=true`。模型名不写死，渠道中配置的模型名会原样转发。
  例如 `gpt-image-2-async`、`gpt-image-2-4k-async` 以及后续新增模型都可以使用。
- `size`、`image_size`、`output_resolution` 等尺寸字段不在 NewAPI 中按固定模型档位改写，按请求值转发给上游校验。
- 渠道高级设置中的“图片任务上游模式”有两种选择：
  - `async`（默认）：上游返回 `task_id`，NewAPI 后台轮询上游任务。
  - `sync`：上游直接返回图片，NewAPI 先保存本地任务，再由后台任务执行同步请求并写回结果。
- 配置保存到渠道 `settings` JSON 时，对应字段为
  `{"image_task_mode":"async"}` 或 `{"image_task_mode":"sync"}`。
- 在系统模型定价中为每个异步模型分别配置按次价格。代码不会预设价格，
  以避免覆盖实际采购成本。
- 保持 `UPDATE_TASK=true`（默认值），否则后台不会轮询异步任务。
- `ASYNC_IMAGE_WORKER_SLOTS` 控制“同步上游生图”在单个 NewAPI 进程中的最大并发数，默认 `20`。超过执行槽数量的任务保持 `QUEUED`，有槽释放后继续执行。此设置不影响异步上游生图、视频及其他请求，修改后需要重启 NewAPI。需要 50 并发时可显式设置 `ASYNC_IMAGE_WORKER_SLOTS=50`。
- 同步上游请求内容写入 `ASYNC_IMAGE_QUEUE_DIR`（Docker Compose 默认为 `/data/async-image-queue`），PostgreSQL 只保存文件名，不再保存完整 JSON 或 multipart 图片。`QUEUED` 任务可在重启后从 `/data` 数据卷继续执行；成功或失败后都会删除对应 payload 文件。
- 同步上游只请求一次，硬超时为 300 秒。达到超时、连接失败或上游返回错误时，任务立即标记为 `FAILURE`、删除 payload 并退款，不自动重试，避免重复生图。
- 同步上游 worker 每 30 秒更新一次轻量心跳。进程重启或异常退出后，超过 90 秒没有心跳的 `IN_PROGRESS` 任务会标记为失败、删除 payload 并退款，不会重新请求上游造成重复生图。

## 提交任务

```http
POST /v1/images/generations
Authorization: Bearer <new-api-token>
Content-Type: application/json
```

```json
{
  "async": true,
  "images": ["https://example.com/reference.png"],
  "model": "gpt-image-2-async",
  "n": 1,
  "prompt": "电影感城市夜景",
  "quality": "medium",
  "size": "1024x1024"
}
```

编辑接口使用 `POST /v1/images/edits`。JSON 参考图别名和 multipart 中重复的
`image` 文件字段会原样转发；multipart 文件单个最大 10 MB。`mask` 文件仅允许
1 个带 alpha 通道的 PNG，并会校验它与第一张 multipart 输入图的格式和尺寸。
HTTPS URL 形式的图片内容校验由图片上游完成。

创建任务时返回的是 NewAPI 生成的公开任务 ID，不会暴露上游任务 ID：

```json
{
  "id": "task_xxx",
  "model": "gpt-image-2-async",
  "object": "image.generation",
  "progress": "10%",
  "status": "queued"
}
```

## 查询任务

```http
GET /v1/images/generations/{task_id}
GET /v1/images/edits/{task_id}
Authorization: Bearer <new-api-token>
```

完成响应：

```json
{
  "data": [{"url": "https://example.com/image.png"}],
  "id": "task_xxx",
  "model": "gpt-image-2-async",
  "object": "image.generation",
  "progress": "100%",
  "status": "completed"
}
```

任务只能由创建它的用户查询。生成任务不能通过编辑查询接口读取，编辑任务也
不能通过生成查询接口读取。
