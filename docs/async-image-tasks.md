# 异步图片任务

本分支为 OpenAI 图片接口增加了可选的异步任务模式。未传 `async` 或传
`async=false` 时，仍使用 NewAPI 原有的同步图片转发流程。

## 渠道配置

- 渠道 Base URL 可填写 `https://mianyunai.com` 或 `https://mianyunai.com/v1`。
- 渠道模型必须包含 `gpt-image-2-async`。
- 在系统模型定价中为 `gpt-image-2-async` 配置按次价格。代码不会预设价格，
  以避免覆盖实际采购成本。
- 保持 `UPDATE_TASK=true`（默认值），否则后台不会轮询异步任务。

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
