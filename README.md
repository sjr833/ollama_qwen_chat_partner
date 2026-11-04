# ollama_qwen_chat_partner

基于 **FastAPI + Ollama(Qwen2.5) + MySQL** 的本地大模型多轮对话后端服务。

对外提供一个 SSE 流式对话接口，回答逐字返回；会话历史按 `session_id` 落库到 MySQL，支持多会话并发隔离、闲置自动过期、请求限流与失败回滚。

---

## 功能特性

- **流式输出**：对接 Ollama `/api/chat`，边生成边推送（`text/event-stream`），前端可逐字渲染
- **多会话隔离**：每个 `session_id` 一把独立异步锁，同一会话串行、不同会话并行
- **历史持久化**：消息存 MySQL（SQLAlchemy 2.0 异步 ORM），重启不丢上下文
- **上下文自动过期**：会话闲置 30 分钟自动清理（后台协程每 60 秒扫描）
- **请求限流**：同一 `session_id` 两次请求至少间隔 3 秒，超限返回 429
- **模型白名单**：只允许 `ALLOWED_MODELS` 中的模型，防止调用未下载/未授权模型
- **失败回滚**：Ollama 报错、连接中断或服务异常时，自动删除刚落库的 user 消息，保证历史干净

---

## 技术栈

| 组件 | 说明 |
|---|---|
| FastAPI | Web 框架，异步路由 + `StreamingResponse` |
| Pydantic v2 | 请求体校验（长度、范围约束） |
| Ollama | 本地大模型推理服务，默认模型 `qwen2.5:7b` |
| aiohttp | 异步调用 Ollama 流式接口 |
| SQLAlchemy 2.0 | 异步 ORM（`create_async_engine` + `AsyncSession`） |
| aiomysql | MySQL 异步驱动 |
| MySQL 8 | 会话消息存储 |

Python 版本要求 **3.10+**（代码使用了 `str | None` 语法）。

---

## 目录结构

```
ollama_qwen_chat_partner/
├── main.py          # 全部服务代码：ORM 模型、数据库操作、流式对话、路由
├── README.md
├── LICENSE
└── .gitignore
```

单文件项目，`main.py` 内部分为四块：**配置常量 → ORM 模型/引擎 → 数据库操作函数 → 路由与业务逻辑**。

---

## 快速开始

### 1. 准备 Ollama

```bash
# 安装后拉取模型（首次约 4.7GB）
ollama pull qwen2.5:7b
ollama serve
```

确认服务在 `http://127.0.0.1:11434` 可用：

```bash
curl http://127.0.0.1:11434/api/tags
```

### 2. 准备 MySQL

```sql
CREATE DATABASE ollama_qwen_chat_partner_messages
  DEFAULT CHARSET utf8mb4
  COLLATE utf8mb4_unicode_ci;

CREATE USER 'hello_qwen'@'localhost' IDENTIFIED BY '123456';
GRANT ALL PRIVILEGES ON ollama_qwen_chat_partner_messages.* TO 'hello_qwen'@'localhost';
FLUSH PRIVILEGES;
```

> 表不用手动建，服务启动时会执行 `Base.metadata.create_all` 自动建表（幂等）。

### 3. 安装依赖

```bash
pip install fastapi uvicorn aiohttp sqlalchemy aiomysql cryptography pydantic
```

`cryptography` 必须有：MySQL 8 默认使用 `caching_sha2_password` 认证，缺了它会报
`RuntimeError: 'cryptography' package is required`。

### 4. 配置数据库连接

修改 `main.py` 中的 `DB_URL`：

```python
DB_URL = "mysql+aiomysql://hello_qwen:123456@localhost:3306/ollama_qwen_chat_partner_messages?charset=utf8"
```

格式：`mysql+aiomysql://用户名:密码@主机:端口/库名?charset=utf8mb4`

> ⚠️ 当前账号密码是硬编码的，推公开仓库前建议改为读环境变量，例如
> `os.getenv("DB_URL", "mysql+aiomysql://...")`，账号密码不要进版本库。

### 5. 启动服务

```bash
python main.py
# 或
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

启动成功后可访问交互式文档：<http://127.0.0.1:8000/docs>

---

## API 说明

### 1. 流式对话

`POST /chat_stream`

请求体（JSON）：

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `prompt` | string | 是 | 1 ~ 4096 字符 | 用户提问内容 |
| `session_id` | string | 是 | 1 ~ 100 字符 | 会话标识，由前端生成并保持稳定 |
| `model` | string | 否 | 必须在白名单内 | 默认 `qwen2.5:7b` |
| `temperature` | float | 否 | 0.0 ~ 2.0 | 默认 0.7，越大越发散 |
| `system` | string | 否 | ≤ 2048 字符 | 系统提示词，仅本次请求生效，不入库 |

请求示例：

```bash
curl -N -X POST http://127.0.0.1:8000/chat_stream \
  -H "Content-Type: application/json" \
  -d '{
        "prompt": "用一句话解释什么是闭包",
        "session_id": "demo-001",
        "model": "qwen2.5:7b",
        "temperature": 0.7,
        "system": "你是一个简洁的技术助手"
      }'
```

响应为 SSE 流，每个分片是一段增量文本：

```
data: 闭包

data: 是指

data: 函数与其
```

前端消费（浏览器）：

```javascript
const resp = await fetch("http://127.0.0.1:8000/chat_stream", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ prompt: "你好", session_id: "demo-001" }),
});
const reader = resp.body.getReader();
const decoder = new TextDecoder();

while (true) {
  const { done, value } = await reader.read();
  if (done) break;
  const chunk = decoder.decode(value, { stream: true });
  for (const line of chunk.split("\n")) {
    if (line.startsWith("data: ")) {
      document.body.append(line.slice(6));   // 逐字追加
    }
  }
}
```

错误码：

| 状态码 | 场景 | 返回 |
|---|---|---|
| 400 | `model` 不在白名单 | `{"detail": "不支持的模型，可选：['qwen2.5:7b']"}` |
| 429 | 同一会话 3 秒内重复请求 | `{"detail": "请求过于频繁，请2.4秒后再试"}` |
| 422 | 请求体校验失败（缺字段/超长） | Pydantic 标准错误结构 |
| 200 + 流内错误 | Ollama 返回非 200 | `data: [Ollama错误 404] ...` |
| 200 + 流内错误 | 连不上 Ollama | `data: [连接失败] 无法连接到Ollama服务，请确认Ollama已启动` |
| 200 + 流内错误 | 其他异常 | `data: [服务器错误] ...` |

> 注意：Ollama 侧的错误是在流开始后才可能出现的，无法用 HTTP 状态码表达，只能以 `data:` 文本下发，前端需按前缀识别。

### 2. 清空会话上下文

`POST /clear?session_id=xxx`

```bash
curl -X POST "http://127.0.0.1:8000/clear?session_id=demo-001"
# {"msg": "会话 demo-001 上下文已经清空"}
```

只删除该会话的消息记录，不删除会话锁（锁复用，避免反复创建）。

---

## 数据库表结构

`messages` 表：

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | INT PK AUTO_INCREMENT | 自增主键，同时是消息顺序 |
| `session_id` | VARCHAR(100) NOT NULL，**索引** | 会话标识 |
| `role` | VARCHAR(32) NOT NULL | `user` / `assistant` |
| `content` | TEXT NOT NULL | 消息正文 |
| `create_time` | FLOAT NOT NULL | Unix 时间戳（`time.time()`） |

等效 DDL（服务会自动创建，无需手写）：

```sql
CREATE TABLE messages (
  id INT AUTO_INCREMENT PRIMARY KEY,
  session_id VARCHAR(100) NOT NULL,
  role VARCHAR(32) NOT NULL,
  content TEXT NOT NULL,
  create_time FLOAT NOT NULL,
  INDEX ix_messages_session_id (session_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

---

## 关键设计说明

### 会话级串行，会话间并行

```
请求 A(session=1) ─┐
请求 B(session=1) ─┴─ 同一把 lock → 串行，避免历史读写交错
请求 C(session=2) ─── 另一把 lock → 与 A 并行
```

没有这把锁时，两个并发请求会读到相同的历史、各自追加 user 消息，导致上下文错乱。

`GLOBAL_META_LOCK` 保护的是 `session_last_active` / `session_last_request` 两个字典；后台清理任务也要动它们，所以必须与请求路径互斥。

### 消息入库时机

- **user 消息**：请求 Ollama 之前就落库，拿到自增 id 作为回滚凭据
- **assistant 消息**：整段流式结束后一次性落库（避免每个 token 写一次库）
- **失败回滚**：任何异常分支都调用 `delete_message_by_id(user_msg_id)`，防止出现"只有提问没有回答"的孤儿消息

### TTL 清理

后台协程每 60 秒扫一遍 `session_last_active`，超过 30 分钟未活跃的 `session_id` 会被删除消息记录，并清空内存里的两个字典。检查与删除分两段执行（先收集、后删除），避免长时间持锁阻塞请求。

---

## 可配置项

`main.py` 顶部常量：

| 常量 | 默认值 | 说明 |
|---|---|---|
| `ALLOWED_MODELS` | `["qwen2.5:7b"]` | 模型白名单，需与 `ollama list` 一致 |
| `SESSION_TTL` | `30 * 60` | 会话闲置过期时间（秒） |
| `CLEAN_INTERVAL` | `60` | 过期扫描周期（秒） |
| `RATE_LIMIT_SECONDS` | `3` | 同会话最小请求间隔（秒） |
| `DB_URL` | `mysql+aiomysql://...` | 数据库连接串 |
| `async_engine(echo=)` | `False` | 改 `True` 可打印所有 SQL，调试用 |

---

## 常见问题

**Q：报 `Can't connect to MySQL server on 'localhost'`**
MySQL 没启动，或端口不是 3306。先用 `mysql -u hello_qwen -p` 手工验证连接。

**Q：报 `'cryptography' package is required`**
`pip install cryptography`，MySQL 8 默认认证方式强依赖它。

**Q：报 `Access denied for user 'hello_qwen'`**
账号或密码错，或该账号没有目标库权限，重新执行授权 SQL。

**Q：回答里中文乱码**
连接串 `charset` 用 `utf8mb4`，且库/表字符集也应是 `utf8mb4`。

**Q：`/chat_stream` 返回 429**
同一 `session_id` 3 秒内重复请求，属正常限流；换 `session_id` 或等待即可。

**Q：流式返回的内容被一次性打印，不"流"**
客户端或 curl 做了缓冲。curl 用 `-N`；Nginx 反代需 `proxy_buffering off;`。

**Q：`--reload` 模式下启动日志出现两次**
热重载会重开进程，`startup` 事件因此执行两次；生产环境不要开 `reload`。

**Q：上下文突然消失**
闲置超过 30 分钟被 TTL 清理了，或调过 `/clear`。两者都会删库记录且不可恢复。

---

## Roadmap

- [ ] RAG 检索增强（文档向量化 + 知识库问答）
- [ ] 多模型支持与模型切换
- [ ] 会话列表 / 历史消息查询接口
- [ ] 数据库连接串与密钥迁移到 `.env`
- [ ] Alembic 数据库迁移
- [ ] Docker Compose 一键启动（app + mysql + ollama）
- [ ] 前端对话界面

---

## License

[MIT](LICENSE) © 2026 sjr833
