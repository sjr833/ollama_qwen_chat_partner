# ollama_qwen_chat_partner

基于 **FastAPI + Ollama + MySQL + Redis** 的本地大模型多轮对话后端。

服务把 Ollama 的生成结果以 SSE 逐字推给前端，对话历史按 `session_id` 存进 MySQL，支持多会话隔离、闲置自动过期、请求限流和失败回滚。模型跑在本机，数据不出网。

```
浏览器 ──POST /chat_stream──▶ FastAPI ──/api/chat──▶ Ollama
                                ├──▶ MySQL：读写会话历史
                                └──▶ Redis：限流计数
```

---

## 目录

- [功能特性](#功能特性)
- [技术栈](#技术栈)
- [项目结构](#项目结构)
- [快速开始](#快速开始)
- [接口说明](#接口说明)
- [数据库表结构](#数据库表结构)
- [关键设计](#关键设计)
- [可配置项](#可配置项)
- [常见问题](#常见问题)
- [已知限制](#已知限制)
- [License](#license)

---

## 功能特性

| 特性 | 说明 |
|---|---|
| **流式输出** | 转发 Ollama `/api/chat` 的 NDJSON 流，按行解析后重新封装成 SSE 帧；每个 token 一帧，文本里的换行拆成多行 `data:`，帧间空行分隔，末尾 `data: [DONE]` |
| **多轮上下文** | 每轮先把该会话最近 `MAX_HISTORY_MESSAGES`（默认 20）条历史读出来，拼上当前提问再送给模型 |
| **多会话隔离** | 每个 `session_id` 一把独立的 `asyncio.Lock`：同会话请求串行、不同会话并行 |
| **历史持久化** | user / assistant 消息都写入 MySQL，服务重启不丢上下文 |
| **上下文自动过期** | 活跃时间写在 `session_meta.last_active_at`，每轮问答 upsert 刷新；闲置 30 分钟后由后台协程清理 |
| **请求限流** | 同一 `session_id` 两次请求至少间隔 3 秒，用 Redis `SET NX EX` 做原子占位，超限返回 429 |
| **模型白名单** | 只放行 `ALLOWED_MODELS` 里的模型，其他直接 400，避免请求打到未下载的模型上 |
| **失败回滚** | Ollama 报错、连不上、流中抛异常时删掉刚落库的 user 消息，不留“只有问没有答”的孤儿记录 |
| **主动清空** | `/clear` 一次清掉该会话的消息、会话状态和限流 key |

## 技术栈

| 组件 | 用途 |
|---|---|
| Python 3.10+ | 代码用了 `str \| None` 语法 |
| FastAPI | Web 框架，异步路由 + `StreamingResponse` |
| uvicorn | ASGI 服务器，`main.py` 直接 `uvicorn.run(...)` |
| Pydantic v2 | 请求体校验（长度、取值范围） |
| aiohttp | 异步调用 Ollama 流式接口 |
| SQLAlchemy 2.0（async） | 异步 ORM，`create_async_engine` + `AsyncSession` |
| aiomysql | MySQL 异步驱动，由连接串 `mysql+aiomysql` 触发加载 |
| Redis | 限流计数，`redis.asyncio`，服务端需 6.0+ |
| Ollama | 本地模型推理，默认 `qwen2.5:7b` |

## 项目结构

```
ollama_qwen_chat_partner/
├── main.py            # FastAPI 主逻辑：app、请求模型、两个路由、启动/关闭生命周期
├── chat.py            # 对话业务：SSE 帧编码 + 流式调用 Ollama + 落库 + 回滚
├── database.py        # 数据层：MySQL（引擎/ORM 模型/CRUD/TTL 任务）+ Redis（限流）
├── session_locks.py   # 会话锁字典 + 全局元锁
├── config.py          # 全部可调参数
├── requirements.txt
├── LICENSE
└── README.md
```

依赖方向是单向的，没有循环导入：

```
config ← session_locks ← database ← chat ← main
```

`main.py` 只管 HTTP 层；业务在 `chat.py`；所有库操作收在 `database.py`，业务代码不直接碰 `async_engine` 和 `redis_client`。

## 快速开始

**前提**：Ollama 已启动并拉过模型，MySQL 8 与 Redis 已运行。

```bash
# 1. 依赖
pip install -r requirements.txt

# 2. 建库（表由服务启动时自动创建，幂等）
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS rag_db DEFAULT CHARSET utf8mb4;"

# 3. 改 config.py 里的 DB_URL / REDIS_HOST，改成你自己的地址和账号

# 4. 启动
python main.py
# 或 python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

启动后可访问 `http://127.0.0.1:8000/docs` 查看交互式文档。

## 接口说明

### POST /chat_stream

流式对话。请求体 JSON：

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `prompt` | string | 是 | 1–4096 字符 | 本轮提问 |
| `session_id` | string | 是 | 1–100 字符 | 会话标识，历史按它分组 |
| `model` | string | 否 | 需在白名单内 | 默认 `qwen2.5:7b` |
| `temperature` | float | 否 | 0.0–2.0 | 默认 `0.7` |
| `system` | string | 否 | ≤2048 字符 | 系统提示词，插在历史最前面 |

```bash
curl -N -X POST http://127.0.0.1:8000/chat_stream \
  -H "Content-Type: application/json" \
  -d '{"prompt":"用一句话解释什么是SQL","session_id":"demo-001","model":"qwen2.5:7b"}'
```

响应 `Content-Type: text/event-stream`，每个 token 一帧，正常结束发送 `data: [DONE]`（下面省略了帧之间的空行，实际每帧以 `\n\n` 结束）：

```
data: SQL
data:  是
data: 结构化
data: 查询语言
data: [DONE]
```

多行内容的每一行都会单独加 `data:` 前缀（这是 SSE 规范要求），前端按行拼接即可还原换行。

**流内错误**：HTTP 状态码仍是 200，错误以特定前缀的帧返回，收到后应视为本轮失败：

| 帧内容 | 触发场景 |
|---|---|
| `data: [Ollama错误 404] ...` | Ollama 返回非 200，后面跟原始响应体 |
| `data: [连接失败] 无法连接到Ollama服务...` | 连不上 Ollama |
| `data: [服务异常] ...` | 流中抛异常，含 Ollama 中途返回 `{"error": "..."}` |

这三种都会先删掉刚落库的 user 消息。

### POST /clear

清空指定会话的消息、`session_meta` 记录和 Redis 限流 key。

```bash
curl -X POST "http://127.0.0.1:8000/clear?session_id=demo-001"
# {"msg": "会话 demo-001 上下文已经清空"}
```

`session_id` 是查询参数，不是 JSON 体。会话锁不删除，留待复用。

### 状态码

| 状态码 | 场景 | 返回体 |
|---|---|---|
| 400 | `model` 不在白名单 | `{"detail": "不支持的模型，可选：['qwen2.5:7b', 'deepseek-r1:8b']"}` |
| 429 | 同会话 3 秒内重复请求 | `{"detail": "请求过于频繁，请等待3.0秒后再试"}` |
| 422 | 请求体校验失败 | Pydantic 标准错误结构 |
| 200 | 正常流 / 流内错误 | 见上表 |

## 数据库表结构

服务启动时 `Base.metadata.create_all` 自动建表，不需要手动执行 DDL。

### messages

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | INT PK AUTO_INCREMENT | 自增主键，同时用作消息顺序 |
| `session_id` | VARCHAR(100) | 会话标识，建了单列索引 |
| `role` | VARCHAR(32) | `user` / `assistant` |
| `content` | TEXT | 消息正文 |
| `create_time` | FLOAT | Unix 时间戳（秒） |

### session_meta

| 字段 | 类型 | 说明 |
|---|---|---|
| `session_id` | VARCHAR(100) PK | 会话标识，主键 |
| `last_active_at` | FLOAT | 最近一次活跃时间，TTL 按它判断 |
| `create_time` | FLOAT | 首次活跃时间，后续 upsert 不覆盖 |

```sql
CREATE TABLE messages (
  id INT AUTO_INCREMENT PRIMARY KEY,
  session_id VARCHAR(100) NOT NULL,
  role VARCHAR(32) NOT NULL,
  content TEXT NOT NULL,
  create_time FLOAT NOT NULL,
  INDEX ix_messages_session_id (session_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE session_meta (
  session_id VARCHAR(100) PRIMARY KEY,
  last_active_at FLOAT NOT NULL,
  create_time FLOAT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## 关键设计

### 一轮问答的流程

`stream_generator()` 是整个对话流程的唯一入口：

1. 取该会话的锁，`async with lock` 进入临界区（同会话串行，避免两条请求交错读写历史）
2. 读最近 20 条历史 → upsert `session_meta.last_active_at` → 插入 user 消息并拿到自增 id
3. 有 `system` 时插到消息列表最前面，然后 POST 到 Ollama `/api/chat`
4. 边读边解析：按 `\n` 切行，`json.loads` 后取 `message.content`，非空才推给前端
5. 流结束，把完整回答作为 assistant 消息入库，再发 `[DONE]`

注意 history 是在插入当前 user 消息**之前**读的，之后才 `append` 当前提问，所以送给模型的列表不会重复。

### 会话锁：同会话串行，跨会话并行

`session_locks` 是个 `dict[str, asyncio.Lock]`，按需创建：

```python
async def get_session_lock(session_id: str) -> asyncio.Lock:
    async with GLOBAL_META_LOCK:
        if session_id not in session_locks:
            session_locks[session_id] = asyncio.Lock()
        return session_locks[session_id]
```

`GLOBAL_META_LOCK` 是 `asyncio.Lock`，只保护字典本身的增删，不保护数据库操作。不同会话拿到不同的锁，因此可以真正并行；同一会话的第二条请求会在 `async with lock` 处排队。

锁是进程内的，多 worker 部署时同一会话可能落到不同进程，串行保证会失效（限流走 Redis，不受影响）。

### TTL 清理

后台协程 `clean_expire_session()` 每 60 秒扫一次 `session_meta`，把 `last_active_at < now - 1800` 的会话挑出来，逐条加锁删除 `messages` 和 `session_meta`，最后把这些锁从字典里移除，避免锁字典随会话数无限增长。

```python
while True:
    await asyncio.sleep(CLEAN_INTERVAL)
    now = time.time()
    # 先查出所有过期会话
    expired_sids = (await db.execute(
        select(SessionMeta.session_id)
        .where(SessionMeta.last_active_at < now - SESSION_TTL)
    )).scalars().all()
    if expired_sids:
        for sid in expired_sids:
            lock = await get_session_lock(sid)   # 和正在进行的对话互斥
            async with lock:
                await delete_session_messages(sid)
        await drop_session_locks(expired_sids)
```

它由 `startup` 用 `asyncio.create_task` 拉起，`shutdown` 时 `cancel()` 并 `await`，所以不会出现 "Task was destroyed but it is pending"。

### 启停顺序

```
startup : init_redis() → init_db()（建表）→ create_task(clean_expire_session)
shutdown: cancel 清理任务 → dispose_db()（关连接池）→ close_redis()
```

顺序不能反：清理任务每轮都要查库，先 `dispose_db` 会让它拿到已关闭的连接；而 `dispose` 是异步的，必须放在事件循环还活着的时候执行。

### 限流

一个会话一个 key：`rate_limit:{session_id}`。`SET NX EX 3` 成功说明距上次请求已超过 3 秒，放行；失败说明 key 还在，返回 429。key 由 Redis 自动过期，不需要手动清理（`/clear` 会主动删掉，否则清空后立刻发言会被自己的限流拦住）。

## 可配置项

全部在 `config.py`：

| 常量 | 默认值 | 说明 |
|---|---|---|
| `ALLOWED_MODELS` | `["qwen2.5:7b", "deepseek-r1:8b"]` | 模型白名单，需与 `ollama list` 一致 |
| `SESSION_TTL` | `1800`（30 分钟） | 会话闲置多久算过期 |
| `CLEAN_INTERVAL` | `60` | 过期扫描周期（秒） |
| `RATE_LIMIT_SECONDS` | `3` | 同会话最小请求间隔 |
| `MAX_HISTORY_MESSAGES` | `20` | 每轮带给模型的历史条数上限 |
| `DB_URL` | `mysql+aiomysql://用户:密码@主机:3306/rag_db` | 数据库连接串 |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB` | `主机` / `6379` / `0` | 限流用 Redis 地址 |
| `OLLAMA_API` | `http://127.0.0.1:11434/api/chat` | Ollama 流式对话接口 |

连接池参数在 `database.py` 的 `create_async_engine()` 里：`pool_size=10`、`max_overflow=20`、`pool_pre_ping=True`（拿连接前先验活，自动淘汰被 MySQL 单方面断开的连接）、`echo=False`（改 `True` 可打印 SQL）。

## 常见问题

**Q：同一个 session 连续发两次，第二次返回 429？**
限流按会话说，间隔不足 3 秒就会被拦。前端应在收到 429 后按 `detail` 里的提示等待，或给发送按钮加冷却。

**Q：前端能收到数据但不逐字渲染，而是一整块出现？**
多半是中间层缓冲了响应。检查是否经过 Nginx（需 `proxy_buffering off`）、是否对 SSE 开了 gzip，以及前端是不是用 `fetch` 后一次性读了 `response.text()`——应改成逐块读 `response.body`。

**Q：重启服务后上下文没了？**
先确认 `session_meta` 里还有该会话且 `last_active_at` 没过期。闲置超过 30 分钟会被后台任务删掉，这是设计行为。另外确认客户端每次带的是同一个 `session_id`。

**Q：`/clear` 之后立刻发言还是 429？**
正常情况下不会——`reset_session` 会连限流 key 一起删。如果仍被拦，先确认两次请求用的是同一个 `session_id`；再看服务日志有没有 `清理限流key失败`（Redis 异常时这一步会被跳过，但库已经清掉了）。

**Q：报 `unknown command 'HELLO'`？**
`redis-py` 8.x 默认走 RESP3，连接时会先发 `HELLO`，而 Redis 服务端低于 6.0 不认识这个命令。升级服务端，或创建客户端时传 `protocol=2` 强制 RESP2。

**Q：Redis 挂了会怎样？**
限流是唯一的 Redis 用途，且没有进程内兜底。Redis 不可用时 `/chat_stream` 会在限流那一步抛异常返回 500，`/clear` 仍能清库、只打印一行“清理限流key失败”。

**Q：回答被截断，只出来一部分？**
历史只按条数截断（20 条），不按 token 计算。若单条内容很长，仍可能超出模型上下文窗口。可以调小 `MAX_HISTORY_MESSAGES` 或在前端限制输入长度。

**Q：client 中途断开（关页面、点停止）会怎样？**
Starlette 会取消生成协程，`CancelledError` 不属于 `Exception`，不会被捕获，因此已生成的部分不入库、刚插入的 user 消息会留在库里。这是当前版本的已知行为。

## 已知限制

**健壮性**

1. **Ollama 调用没有超时**：模型卡住（加载中、显存不足）时请求会一直挂着，同会话的后续请求也跟着排队。
2. **无 CORS 配置**：浏览器从其他端口的前端直连会被同源策略拦住，需自行加 `CORSMiddleware`。
3. **无鉴权**：知道 `session_id` 就能读写该会话，只适合内网/本机使用。
4. **空回复也会入库**：模型只返回结束帧时，会插入一条 `content=''` 的 assistant 记录。
5. **无全局异常处理**：未捕获异常返回纯文本 500，没有结构化错误体。
6. **日志只有 `print`**：无时间戳、无级别、无 `session_id`，多 worker 下输出会交错。
7. **限流只按 session**：换一个 `session_id` 即可绕过。
8. **回滚可能掩盖原始错误**：`except` 分支里的 `delete_message_by_id` 若也失败，新异常会顶掉原始异常。

**性能与可维护性**

9. **时间戳用 `FLOAT`**：只有约 7 位有效数字，10 位秒级时间戳会丢精度（实测约 ±1 分钟），对 30 分钟 TTL 够用，但时间展示和排序会失真；建议改 `DOUBLE` 或 `DATETIME(3)`。
10. **`messages` 只有单列索引**：`WHERE session_id=? ORDER BY id DESC LIMIT 20` 仍需 filesort，可加复合索引 `(session_id, id)`。
11. **会话锁在进程内存**：`uvicorn --workers N` 或多副本部署时，同会话串行失效。
12. **连接池未设 `pool_recycle`**：长时间空闲后靠 `pool_pre_ping` 兜底，可加 `pool_recycle=3600` 提前淘汰。
13. **`@app.on_event` 已废弃**：建议迁移到 `lifespan`。
14. **账号密码硬编码在 `config.py`**：公开仓库前应改读环境变量。
15. **无数据库迁移工具**：改表结构只能靠 `create_all`（它不会修改已有列），建议引入 Alembic。
16. **无健康检查接口**：无法给容器探针使用，可加 `GET /healthz`。
17. **SSE 无心跳**：长思考期间若中间设备有空闲超时，连接可能被掐断。

## License

[MIT](LICENSE)
