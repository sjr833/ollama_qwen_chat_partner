# ollama_qwen_chat_partner

基于 **FastAPI + Ollama(Qwen2.5) + MySQL** 的本地大模型多轮对话后端服务。

对外提供两个接口：`/chat_stream` 逐字返回 SSE 流式回答，`/clear` 清空指定会话。会话历史按 `session_id` 落库到 MySQL，支持多会话并发隔离、闲置自动过期、请求限流与失败回滚。

---

## 功能特性

- **流式输出**：对接 Ollama `/api/chat`，边生成边推送（`text/event-stream`），前端可逐字渲染
- **多会话隔离**：每个 `session_id` 一把独立异步锁，同一会话串行、不同会话并行
- **历史持久化**：消息存 MySQL（SQLAlchemy 2.0 异步 ORM），重启不丢上下文
- **上下文自动过期**：会话闲置 30 分钟自动清理（后台协程每 60 秒扫描）
- **请求限流**：同一 `session_id` 两次请求至少间隔 3 秒，超限返回 429（Redis 原子 `SET NX EX`）
- **模型白名单**：只允许 `ALLOWED_MODELS` 中的模型，防止调用未下载/未授权模型
- **失败回滚**：Ollama 返回错误、连不上 Ollama 或流中抛异常时，自动删除刚落库的 user 消息，保证历史干净

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
| MySQL 8 | 会话消息存储（8.0+） |
| Redis | 请求限流（`SET NX EX` 原子操作），需 6.0+ |

Python 版本要求 **3.10+**（代码使用了 `str | None` 语法）。

---

## 目录结构

```
ollama_qwen_chat_partner/
├── main.py            # FastAPI 主逻辑：应用实例、请求模型、路由、启动/关闭生命周期
├── chat.py            # 对话业务：SSE 帧编码 + 流式调用 Ollama 并落库
├── database.py        # 数据层：MySQL（ORM 模型 + 增删查）与 Redis（限流 key）都在这里
├── session_locks.py   # 会话级异步锁 + 保护锁字典的全局元锁
├── config.py          # 全局配置：MySQL/Redis/Ollama 连接参数、TTL、限流、历史条数、模型白名单
├── requirements.txt   # 依赖清单（pip install -r requirements.txt）
├── README.md
├── LICENSE
└── .gitignore
```

分层原则：`main.py` 只保留 FastAPI 相关的东西（路由、请求模型、生命周期），业务与存储细节下沉；
`database.py` 是唯一碰 MySQL 引擎和 Redis 客户端的文件，其他模块只调用它暴露的函数。

---

## 快速开始

### 前置：数据库放在哪

MySQL / Redis 装在哪都行，只要 `config.py` 里的地址连得通。本项目开发时两套数据库都装在 **WSL2 的 Ubuntu 22.04** 里，
从 Windows 侧访问踩过几个坑，记在这里备查；数据库装在本机或远程服务器上的话可以跳过本节。

| 组件 | 关键配置 |
|---|---|
| MySQL | `bind_address=0.0.0.0`（配置在 `/etc/mysql/mysql.conf.d/mysqld.cnf`，改完 `sudo service mysql restart`）；账号 host 要允许外部来源，`root@%` 能连，`root@localhost` 只接受本机连接 |
| Redis | `bind 0.0.0.0`、`protected-mode no`，否则非回环来源会被拒绝 |

WSL2 虚拟网卡的地址（`172.19.x.x`）**每次重启可能变**，`config.py` 里写死它就得跟着改。
更稳的写法是 `127.0.0.1`：WSL2 默认开了 localhost 转发，Windows 侧连 `127.0.0.1:3306` / `127.0.0.1:6379`
会直接转到 Ubuntu 里的服务，服务本身跑在 Windows 或 WSL 里都成立。查当前 WSL IP：

```powershell
wsl hostname -I
```

WSL 里的服务不会开机自启（Ubuntu 默认没开 systemd），每次开工先在 Ubuntu 终端里拉起来：

```bash
sudo service mysql start
sudo service redis-server start
sudo service mysql status && redis-cli ping
```

### 1. 准备 Ollama

```bash
# 安装后拉取模型（首次约 4.7GB）
ollama pull qwen2.5:7b
ollama serve
```

`config.py` 的白名单里还有 `deepseek-r1:8b`，要用它得先 `ollama pull deepseek-r1:8b`，
否则请求会走到 Ollama 报 404（`model not found`）。白名单只做名字校验，不检查模型是否已下载。

确认服务在 `http://127.0.0.1:11434` 可用：

```bash
curl http://127.0.0.1:11434/api/tags
```

### 2. 准备 MySQL

```sql
CREATE DATABASE rag_db
  DEFAULT CHARSET utf8mb4
  COLLATE utf8mb4_unicode_ci;

-- 可选：建专用账号。config.py 当前用的是 root/123456，直接用 root 就跳过这两句，
-- 用专用账号则要同步改 DB_URL，否则会 Access denied
CREATE USER 'hello_qwen'@'%' IDENTIFIED BY '123456';
GRANT ALL PRIVILEGES ON rag_db.* TO 'hello_qwen'@'%';
FLUSH PRIVILEGES;
```

> 库名、账号、地址都要和 `config.py` 里的 `DB_URL` 对上（默认库 `rag_db`，账号 `root`）。
> 表不用手动建，服务启动时会执行 `Base.metadata.create_all` 自动建表（幂等）。
>
> 装在 WSL 里的话，`sudo mysql` 可以直接免密进入（root 走 socket 认证）；从 Windows 侧连用
> `mysql -h 127.0.0.1 -u root -p`。连不上时回头看上面「数据库放在哪」的 `bind-address` 和账号 host 两条。

### 3. 准备 Redis

限流用 Redis 的 `SET NX EX` 做原子占位，在装 Redis 的那台机器上执行（WSL 里先 `wsl` 进去）：

```bash
sudo service redis-server start     # 走 /etc/redis/redis.conf；别直接敲 redis-server（前台、不读配置）
redis-cli ping                      # 期望返回 PONG
```

> **Redis 必须是 6.0 及以上**：`redis-py` 8.x 默认走 RESP3，连接时会先发 `HELLO` 命令，
> 5.x 及更早的服务端会直接报 `unknown command 'HELLO'`。
> 本项目在 6.0.16 上验证通过，代码里没有传 `protocol=2`；服务端确实偏低时的处理见 FAQ。
> 连接参数在 `config.py` 的 `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB`。
>
> 想让 Windows 侧用 WSL 的 IP 连进来，`/etc/redis/redis.conf` 里要 `bind 0.0.0.0` 且 `protected-mode no`。
> 只 bind `127.0.0.1` 则只有 Ubuntu 本机能连；`protected-mode yes` 且没设密码时，非回环来源会被拒，
> 报 `DENIED Redis is running in protected mode`。
>
> Redis 是必需依赖：限流只走 Redis，不做进程内兜底，Redis 不可用时请求会直接报错。
> 多进程/多实例部署同样依赖 Redis 共享限流状态，各进程才能统一计数。

### 4. 安装依赖

```bash
pip install -r requirements.txt
# 或者手动装：
pip install fastapi uvicorn[standard] aiohttp "SQLAlchemy[asyncio]" aiomysql cryptography pydantic redis
```

几个容易被漏掉、但缺了就跑不起来的包：

- `aiomysql`：异步 MySQL 驱动。代码里没有 `import aiomysql`，是 `DB_URL` 里的 `mysql+aiomysql` 让 SQLAlchemy 去加载它，
  缺了在 `create_async_engine()` 那一刻就抛 `ModuleNotFoundError`，服务起不来
- `cryptography`：账号用 `caching_sha2_password` / `sha256_password` 认证时才需要（PyMySQL 用它做 RSA 加密），
  缺了报 `RuntimeError: 'cryptography' package is required`。PyMySQL 只在 `[rsa]` extra 里带它，所以这里显式列上
- `SQLAlchemy[asyncio]` 会带 `greenlet`，异步会话依赖它
- `pymysql` 由 `aiomysql` 自动带入（`aiomysql` 0.3+ 声明了 `PyMySQL>=1.0`），不用单独写

### 5. 配置数据库连接

修改 `config.py` 中的连接参数（当前值如下）：

```python
DB_URL = "mysql+aiomysql://root:123456@172.19.66.169:3306/rag_db"
REDIS_HOST = "172.19.66.169"
REDIS_PORT = 6379
REDIS_DB = 0
```

格式：`mysql+aiomysql://用户名:密码@主机:端口/库名?charset=utf8mb4`

如果库装在 WSL 里，上面这个 IP 会随 WSL 重启变化，换成 `127.0.0.1` 更省事（WSL2 的 localhost 转发对两个库都生效）：

```python
DB_URL = "mysql+aiomysql://root:123456@127.0.0.1:3306/rag_db"
REDIS_HOST = "127.0.0.1"
```

连接串没有带 `charset`，aiomysql 默认就是 `utf8mb4`，中文和 emoji 不需要额外配置。

> ⚠️ 账号密码是硬编码在 `config.py` 里的，放到公开仓库前建议改成读环境变量
> （`os.getenv("DB_URL", ...)`），或只提交 `config.example.py`。

### 6. 启动服务

```bash
python main.py
# 或
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

启动成功后可访问交互式文档：<http://127.0.0.1:8000/docs>

> `python main.py` 内部是 `uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)`，
> 改了代码会自动重启；不想开热重载就直接用下面那条命令（去掉 `--reload`）。

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

响应为 SSE 流，每个 `data:` 帧是一段增量文本，最后以 `data: [DONE]` 结束：

```
data: 闭包

data: 是指

data: 函数与其

data: [DONE]
```

> 文本里的换行会拆成多条 `data:` 行放在同一个事件里（`data: 第一行\ndata: 第二行\n\n`），
> 客户端把同一事件的 data 行用 `\n` 拼起来即可还原原始换行。这是 SSE 规范要求的转义方式，
> 直接把带换行的文本塞进一个 `data:` 会丢换行。

前端消费（浏览器，按 SSE 规范解析事件）：

```javascript
const resp = await fetch("http://127.0.0.1:8000/chat_stream", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ prompt: "你好", session_id: "demo-001" }),
});
const reader = resp.body.getReader();
const decoder = new TextDecoder();
let buf = "";

while (true) {
  const { done, value } = await reader.read();
  if (done) break;
  buf += decoder.decode(value, { stream: true });

  // SSE 以空行分隔事件，缓冲到完整的空行再处理，避免把半截帧切坏
  let idx;
  while ((idx = buf.indexOf("\n\n")) !== -1) {
    const block = buf.slice(0, idx);
    buf = buf.slice(idx + 2);

    const text = block
      .split("\n")
      .filter((l) => l.startsWith("data: "))
      .map((l) => l.slice(6))
      .join("\n");           // 同一事件的多行 data 用换行拼回

    if (text === "[DONE]") return;        // 流正常结束
    if (/^\[(Ollama错误|连接失败|服务异常)/.test(text)) {  // 流内错误
      console.error(text);
      return;
    }
    document.body.append(text);           // 逐字追加
  }
}
```

错误码：

| 状态码 | 场景 | 返回 |
|---|---|---|
| 400 | `model` 不在白名单 | `{"detail": "不支持的模型，可选：['qwen2.5:7b', 'deepseek-r1:8b']"}` |
| 429 | 同一会话 3 秒内重复请求 | `{"detail": "请求过于频繁，请等待3.0秒后再试"}` |
| 422 | 请求体校验失败（缺字段/超长/越界） | Pydantic 标准错误结构 |
| 500 | Redis 不可用（限流失败）或数据库异常 | 纯文本 `Internal Server Error` |
| 200 + 流内错误 | Ollama 返回非 200 | `data: [Ollama错误 404] ...` |
| 200 + 流内错误 | 连不上 Ollama | `data: [连接失败] 无法连接到Ollama服务，请确认Ollama已启动` |
| 200 + 流内错误 | 其他异常（含流内 `error` 帧） | `data: [服务异常]<异常信息>` |

> 注意：Ollama 侧的错误是在流开始后才可能出现的，无法用 HTTP 状态码表达，只能以 `data:` 文本下发，前端需按前缀识别。

### 2. 清空会话上下文

`POST /clear?session_id=xxx`

```bash
curl -X POST "http://127.0.0.1:8000/clear?session_id=demo-001"
# {"msg": "会话 demo-001 上下文已经清空"}
```

删除该会话的消息记录和 `session_meta` 记录，同时清掉 Redis 里的限流 key（否则清空后立刻发消息会被 429 拦住）。
会话锁不删除，留待复用。

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

`session_meta` 表（只存会话活跃时间，TTL 清理靠它判断过期）：

| 字段 | 类型 | 说明 |
|---|---|---|
| `session_id` | VARCHAR(100) **PK** | 会话标识，一个会话一行 |
| `last_active_at` | FLOAT NOT NULL | 最近一次对话的 Unix 时间戳，每轮问答都会被 upsert 刷新 |
| `create_time` | FLOAT NOT NULL | 首次写入时间，后续更新不会覆盖 |

```sql
CREATE TABLE session_meta (
  session_id VARCHAR(100) NOT NULL PRIMARY KEY,
  last_active_at FLOAT NOT NULL,
  create_time FLOAT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

> 写入用 `mysql_insert(...).on_duplicate_key_update(last_active_at=...)`（`database.update_session_active`），
> 一条 SQL 完成「没有就插入、有就只刷新活跃时间」，不用先查再判。
> `/clear` 会同时删掉该会话在两张表里的记录，所以清空后 `session_meta` 里也不再有它的行。

---

## 关键设计说明

### 会话级串行，会话间并行

```
请求 A(session=1) ─┐
请求 B(session=1) ─┴─ 同一把 lock → 串行，避免历史读写交错
请求 C(session=2) ─── 另一把 lock → 与 A 并行
```

没有这把锁时，两个并发请求会读到相同的历史、各自追加 user 消息，导致上下文错乱。

`GLOBAL_META_LOCK`（`threading.Lock`）保护的是 `session_locks` 字典的增删：请求路径要取锁、后台清理任务要删锁，
两者必须互斥，否则可能一边删除一边被重新创建，导致锁字典泄漏。字典操作是纯内存、无 `await`，用线程锁即可。

### 消息入库时机

- **user 消息**：请求 Ollama 之前就落库，拿到自增 id 作为回滚凭据
- **assistant 消息**：整段流式结束后一次性落库（避免每个 token 写一次库）
- **失败回滚**：各异常分支（Ollama 非 200 / 连不上 / 流内 error 帧 / 其他异常）都会调用 `delete_message_by_id(user_msg_id)`，防止出现"只有提问没有回答"的孤儿消息
- **客户端断连不补存**：连接断开时 Starlette 直接取消生成器，已生成内容不落库、user 消息保留（见 FAQ「客户端中途断开」）

### TTL 清理

后台协程每 60 秒扫一遍 `session_meta.last_active_at`，超过 30 分钟未活跃的 `session_id` 会被删除消息记录，并从 `session_locks` 里移除对应的锁。检查与删除分两段执行（先收集、后删除），避免长时间持锁阻塞请求。

> 注意：TTL 只处理 `session_meta` 里有记录的会话。如果某条消息所属的 `session_id` 从未调用过 `update_session_active()`（正常流程不会出现），它不会被清理。

### 启动与关闭顺序

`startup`：连 Redis → `init_db()` 建表 → 起 TTL 清理后台任务（引用存在 `main._clean_task`，否则退出时会报 `Task was destroyed but it is pending`）

`shutdown`：取消并 `await` TTL 任务 → `dispose_db()` 关 MySQL 连接池 → `close_redis()`

顺序不能反：TTL 任务每轮都要开数据库会话，先 dispose 会让它拿到失效连接报错；
两个关闭函数都是协程，必须在事件循环还活着时 `await`，放 `atexit` / `__del__` 里就来不及了。

---

## 可配置项

`config.py` 顶部常量（`async_engine` 的 `echo` 在 `database.py`）：

| 常量 | 默认值 | 说明 |
|---|---|---|
| `ALLOWED_MODELS` | `["qwen2.5:7b", "deepseek-r1:8b"]` | 模型白名单，需与 `ollama list` 一致 |
| `SESSION_TTL` | `30 * 60` | 会话闲置过期时间（秒） |
| `CLEAN_INTERVAL` | `60` | 过期扫描周期（秒） |
| `RATE_LIMIT_SECONDS` | `3` | 同会话最小请求间隔（秒） |
| `MAX_HISTORY_MESSAGES` | `20` | 每次送给模型的历史消息条数上限（只取最近 N 条，防止超上下文） |
| `DB_URL` | `mysql+aiomysql://root:123456@172.19.66.169:3306/rag_db` | 数据库连接串 |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB` | `172.19.66.169` / `6379` / `0` | 限流用的 Redis 地址 |
| `OLLAMA_API` | `http://127.0.0.1:11434/api/chat` | Ollama 流式对话接口地址 |
| `async_engine(echo=)` | `False` | 改 `True` 可打印所有 SQL，调试用 |
| `async_engine(pool_size/max_overflow)` | `10` / `20` | 连接池常驻连接数与最大溢出数 |

---

## 常见问题

**Q：报 `Can't connect to MySQL server on ...` / `Connection refused`**
先确认服务在跑（`sudo service mysql status`，没起就 `sudo service mysql start`），再确认 `config.py` 里的地址端口对得上。
如果库装在 WSL 里，再多查两条：`wsl hostname -I` 看 IP 是不是变了（或者干脆换成 `127.0.0.1`），
以及 `mysqld.cnf` 里的 `bind-address` 是不是被改回了 `127.0.0.1`。

**Q：Redis 连不上 / 报 `DENIED Redis is running in protected mode`**
先 `sudo service redis-server start`；再确认 `/etc/redis/redis.conf` 是 `bind 0.0.0.0` + `protected-mode no`
（用 WSL IP 连时，`protected-mode yes` 且无密码会直接拒绝非回环来源）。Ubuntu 本机 `redis-cli ping` 应返回 `PONG`。

**Q：报 `'cryptography' package is required`**
账号用的是 `caching_sha2_password` / `sha256_password` 认证，装一下即可：`pip install cryptography`。

**Q：报 `Access denied for user 'root'`**
账号或密码错，或该账号没有目标库权限。注意 `DB_URL` 里的账号要和实际授权的一致——
README 建库示例里给了 `hello_qwen`，但 `config.py` 目前写的是 `root`，两者不一致就会报这个错。

**Q：回答里中文乱码**
连接串不必带 `charset`（aiomysql 默认 `utf8mb4`），乱码一般出在库或表的字符集上：
`SHOW CREATE TABLE messages` 看是不是 `utf8mb4`，不是的话用 `ALTER TABLE ... CONVERT TO CHARACTER SET utf8mb4` 修。

**Q：`/chat_stream` 返回 429**
同一 `session_id` 3 秒内重复请求，属正常限流；换 `session_id` 或等待即可。

**Q：流式返回的内容被一次性打印，不"流"**
客户端或 curl 做了缓冲。curl 用 `-N`；Nginx 反代需 `proxy_buffering off;`。

**Q：`--reload` 模式下启动日志出现两次**
热重载会重开进程，`startup` 事件因此执行两次；生产环境不要开 `reload`。

**Q：上下文突然消失**
闲置超过 30 分钟被 TTL 清理了，或调过 `/clear`。两者都会删库记录且不可恢复。

**Q：启动或请求时报 `redis.exceptions.ResponseError: unknown command 'HELLO'`**
Redis 服务端版本低于 6.0（Windows 上一些集成环境自带的就是 3.0.x），不支持 `HELLO` 命令，
而 redis-py 8.x 默认走 RESP3，连接时会先发 `HELLO` 握手。
两种解法：把服务端升级到 6.0+（推荐），或在 `database.init_redis()` 里改成
`Redis(host=..., port=..., db=..., protocol=2)` 强制走 RESP2。

**Q：请求时报 Redis 连接错误**
限流只走 Redis，不做进程内兜底，Redis 不可用时 `/chat_stream` 会返回 500。
确认 Redis 已启动、`config.py` 里的 `REDIS_HOST` / `REDIS_PORT` 可达即可。

**Q：客户端中途断开（关页面、点停止生成），历史里会留下"只有问没有答"的记录吗**
会。当前没有做断连补存：连接断开后 Starlette 会取消流式生成器，已经生成的内容不会落库，
刚插入的 user 消息也会原样保留。需要时再补这块逻辑。

**Q：历史太长的老会话，模型好像"忘了"早期内容**
`MAX_HISTORY_MESSAGES = 20` 只把最近 20 条消息送给模型，更早的仍在库里但不再参与推理，
这是为了避免超过模型上下文窗口。需要更长记忆就调大该值，或后续接摘要/RAG。

---

## 已知限制

以下为当前版本尚未处理的问题，都是实测确认过的行为，按优先级排列。

### 健壮性

| # | 问题 | 影响 | 建议 |
|---|---|---|---|
| 1 | 调用 Ollama 没有超时 | `aiohttp.ClientSession()` / `client.post()` 都未设 timeout。Ollama 卡住（模型加载、显存不足、进程假死）时请求永不返回，连接和会话锁一直被占用，同 `session_id` 的后续请求全部排队阻塞 | 加 `aiohttp.ClientTimeout(total=..., sock_read=...)`，超时后下发 `[服务异常]` 并回滚 user 消息 |
| 2 | 没有 CORS 配置 | 浏览器从其他端口的前端调用会被同源策略拦截，预检请求返回 405 且无 `Access-Control-Allow-Origin` | 加 `CORSMiddleware`，`allow_origins` 收敛到实际前端域名 |
| 3 | 模型空回复也入库 | 模型只返回结束帧（无 content）时，`assistant_full` 为空串，仍会插入一条 `content=''` 的 assistant 记录 | 入库前判空，空回复不写或写占位说明 |
| 4 | 时间戳用 `FLOAT` 存 | `FLOAT` 只有约 7 位有效数字，实测 `1794108149.137` 读回 `1794110000.0`。SQL 侧比较的实际误差约 ±1 分钟，对 30 分钟 TTL 勉强够用，但时间展示、按时间排序/统计会失真 | 改成 `DOUBLE` 或 `DATETIME(3)`；改类型需要手动 `ALTER TABLE` 或 Alembic 迁移 |
| 5 | 无任何鉴权 | 任何人知道 `session_id` 就能读历史（间接）、清空、并发起对话；`/clear` 是 `POST` + 查询参数，可被随意批量调用 | 加 API Key / JWT；`session_id` 与用户绑定后再校验归属 |
| 6 | 无全局异常处理 | 未捕获异常直接 500，返回纯文本 `Internal Server Error`，前端拿不到结构化错误；也不会记录 traceback | 注册 `@app.exception_handler(Exception)`，统一 JSON 错误体 + 日志 |
| 7 | 日志只有 `print` | 无时间戳、无级别、无请求上下文，多 worker 下输出交错，线上无法检索 | 换 `logging`（或 loguru），关键路径加 `session_id` |
| 8 | 限流只按 `session_id` | 换一个 `session_id` 即可绕过；也无法防单 IP 刷接口 | 叠加按 IP / 全局并发令牌桶限流 |
| 9 | 回滚里的二次异常会掩盖原始错误 | `except` 分支再次调用 `delete_message_by_id`，若 DB 同时不可用，会抛新异常顶掉原始异常 | 回滚操作单独 try/except 并记录日志 |

### 性能与可维护性

| # | 问题 | 建议 |
|---|---|---|
| 10 | `messages` 只有 `session_id` 单列索引，`WHERE session_id=? ORDER BY id DESC LIMIT 20` 需要 filesort | 加复合索引 `(session_id, id)` |
| 11 | 连接池未设 `pool_recycle` | 加 `pool_recycle=3600`，比只靠 `pool_pre_ping` 更早淘汰旧连接 |
| 12 | `@app.on_event("startup"/"shutdown")` 已废弃 | 迁移到 `lifespan` 异步上下文管理器 |
| 13 | 账号密码硬编码在 `config.py` | 改读环境变量 / `.env`，并把 `config.py` 从版本库排除或只提交 `config.example.py` |
| 14 | 无数据库迁移工具 | 表结构随 ORM 改，线上只能靠 `create_all`（它不会改已有列），建议引入 Alembic |
| 15 | 历史按“条数”截断，不按 token | 20 条 × 4096 字符理论上可超模型上下文窗口，导致 400 或回答质量下降；建议按 token 预算截断，或超长时做摘要 |
| 16 | `assistant_full` 无上限 | 模型异常长输出会持续占用内存，建议设最大长度，超出即中断并提示 |
| 17 | 无健康检查接口 | 无法给负载均衡/容器探针用，建议加 `GET /healthz`（探测 MySQL + Redis） |
| 18 | SSE 无心跳 | 长思考期间若中间设备有空闲超时，连接会被掐断，建议定期发送 `: ping` 注释帧 |

---

## 功能验证记录

2026-11-08 在真实环境（MySQL 8.0 + Redis 6.0 + Ollama `qwen2.5:7b`）逐项验证，测试会话统一用 `zz_` 前缀，跑完已清理：

| 能力 | 结果 | 说明 |
|---|---|---|
| 流式输出 | ✅ | 200，多帧增量，`[DONE]` 结尾，7 秒内返回完整回答 |
| 多轮上下文拼接 | ✅ | 抓取下游请求体确认顺序为 `system → 历史 user/assistant → 当前 user` |
| 同会话串行 | ✅ | 并发两个请求，下游最大并发为 1，消息成对写入不交错 |
| 跨会话并行 | ✅ | 两个会话并发时下游最大并发为 2 |
| 历史截断 | ✅ | 写入 25 条后只返回最近 20 条，且保持旧→新 |
| 请求限流 | ✅ | 同会话并发两请求，恰好一个 200、一个 429 |
| 模型白名单 | ✅ | 非白名单模型返回 400 |
| 参数校验 | ✅ | 空 prompt / 超 4096 / temperature 越界 / 空 session_id 均 422 |
| 失败回滚 | ✅ | Ollama 500、连接失败、流内 `error` 帧三种情况均下发错误帧且删除 user 消息 |
| TTL 清理 | ✅ | 造过期会话后 2 秒内被删除，会话锁同步移除 |
| upsert 幂等 | ✅ | 反复更新只保留一行，`create_time` 不被覆盖 |
| SSE 转义 | ✅ | 多行文本逐行加 `data:`，`\r\n` 统一为 `\n` |
| 中文 / emoji | ✅ | 无乱码 |
| SQL 注入 | ✅ | `session_id` 带 `' OR '1'='1` 正常处理（ORM 参数化） |
| 客户端断连 | ✅ | 服务不崩、连接释放，后续请求正常（但半截回答不落库，见 FAQ） |
| `/clear` | ✅ | 200，消息、`session_meta`、限流 key 一并清空，清空后立即发言不再 429；12 个并发请求全部 200、无死锁 |
| CORS | ❌ | 无 CORS 头，见「已知限制」第 2 条 |

---

## Roadmap

- [ ] Ollama 调用加超时 + 空回复不入库 + 全局异常处理
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
