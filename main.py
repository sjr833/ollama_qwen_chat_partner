from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
import aiohttp
import json
import asyncio
import time
import uvicorn
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy import Column, Integer, String, Float, Text


app = FastAPI()     #创建fastapi实例


# 每个session_id独立异步锁（过期/清空不删除锁，锁复用）
session_locks = {}

# 记录每个session最后活跃时间戳（TTL用）
session_last_active: dict[str, float] = {}

# 记录每个session上次请求时间戳（限流用）
session_last_request: dict[str, float] = {}

# 全局元锁：保护上面两个字典并发读写
GLOBAL_META_LOCK = asyncio.Lock()

ALLOWED_MODELS = ["qwen2.5:7b"]  # 允许使用的模型白名单

# 配置
SESSION_TTL = 30 * 60       # 会话30分钟闲置过期
CLEAN_INTERVAL = 60         # 每60秒扫描一次过期会话
RATE_LIMIT_SECONDS = 3      # 同一session两次请求最小间隔（秒）


# ========== SQLAlchemy 异步MySQL配置 ==========
# 建表
## 创建异步引擎
DB_URL = "mysql+aiomysql://hello_qwen:123456@localhost:3306/ollama_qwen_chat_partner_messages?charset=utf8"
async_engine = create_async_engine(
    DB_URL,
    echo=False,   # echo=True 打印SQL，调试打开，上线关掉
    pool_pre_ping=True,  # 自动检测断开重连，MySQL很重要
    pool_size=10,  # 设置连接池活跃的连接数量
    max_overflow=20  # 允许额外的连接数
)

## 定义模型类:基类+表对应的模型类
### 基类，所有ORM模型继承它
class Base(DeclarativeBase):
    pass

### 异步会话工厂
AsyncSessionLocal = sessionmaker(
    bind=async_engine,  #绑定数据库引擎
    class_=AsyncSession,    #指定会话类
    expire_on_commit=False  #提交后会话不过期，不会重新查询数据库
)

# ========== ORM 消息模型 ==========
class Message(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100), nullable=False, index=True) # 加索引，按session查询快
    role = Column(String(32), nullable=False)
    content = Column(Text, nullable=False)
    create_time = Column(Float, nullable=False)




class StreamReq(BaseModel):
    # 参数约束
    prompt: str = Field(..., min_length=1, max_length=4096, description="用户提问内容")
    model: str = Field(default="qwen2.5:7b", description="Ollama模型名")
    temperature: float = Field(default=0.7, ge=0.0, le=2.0, description="温度，0~2之间")
    system: str | None = Field(default=None, max_length=2048, description="系统提示词")
    session_id: str = Field(..., min_length=1, max_length=100, description="会话ID")


# ========== 数据库相关函数 ==========
async def init_db():
    # 自动建表；已经手动建表的话，这个只是幂等，不会重复创建
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)



async def load_messages_from_db(session_id: str) -> list[dict]:
    """读取某个会话全部历史消息，按id升序"""
    async with AsyncSessionLocal() as db:
        # select 筛选，按id正序
        res = await db.execute(
            Message.__table__.select()
            .where(Message.session_id == session_id)
            .order_by(Message.id.asc())
        )
        rows = res.all()
        # 转成代码兼容的dict格式
        return [{"role": row.role, "content": row.content} for row in rows]



async def insert_message_to_db(session_id: str, role: str, content: str):
    """插入一条消息，返回自增id"""
    async with AsyncSessionLocal() as db:
        new_msg = Message(
            session_id=session_id,
            role=role,
            content=content,
            create_time=time.time()
        )
        db.add(new_msg)
        await db.commit()
        await db.refresh(new_msg) # 刷新，拿到数据库生成的id
        return new_msg.id



async def delete_message_by_id(msg_id: int):
    """根据id单条删除消息"""
    async with AsyncSessionLocal() as db:
        msg = await db.get(Message, msg_id)
        if msg:
            await db.delete(msg)
            await db.commit()




async def delete_session_messages(session_id: str):
    """删除一个session下全部消息（/clear和过期清理用）"""
    async with AsyncSessionLocal() as db:
        await db.execute(
            Message.__table__.delete().where(Message.session_id == session_id)
        )
        await db.commit()


# ========== 后台TTL清理任务 ==========
async def clean_expire_session():
    while True:
        await asyncio.sleep(CLEAN_INTERVAL)
        now = time.time()
        async with GLOBAL_META_LOCK:
            all_sids = list(session_last_active.keys())
            expired_sids = []
            for sid in all_sids:
                if now - session_last_active[sid] > SESSION_TTL:
                    expired_sids.append(sid)
        if expired_sids:
            # 循环调用ORM删除函数，替换原来sqlite原生sql
            for sid in expired_sids:
                await delete_session_messages(sid)
            # 清理内存字典
            for sid in expired_sids:
                session_last_active.pop(sid, None)
                session_last_request.pop(sid, None)

@app.on_event("startup")
async def startup():
    await init_db()
    asyncio.create_task(clean_expire_session())


# ========== 流式对话生成器 ==========
async def stream_generator(prompt: str, model: str, temp: float | int, system: str | None, session_id: str):
    if session_id not in session_locks:
        session_locks[session_id] = asyncio.Lock()
    lock = session_locks[session_id]

    async with lock:
        # 更新活跃时间（TTL计时）
        async with GLOBAL_META_LOCK:
            session_last_active[session_id] = time.time()

        # 从数据库读取该session历史消息
        messages = await load_messages_from_db(session_id)

        # system prompt临时拼在最前面，不入库
        if system is not None:
            if len(messages) == 0 or messages[0].get("role") != "system":
                messages.insert(0, {"role": "system", "content": system})

        # 用户消息入库，拿到这条消息id，出错时用来回滚
        user_msg_id = await insert_message_to_db(session_id, "user", prompt)
        messages.append({"role": "user", "content": prompt})

        ollama_api = "http://127.0.0.1:11434/api/chat"
        payload = {
            "model": model,
            "messages": messages,
            "stream": True,
            "options": {"temperature": temp}
        }

        assistant_full = ""
        buffer = b""
        try:
            async with aiohttp.ClientSession() as client:
                async with client.post(ollama_api, json=payload) as resp:
                    if resp.status != 200:
                        err_text = await resp.text()
                        # 回滚：删掉刚插入的user消息
                        await delete_message_by_id(user_msg_id)
                        yield f"data: [Ollama错误 {resp.status}] {err_text}\n\n"
                        return
                    async for chunk in resp.content.iter_chunked(1024):
                        if not chunk:
                            continue
                        buffer += chunk
                        while b"\n" in buffer:
                            line, buffer = buffer.split(b"\n", 1)
                            line = line.strip(b"\r\n")
                            if not line:
                                continue
                            line_str = line.decode("utf-8")
                            data = json.loads(line_str)
                            token = data["message"]["content"]
                            assistant_full += token
                            yield f"data: {token}\n\n"
            # 流式结束，assistant完整回答入库
            await insert_message_to_db(session_id, "assistant", assistant_full)
        except aiohttp.ClientConnectionError:
            # 回滚：删掉刚插入的user消息
            await delete_message_by_id(user_msg_id)
            yield "data: [连接失败] 无法连接到Ollama服务，请确认Ollama已启动\n\n"
        except Exception as e:
            # 回滚：删掉刚插入的user消息
            await delete_message_by_id(user_msg_id)
            yield f"data: [服务器错误] {str(e)}\n\n"


@app.post("/chat_stream")
async def chat_stream(req: StreamReq):
    # 模型白名单校验
    if req.model not in ALLOWED_MODELS:
        raise HTTPException(status_code=400, detail=f"不支持的模型，可选：{ALLOWED_MODELS}")

    # 限流：同一session两次请求最小间隔
    now = time.time()
    async with GLOBAL_META_LOCK:
        last_req = session_last_request.get(req.session_id, 0)      # 如果不存在session_id，返回0
        if now - last_req < RATE_LIMIT_SECONDS:
            wait = RATE_LIMIT_SECONDS - (now - last_req)
            return JSONResponse(
                status_code=429,
                content={"detail": f"请求过于频繁，请{wait:.1f}秒后再试"}
            )
        session_last_request[req.session_id] = now

    return StreamingResponse(
        stream_generator(req.prompt, req.model, req.temperature, req.system, req.session_id),
        media_type="text/event-stream"       # application/json一次性返回全部数据
                                             # text/event-stream长连接，服务端可以多次不断推送消息，前端持续吸收
    )


@app.post("/clear")
async def clear_context(session_id: str):
    if session_id not in session_locks:
        session_locks[session_id] = asyncio.Lock()
    lock = session_locks[session_id]

    async with lock:        # 防止同一个会话同时读写和删除全局变量
        async with GLOBAL_META_LOCK:   # 防止不同会话同时读写session_last_active
            session_last_active[session_id] = time.time()
        await delete_session_messages(session_id)
    return {"msg": f"会话 {session_id} 上下文已经清空"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
