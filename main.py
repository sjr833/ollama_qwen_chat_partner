"""FastAPI 主逻辑：应用实例、请求模型、路由、启动/关闭生命周期
具体的数据存取看 database.py，对话业务流程看 chat.py，可调参数看 config.py
"""
import asyncio

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field

from chat import stream_generator
from config import ALLOWED_MODELS, RATE_LIMIT_SECONDS
from database import (check_rate_limit, clean_expire_session, close_redis, dispose_db, init_db,
                      init_redis, reset_session)

#  后台清理任务的引用：shutdown 时要能取消它，否则退出时会打印 Task was destroyed but it is pending
_clean_task: asyncio.Task|None = None

app = FastAPI()     #创建fastapi实例

# 请求体参数
class StreamReq(BaseModel):
    # 参数约束
    prompt: str = Field(..., min_length=1, max_length=4096, description="用户提问内容")
    model: str = Field(default="qwen2.5:7b", description="Ollama模型名")
    temperature: float = Field(default=0.7, ge=0.0, le=2.0, description="温度，0~2之间")
    system: str | None = Field(default=None, max_length=2048, description="系统提示词")
    session_id: str = Field(..., min_length=1, max_length=100, description="会话ID")


@app.on_event("startup")
async def startup():
    global _clean_task
    # 连接redis
    await init_redis()
    # 建表
    await init_db()
    # 启动过期清理任务
    _clean_task = asyncio.create_task(clean_expire_session())   # 保存任务引用，shutdown 时才能取消

@app.on_event("shutdown")
async def shutdown():
    # 关闭过期清理任务
    global _clean_task
    if _clean_task:
        _clean_task.cancel()    # 发一个取消信号，不会立刻杀死任务
        try:
            await _clean_task   # 等待任务执行完毕，完成退出|任务被取消时会抛异常，必须捕获
        except asyncio.CancelledError:
            pass                # 捕获并吃掉异常
        _clean_task = None
    # 关闭MySQL连接池
    await dispose_db()
    # 关闭Redis连接
    await close_redis()

@app.post("/chat_stream")
async def chat_stream(req: StreamReq):
    # 模型白名单校验
    if req.model not in ALLOWED_MODELS:
        raise HTTPException(status_code=400, detail=f"不支持的模型，可选：{ALLOWED_MODELS}")

    # 限流校验：同一session两次请求最小间隔（超限返回429）
    ok = await check_rate_limit(req.session_id)
    if not ok:
        return JSONResponse(status_code=429, content={"detail": f"请求过于频繁，请等待{RATE_LIMIT_SECONDS:.1f}秒后再试"})

    return StreamingResponse(
        stream_generator(req.prompt, req.model, req.temperature, req.system, req.session_id),
        media_type="text/event-stream"       # application/json一次性返回全部数据
                                             # text/event-stream长连接，服务端可以多次不断推送消息，前端持续吸收
    )

@app.post("/clear")
async def clear_context(session_id: str):
    # 清空消息、会话状态、限流 key 都在 reset_session 里做（内部按会话加锁）
    await reset_session(session_id)
    return {"msg": f"会话 {session_id} 上下文已经清空"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
