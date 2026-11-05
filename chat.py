"""对话业务：SSE 帧编码 + 流式调用 Ollama 并落库
只负责“一轮问答怎么跑”，不涉及 FastAPI 路由和生命周期
"""
import aiohttp
import json

from config import OLLAMA_API
from database import (delete_message_by_id, insert_message_to_db, load_messages_from_db,
                      update_session_active)
from session_locks import get_session_lock


# 把一段文本包装成合法的 SSE 帧
# 原实现是 yield f"data: {token}\n\n"，一旦 token 里带换行（模型几乎每条回答都有），
# 实际发出的是 "data: 第一行\n第二行\n\n"：第二行没有 data: 前缀，按 SSE 规范会被丢弃，
# 前端看到的换行就全没了；极端情况下还会把一帧拆成两帧。
# 这里按行拆开、每行都加 data: 前缀，客户端用 "\n" 拼接 data 行即可还原原始换行。
def sse_frame(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return "".join(f"data: {line}\n" for line in text.split("\n")) + "\n"


# ========== 流式对话生成器 ==========
async def stream_generator(prompt: str, model: str, temp: float | int, system: str | None, session_id: str):
    # [修改] 整个回合（读历史 -> 调模型 -> 落库）都放进该会话的锁里：
    #        同一会话的多个请求串行执行，不同会话之间各用各的锁、互不影响。
    #        这是原注释里“隔离同一个会话的并发请求”真正落地的地方。
    lock = get_session_lock(session_id)
    async with lock:
        # 加载历史上下文
        messages = await load_messages_from_db(session_id)
        # 更新会话活跃时间
        await update_session_active(session_id)
        user_msg_id = await insert_message_to_db(session_id, "user", prompt)
        messages.append({"role": "user", "content": prompt})
        if system:
            messages.insert(0, {"role": "system", "content": system})
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
                async with client.post(OLLAMA_API, json=payload) as resp:
                    if resp.status != 200:
                        err_text = await resp.text()
                        # 回滚：删掉刚插入的user消息
                        await delete_message_by_id(user_msg_id)
                        yield sse_frame(f"[Ollama错误 {resp.status}] {err_text}")
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
                            # 流中途 Ollama 也可能返回 {"error": "..."}，
                            # 原来直接取 data["message"]["content"] 会 KeyError，
                            # 用户只会看到一句没用的 [服务异常] KeyError: 'message'
                            if "error" in data:
                                raise RuntimeError(f"Ollama: {data['error']}")
                            token = data["message"]["content"]
                            if not token:      # 结束帧的 content 是空串，过滤掉，不再往前端发空的 data 帧
                                continue
                            assistant_full += token
                            yield sse_frame(token)
            # 流式结束，assistant完整回答入库
            await insert_message_to_db(session_id, "assistant", assistant_full)
            # 明确的结束标记：前端不用靠“连接断开”猜这一轮回答是否完整
            yield sse_frame("[DONE]")
        except aiohttp.ClientConnectionError:
            # 回滚：删掉刚插入的user消息
            await delete_message_by_id(user_msg_id)
            yield sse_frame("[连接失败] 无法连接到Ollama服务，请确认Ollama已启动")
        except Exception as e:
            print("流式异常", e)
            await delete_message_by_id(user_msg_id)
            yield sse_frame(f"[服务异常]{str(e)}")
