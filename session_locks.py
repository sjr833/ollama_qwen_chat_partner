import asyncio

# 每个session_id独立异步锁，用来隔离同一个会话（session_id）的并发请求；会话过期清理时一并移除
session_locks = {}

# 全局元锁：保护 session_locks 的并发读写，保证同一时刻只有一个协程在操作它
GLOBAL_META_LOCK = asyncio.Lock()

# 说明
"""
    数据库的读写删除操作只需要隔离同一个session_id
    不同session_id的数据是隔离的
    因此数据库的读写删除操作无需再上一个全局锁
"""
"""
    调用异步函数->生成协程对象，但代码不跑->await/asyncio.create_task(...)才跑，拿到return的值/asyncio.create_task()
"""
async def get_session_lock(session_id: str) -> asyncio.Lock:
    """
        取某个会话的锁，没有就建一个
    """
    async with GLOBAL_META_LOCK:
        if session_id not in session_locks:
            session_locks[session_id]=asyncio.Lock()
        return session_locks[session_id]


async def drop_session_locks(session_ids) -> None:
    """
        清理过期会话时，把手里的锁一起删掉
        否则锁字典只增不减，会随会话数无限增长（内存泄漏）
    """
    async with GLOBAL_META_LOCK:
        for sid in session_ids:
            session_locks.pop(sid, None)
