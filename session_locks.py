import asyncio

# 每个session_id独立异步锁，用来隔离同一个会话（session_id）的并发请求；会话过期清理时一并移除
session_locks = {}

# 全局元锁：保护 session_locks 的并发读写，保证同一时刻只有一个协程在操作它
GLOBAL_META_LOCK = asyncio.Lock()


def get_session_lock(session_id: str) -> asyncio.Lock:
    """取某个会话的锁，没有就建一个
    用 setdefault 取/建锁（与原 if not in + 赋值等价，但只有一行、且是原子操作）
    """
    return session_locks.setdefault(session_id, asyncio.Lock())


async def drop_session_locks(session_ids) -> None:
    """清理过期会话时，把手里的锁一起删掉
    否则锁字典只增不减，会随会话数无限增长（内存泄漏）
    """
    async with GLOBAL_META_LOCK:
        for sid in session_ids:
            session_locks.pop(sid, None)
