import asyncio
import threading

# 每个session_id独立异步锁，用来隔离同一个会话（session_id）的并发请求；会话过期清理时一并移除
session_locks = {}

# 全局元锁：保护 session_locks 的并发读写，保证同一时刻只有一个协程在操作它
GLOBAL_META_LOCK = threading.Lock()   # 线程锁


def get_session_lock(session_id: str) -> asyncio.Lock:
    """取某个会话的锁，没有就建一个
    """
    with GLOBAL_META_LOCK:
        if session_id not in session_locks:
            session_locks[session_id]=asyncio.Lock()
        return session_locks[session_id]


def drop_session_locks(session_ids) -> None:
    """清理过期会话时，把手里的锁一起删掉
    否则锁字典只增不减，会随会话数无限增长（内存泄漏）
    """
    with GLOBAL_META_LOCK:
        for sid in session_ids:
            session_locks.pop(sid, None)
