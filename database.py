"""
    数据层：MySQL（ORM 模型 + 增删查）和 Redis（限流 key）都放在这个文件里
    业务代码只调用这里的函数，不直接碰 engine / redis_client
"""
import asyncio
import time

from redis.asyncio import Redis
from sqlalchemy import Column, Integer, String, Float, Text, select, false, delete
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine,async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from config import (CLEAN_INTERVAL, DB_URL, MAX_HISTORY_MESSAGES, RATE_LIMIT_SECONDS,
                    REDIS_DB, REDIS_HOST, REDIS_PORT, SESSION_TTL)
from session_locks import drop_session_locks, get_session_lock


# ========== SQLAlchemy 异步MySQL配置 ==========
# 创建异步引擎 + 初始化连接池   整个服务运行期间一直存在除非手动关闭
async_engine = create_async_engine(
    DB_URL,
    echo=False,          # echo=True 打印SQL，调试打开，上线关掉
    pool_pre_ping=True,  # 自动检测断开重连，MySQL很重要，如果一条连接空闲太久，一直没发SQL，MySQL 会主动把这条连接断开、扔掉。
                         # 但是！SQLAlchemy 的连接池自己不知道这件事。池子里面还存着这条连接，标记它是“可用”的。
                         # pool_pre_ping=True = 每次拿连接先验活，提前干掉池子里已经被MySQL单方面踢掉的“僵尸连接”
    pool_size=10,        # 设置连接池活跃的连接数量
    max_overflow=20      # 允许额外的连接数
)

# 基类，所有ORM模型继承它
class Base(DeclarativeBase):
    pass

# ========== ORM 消息模型 ==========
# 存储聊天记录
class Message(Base):
    __tablename__ = "messages"  # 表名
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100), nullable=False, index=True) # 加索引，按session查询快
    role = Column(String(32), nullable=False)
    content = Column(Text, nullable=False)
    create_time = Column(Float, nullable=False)

# 存储会话状态
class SessionMeta(Base):
    __tablename__ = "session_meta"
    session_id = Column(String(100), primary_key=True)
    last_active_at = Column(Float, nullable=False)
    create_time = Column(Float, nullable=False)


# 异步会话工厂
AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,  # 绑定数据库引擎
    class_=AsyncSession,  # 指定为异步会话类
    expire_on_commit=False  # 提交后会话不过期，不会重新查询数据库
)
""" 代码解释
    msg = await db.get(Message, 1)
    print(msg.content)

    msg.content = "新的聊天内容"
    await db.commit()

    # ✨commit之后，msg对象【不过期】，内存里还保留刚才修改后的值
    print(msg.content)
    # 直接读内存里存好的值！！【不会再去数据库查询】
"""


# ========== 建表 / 关连接 ==========
async def init_db():
    # 自动建表；已经手动建表的话，这个只是幂等，不会重复创建（库已经提前手动建好了）
    try:
        async with async_engine.begin() as conn:    # 开启一个事务并拿到一个数据库连接(异步)对象（临时）
            # 先探测数据库连通性
            def ping(c):
                c.exec_driver_sql("SELECT 1")
            await conn.run_sync(ping)
            print("数据库连接检测成功!")
            # 连通正常后执行建表
            await conn.run_sync(Base.metadata.create_all)   # 建表
            print("数据表初始化完成!")                          # Base.metadata.create_all 是同步函数，SQLAlchemy原生建表方法没有async版本
                                                            # run_sync()的作用：在这个异步连接的底层线程里安全地跑完这个同步函数
                                                            # Base.metadata收集了所有继承Base的ORM模型
                                                            # .create_all：去MySQL查一遍，不存在的表才创建
    except Exception as e:
        print(f"数据库连接/建表失败：{e}")
        # 抛出异常，服务启动终止，不会带着数据库异常继续跑
        raise RuntimeError(f"数据库初始化失败：{e}")


async def dispose_db():
    # 关闭MySQL连接池：把池里的连接全部关掉并释放
    # 不关也能退出（进程一死，OS 会回收 fd，MySQL 侧检测到连接断开后自己清理），
    # 但那是靠系统兜底；显式 dispose 才是正常关闭，也能避免热重载/反复启停时连接堆积
    await async_engine.dispose()


# ========== 消息 & 会话状态操作 ==========
async def load_messages_from_db(session_id: str) -> list[dict]: # 返回值类型注解
    """ 读取某个会话“最近 MAX_HISTORY_MESSAGES 条”历史消息，仍按id升序返回 """
    async with AsyncSessionLocal() as db:   # 调用AsyncSessionLocal会话工厂 生成一个异步会话实例 db
        # select 筛选，按id倒序取最近 N 条（下面再翻转回升序）
        stmt = (select(Message).where(Message.session_id == session_id).order_by(Message.id.desc()).limit(MAX_HISTORY_MESSAGES))
        res = await db.execute(stmt)
        rows = list(res.scalars().all())[::-1]   # 倒序结果翻转回“旧 -> 新”，保持上下文时序
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
        await db.refresh(new_msg) # 刷新，拿到数据库生成的id。去数据库查询这条刚插入的数据，把数据库里生成的自增id，同步刷新到内存的 new_msg 对象上。
        return new_msg.id

async def delete_message_by_id(msg_id: int):
    """根据id单条删除消息"""
    async with AsyncSessionLocal() as db:
        msg = await db.get(Message, msg_id)
        if msg:
            await db.delete(msg)
            await db.commit()

async def delete_session_messages(session_id: str):
    async with AsyncSessionLocal() as db:
        # 直接语句批量删除
        stmt_msg = delete(Message).where(Message.session_id == session_id)
        await db.execute(stmt_msg)
        stmt_meta = delete(SessionMeta).where(SessionMeta.session_id == session_id)
        await db.execute(stmt_meta)
        await db.commit()


# 封装【更新会话活跃时间】函数（每次对话都要更新MySQL里的last_active_at）
async def update_session_active(session_id: str):
    now = time.time()
    async with AsyncSessionLocal() as db:
        stmt = (
            mysql_insert(SessionMeta)
            .values(session_id=session_id, last_active_at=now, create_time=now) # 没有重复主键时插入，反之更新
            .on_duplicate_key_update(last_active_at=now)   # 命中主键就只刷新活跃时间，create_time 保持不变
        )
        await db.execute(stmt)
        await db.commit()


# ========== Redis 客户端 ==========
# 模块级变量，只在 init_redis / close_redis 里赋值
# 其他文件不要直接 import 它（import 拿到的是赋值前的 None），统一走下面的函数
redis_client: Redis|None = None

# redis连接，服务自带库，库中储存键值对
async def init_redis():
    global redis_client
    try:
        redis_client = Redis(host=REDIS_HOST,port=REDIS_PORT,db=REDIS_DB)
        # ping 探测redis连通
        await redis_client.ping()
        print("Redis连接检测成功!")
    except Exception as e:
        print(f"Redis连接失败：{e}")
        raise RuntimeError(f"Redis初始化失败：{e}")

async def close_redis():
    if redis_client:
        await redis_client.aclose()

# ===== 限流工具函数(Redis版本) ===== #
async def check_rate_limit(session_id: str,prefix:str) -> bool:
    key = f"{prefix}:{session_id}"  # 以前缀区分检查chat/clear限流
    try:
        print("准备写入redis key: ",key)
        ok = await redis_client.set(key, str(time.time()), ex=RATE_LIMIT_SECONDS, nx=True)  # set 的 nx=True 就是“不存在才写入”，ex=RATE_LIMIT_SECONDS 让 key 自动过期
        print("set返回值 ok =",ok)
        return ok is not None      # 写入成功=首次请求放行；写入失败=key还在，说明间隔不足，被限流
    except Exception as e:
        print("redis set报错!!!",repr(e))
        return False

async def clear_rate_limit(session_id: str):
    # 清空上下文时把限流 key 一起删掉，否则刚清空马上重新发消息会被 429 拦住
    try:
        # 同时删除chat限流和clear限流
        key_chat=f"rate_limit:chat:{session_id}"
        key_clear=f"rate_limit:clear:{session_id}"
        await redis_client.delete(key_chat,key_clear)
    except Exception as e:
        print("清理限流key失败:", e)

# ========== 后台TTL清理任务 ==========
async def clean_expire_session():
    while True:
        await asyncio.sleep(CLEAN_INTERVAL)
        now = time.time()
        try:
            async with AsyncSessionLocal() as db:
                # 查询所有过期的session_id
                stmt=select(SessionMeta.session_id).where(SessionMeta.last_active_at < now - SESSION_TTL)
                res=await db.execute(stmt)
                expired_sids = res.scalars().all()
            if expired_sids:
                for sid in expired_sids:
                    lock=await get_session_lock(sid)    # 先获取对应的锁,防止一边删除记录一边加入记录（极端情况）
                    async with lock:
                        await delete_session_messages(sid)  # 删掉聊天记录和活跃信息
                # 顺手把 session_locks 里对应的条目也删掉
                await drop_session_locks(expired_sids)
                print("清理过期会话: ",expired_sids)
        except Exception as e:
            print("清理任务异常: ",e)


# ========== 整会话清理（/clear 用） ==========
async def reset_session(session_id: str):
    """清空一个会话：消息 + 会话状态 + 限流 key"""
    lock =await get_session_lock(session_id)
    async with lock:    # 防止同一个会话同时读写和删除数据库信息
        await delete_session_messages(session_id)
        await clear_rate_limit(session_id)
