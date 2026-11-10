# ========== 全局配置 ==========
# 所有可调参数集中在这里，其他文件只读取、不修改

# 允许使用的模型白名单
ALLOWED_MODELS = ["qwen2.5:7b","deepseek-r1:8b"]

# ========== 会话清理相关配置 ==========
SESSION_TTL = 30*60         # 会话30分钟闲置过期
CLEAN_INTERVAL = 60         # 每60秒扫描一次过期会话
RATE_LIMIT_SECONDS = 3      # 同一session两次请求最小间隔（秒）

# 每次送给模型的历史消息条数上限（只取最近 N 条）
MAX_HISTORY_MESSAGES = 20

# ========== MySQL 配置 ==========
# SQLAlchemy 异步MySQL连接字符串
# mysql+aiomysql：驱动协议，root账号，密码，ip，端口，数据库名
DB_URL = "mysql+aiomysql://root:123456@172.19.66.169:3306/rag_db"

# ========== Redis 配置 ==========
REDIS_HOST = "172.19.66.169"    # Redis服务ip地址
REDIS_PORT = 6379               # Redis默认端口
REDIS_DB = 0                    # Redis使用的数据库编号，默认0号库

# ========== Ollama 配置 ==========
OLLAMA_API = "http://127.0.0.1:11434/api/chat"   # 流式对话接口
