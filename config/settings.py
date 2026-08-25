"""运行时用户设置(可持久化到 SQLite)"""
from __future__ import annotations

import dataclasses
import json
from typing import Any, Dict, FrozenSet

from . import constants as C

# 修改这些字段后需要重启服务才能生效(运行时无法热加载)
RESTART_FIELDS: FrozenSet[str] = frozenset({
    "port",        # 端口变更需要重新 bind
    "web_port",    # Web 端口变更需要重新 bind
    "host",        # 监听地址变更需要重新 bind
    "max_concurrent",  # 并发连接数变更需要重新配置 ASGI server
    "workers",     # worker 数量变更需要重启
})


@dataclasses.dataclass
class UserSettings:
    # 网关
    gateway_enabled: bool = True   # 运行时开关(Web 控制台可暂停)
    host: str = C.DEFAULT_HOST
    port: int = C.DEFAULT_PORT
    gateway_key: str = ""            # 网关访问密钥(留空=不鉴权)
    gateway_key_enabled: bool = True   # 网关是否启用访问密钥鉴权(开关持久化; 关闭=无需 key 即可调用)
    max_concurrent: int = C.MAX_CONCURRENT_CONNECTIONS
    workers: int = 1                  # Uvicorn worker 数量(>1 时启用多进程)

    # 负载均衡
    load_balance_strategy: str = "smart"

    # 缓存
    cache_enabled: bool = True
    cache_ttl_seconds: int = C.DEFAULT_CACHE_TTL_SECONDS
    cache_max_entries: int = C.DEFAULT_CACHE_MAX_ENTRIES

    # 限流
    rate_limit_enabled: bool = True
    ip_rate_limit_per_minute: int = C.DEFAULT_IP_RATE_LIMIT_PER_MINUTE
    global_rpm_limit: int = C.DEFAULT_GLOBAL_RPM_LIMIT
    token_rate_limit_per_minute: int = C.DEFAULT_TOKEN_RATE_LIMIT_PER_MINUTE
    burst_multiplier: float = C.DEFAULT_BURST_MULTIPLIER
    adaptive_tpm_enabled: bool = True

    # 额度
    quota_warn_threshold: float = C.DEFAULT_QUOTA_WARN_THRESHOLD
    quota_warn_enabled: bool = True

    # 同步
    sync_enabled: bool = True
    sync_interval_minutes: int = 60

    # 日志
    log_retention_days: int = C.DEFAULT_LOG_RETENTION_DAYS
    max_log_entries: int = C.DEFAULT_MAX_LOG_ENTRIES

    # 管理 Web
    web_enabled: bool = True
    web_port: int = C.DEFAULT_WEB_PORT
    web_password: str = ""           # 管理台密码(留空=仅本机可访问)

    # 告警 Webhook(配额耗尽/错误率过高等事件推送)
    webhook_enabled: bool = False
    webhook_url: str = ""
    webhook_secret: str = ""

    # 上游连接池(httpx)
    upstream_max_connections: int = 100       # 每上游最大连接数
    upstream_max_keepalive: int = 20         # 每上游保持连接数

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "UserSettings":
        known = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in known})
