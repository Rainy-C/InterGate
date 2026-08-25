"""额度监控:每日用量、错误率、预警与统计落库。"""
from __future__ import annotations

import datetime
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from config import constants as C
from db.database import Database
from models.api_key import ApiKey, KeyStatus

log = logging.getLogger("intergate.quota")


class QuotaMonitor:
    def __init__(self, db: Database, warn_threshold: float = C.DEFAULT_QUOTA_WARN_THRESHOLD,
                 warn_enabled: bool = True):
        self._db = db
        self._threshold = warn_threshold
        self._warn_enabled = warn_enabled
        self._lock = threading.Lock()
        self.on_key_changed: Optional[Callable[[ApiKey], None]] = None
        # 内存告警列表(从 DB 恢复, 避免重启后丢失)
        self.alerts: List[Dict[str, Any]] = []
        self._load_alerts()
        # webhook 配置提供器: 返回 (url, secret, enabled), 运行时读最新设置
        self.webhook_provider: Optional[Callable[[], Any]] = None

    def _load_alerts(self) -> None:
        """从数据库恢复历史告警(最多 MAX_PERSISTED_ALERTS 条)。"""
        try:
            rows = self._db.query_alerts(limit=C.MAX_PERSISTED_ALERTS)
            # 反转为时间正序(数据库查询是倒序的)
            self.alerts = list(reversed([
                {"ts": r["ts"], "type": r["type"], "key_id": r["key_id"],
                 "provider": r["provider"], "message": r["message"]}
                for r in rows
            ]))
        except Exception as e:
            log.warning("从数据库恢复告警失败: %s", e)
            self.alerts = []

    def _persist_alert(self, alert: Dict[str, Any]) -> None:
        """将告警持久化到数据库。"""
        try:
            alert_id = self._db.insert_alert(
                ts=alert["ts"], alert_type=alert["type"],
                key_id=alert["key_id"], provider=alert["provider"],
                message=alert["message"],
            )
            alert["id"] = alert_id
            # 裁剪内存和数据库中的告警, 防止无限增长
            if len(self.alerts) > C.MAX_PERSISTED_ALERTS:
                self.alerts = self.alerts[-C.MAX_PERSISTED_ALERTS:]
                try:
                    self._db.prune_alerts(C.MAX_PERSISTED_ALERTS)
                except Exception as e:
                    log.warning("裁剪告警表失败: %s", e)
        except Exception as e:
            log.warning("持久化告警失败: %s", e)

    def _webhook(self, title: str, content: str) -> None:
        try:
            if not self.webhook_provider:
                return
            url, secret, enabled = self.webhook_provider()
            if not enabled or not url:
                return
            from services.webhook import notify_webhook_sync
            notify_webhook_sync(url, title, content, secret or '')
        except Exception as e:
            log.warning("Webhook 推送失败: %s", e)

    def record(self, key: ApiKey, status_code: int, usage: Dict[str, Any],
               cost_usd: float = 0.0) -> Dict[str, Any]:
        today = datetime.date.today().isoformat()
        month = today[:7]
        is_error = status_code >= 400
        with self._lock:
            if key.day_stamp != today:
                key.day_stamp = today
                key.used_today = 0
                key.requests_today = 0
                key.errors_today = 0
            if key.month_stamp != month:
                key.month_stamp = month
                key.used_month = 0
                key.requests_month = 0

            prompt = int(usage.get("prompt_tokens", 0))
            completion = int(usage.get("completion_tokens", 0))
            key.used_today += prompt + completion
            key.used_month += prompt + completion
            key.requests_today += 1
            key.requests_month += 1
            if is_error:
                key.errors_today += 1
            key.last_used = int(time.time() * 1000)

            try:
                # 后台批量队列落库, 避免每次请求同步写阻塞事件循环
                self._db.enqueue_stats(
                    date=today, provider=key.provider, key_id=key.id,
                    requests=1, errors=1 if is_error else 0,
                    prompt_tokens=prompt, completion_tokens=completion,
                    cost_usd=cost_usd,
                )
            except Exception as e:
                log.warning("写入统计失败 key=%s: %s", key.id, e)

            warnings = self._evaluate(key)
            if self.on_key_changed:
                try:
                    self.on_key_changed(key)
                except Exception as e:
                    log.warning("on_key_changed 回调失败 key=%s: %s", key.id, e)
        return {"warnings": warnings, "key": key}

    def _evaluate(self, key: ApiKey) -> List[str]:
        if not self._warn_enabled:
            return []
        warns: List[str] = []
        if key.daily_quota > 0 and key.quota_ratio >= self._threshold:
            warns.append(f"额度已达 {key.quota_ratio:.0%}")
            if key.quota_ratio >= 1.0:
                key.status = KeyStatus.EXHAUSTED
                warns.append("额度耗尽,Key 已禁用")
                alert = {
                    "ts": int(time.time() * 1000),
                    "type": "quota_exhausted",
                    "key_id": key.id,
                    "provider": key.provider,
                    "message": f"Key {key.name} 额度已耗尽",
                }
                self.alerts.append(alert)
                self._persist_alert(alert)
                self._webhook("InterGate 告警: Key 额度耗尽",
                              f"Key「{key.name}」({key.provider}) 每日配额已用完, "
                              f"已自动禁用。用量 {key.used_today:,} token")
        if key.requests_today >= 10 and key.error_rate > 0.5:
            warns.append(f"错误率过高 {key.error_rate:.0%}")
            if len(self.alerts) == 0 or self.alerts[-1].get('type') != 'high_error_rate':
                alert = {
                    "ts": int(time.time() * 1000),
                    "type": "high_error_rate",
                    "key_id": key.id,
                    "provider": key.provider,
                    "message": f"Key {key.name} 错误率过高 {key.error_rate:.0%}",
                }
                self.alerts.append(alert)
                self._persist_alert(alert)
                self._webhook("InterGate 告警: Key 错误率过高",
                              f"Key「{key.name}」({key.provider}) 今日错误率 "
                              f"{key.error_rate:.0%} (请求 {key.requests_today} 次)")
        return warns

    def summary(self, days: int = 7) -> Dict[str, Any]:
        return self._db.stats_summary(days)

    def recent_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self.alerts[-limit:]
