"""多维限流:IP / 全局 / Key / Token,自适应 TPM(AIMD)。"""
from __future__ import annotations

import asyncio
import threading
import time
from collections import defaultdict, deque
from typing import Deque, Dict, Optional, Tuple

from config import constants as C
from config.settings import UserSettings


class SlidingWindow:
    """滑动窗口计数器(每分钟)。"""

    def __init__(self, window_sec: float = 60.0):
        self.window_sec = window_sec
        # 元素为 (timestamp, weight), 支持按 token 加权记账
        self._events: Deque[tuple] = deque()
        self._sum = 0.0

    def add(self, weight: int = 1) -> int:
        """加入一个(加权)事件, 返回当前窗口累计权重(默认 weight=1 即事件数)。"""
        now = time.monotonic()
        self._prune(now)
        self._events.append((now, float(weight)))
        self._sum += weight
        return int(self._sum)

    def count(self) -> int:
        self._prune(time.monotonic())
        return int(self._sum)

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_sec
        while self._events and self._events[0][0] <= cutoff:
            self._sum -= self._events[0][1]
            self._events.popleft()


class AdaptiveTPM:
    """每 Key 自适应 TPM 上限:AIMD(乘性减、加性增)。"""

    def __init__(self, enabled: bool = True, min_tpm: float = 60.0,
                 max_tpm: float = 100_000.0):
        self.enabled = enabled
        self._limits: Dict[str, float] = defaultdict(lambda: max_tpm)
        self._min, self._max = min_tpm, max_tpm
        self._lock = threading.Lock()

    def limit_for(self, key_id: str) -> float:
        return self._limits.get(key_id, self._max)

    def adjust_down(self, key_id: str) -> None:
        if not self.enabled:
            return
        with self._lock:
            cur = self._limits.get(key_id, self._max)
            self._limits[key_id] = max(self._min, cur * C.TPM_AIMD_DOWN)

    def adjust_up(self, key_id: str) -> None:
        if not self.enabled:
            return
        with self._lock:
            cur = self._limits.get(key_id, self._max)
            self._limits[key_id] = min(self._max, cur * (1.0 + C.TPM_AIMD_UP))

    def stats(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._limits)


class RateLimiter:
    """多维限流器。

    设计说明: IP/Key/TPM 计数器为瞬时数据, 重启后清空。
    限流窗口最大 60 秒, 重启后的短暂空窗期可接受。
    自适应 TPM 上限同理——冷启动使用 max_tpm, 逐步收敛。
    """
    def __init__(self, settings: UserSettings):
        self._s = settings
        self._ip: Dict[str, SlidingWindow] = defaultdict(SlidingWindow)
        self._global = SlidingWindow()
        self._key: Dict[str, SlidingWindow] = defaultdict(SlidingWindow)
        self._tpm: Dict[str, SlidingWindow] = defaultdict(SlidingWindow)
        self.adaptive = AdaptiveTPM(settings.adaptive_tpm_enabled)
        self._lock = threading.Lock()

    # ---------- 检查(返回 (allowed, retry_after_seconds, reason)) ----------
    def check(self, ip: str, key_id: str | None,
              tokens: int = 0) -> Tuple[bool, int, str]:
        s = self._s
        if not s.rate_limit_enabled:
            return True, 0, ""

        # 全局 RPM
        if s.global_rpm_limit > 0 and self._global.add() > s.global_rpm_limit:
            return False, _retry_after(s.global_rpm_limit), "global"

        # IP 限流
        if s.ip_rate_limit_per_minute > 0:
            with self._lock:
                w = self._ip[ip]
            if w.add() > s.ip_rate_limit_per_minute:
                return False, _retry_after(s.ip_rate_limit_per_minute), "ip"

        return True, 0, ""

    def check_key_rpm(self, key_id: str, max_rpm: int) -> Tuple[bool, int]:
        if max_rpm <= 0:
            return True, 0
        w = self._key.setdefault(key_id, SlidingWindow())
        if w.add() > max_rpm:
            return False, _retry_after(max_rpm)
        return True, 0

    def check_tpm(self, key_id: str, tokens: int) -> Tuple[bool, float, float]:
        """自适应 TPM 检查:返回 (allowed, wait_ms, limit)。"""
        if not self._s.adaptive_tpm_enabled:
            return True, 0.0, 0.0
        limit = self.adaptive.limit_for(key_id)
        w = self._tpm.setdefault(key_id, SlidingWindow())
        used = w.count()
        if used + tokens > limit:
            # AIMD 降速
            self.adaptive.adjust_down(key_id)
            wait = (used + tokens - limit) / limit * 60_000
            return False, min(wait, C.TPM_WAIT_BUDGET_MS), limit
        # 按 token 当量一次性记账, 避免大量 token 时逐条 add 的开销
        w.add(max(1, tokens // max(1, int(limit // 60))))
        self.adaptive.adjust_up(key_id)
        return True, 0.0, limit

    def stats(self) -> Dict[str, object]:
        return {
            "ip_count": len(self._ip),
            "key_count": len(self._key),
            "adaptive_tpm": self.adaptive.stats(),
        }


def _retry_after(limit_per_min: int) -> int:
    if limit_per_min <= 0:
        return 1
    return max(1, int(60 / limit_per_min) + 1)
