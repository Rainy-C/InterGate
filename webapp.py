"""InterGate Web 管理 API(FastAPI,端口 51235)。"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import time
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse

from config import constants as C
from config.pricing import estimate_cost
from config.settings import UserSettings
from db.database import Database, get_db
from gateway import GatewayApp
from models.api_key import KeyStatus

WEB_DIR = Path(__file__).parent / "web"

log = logging.getLogger("intergate.webapp")

# 登录失败限速: ip -> [失败时间戳]
_login_attempts: Dict[str, List[float]] = {}
_LOGIN_MAX_FAILURES = 5
_LOGIN_WINDOW_SECONDS = 60.0


def _provider_defaults() -> Dict[str, str]:
    return dict(C.PROVIDER_BASE_URLS)


def _detect_ips() -> Dict[str, Any]:
    """探测本机回环地址与局域网 IPv4 地址列表。

    优先用 UDP connect 拿默认路由出口 IP(在 Termux/Android WiFi 下可靠,
    即使无外网也能拿到局域网地址),再尝试解析主机名与 `ip -4 addr` 兜底。
    回环地址固定为 127.0.0.1。
    """
    lan: List[str] = []
    seen: set = set()

    def _add(ip: str) -> None:
        ip = (ip or "").strip()
        if not ip:
            return
        # 去掉 CIDR 后缀(如 192.168.1.167/24)
        ip = ip.split("/")[0]
        if ip.startswith("127.") or ip == "::1":
            return
        if ip not in seen:
            seen.add(ip)
            lan.append(ip)

    # 1) UDP 默认路由出口 IP(不实际发包,仅触发路由选择)
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            _add(s.getsockname()[0])
        finally:
            s.close()
    except Exception:
        pass

    # 2) 主机名解析
    try:
        import socket
        for info in socket.getaddrinfo(socket.gethostname(), None):
            _add(info[4][0])
    except Exception:
        pass

    # 3) `ip -4 addr` 兜底(部分环境可用)
    try:
        import re
        import subprocess
        out = subprocess.run(["ip", "-4", "addr", "show"],
                             capture_output=True, text=True, timeout=3)
        for m in re.finditer(r"inet\s+(\S+)", out.stdout):
            _add(m.group(1))
    except Exception:
        pass

    return {"loopback": "127.0.0.1", "lan": sorted(set(lan))}


class WebApp:
    def __init__(self, gateway: GatewayApp, db: Optional[Database] = None):
        self.gateway = gateway
        self.db = db or gateway.db
        self.settings = gateway.settings
        _pricing_reload(self.db)
        self.app = FastAPI(title="InterGate Web Console", version=C.APP_VERSION,
                           docs_url=None)
        self._routes()

    # ---------------- 路由 ----------------
    def _routes(self) -> None:
        a = self.app

        @a.middleware("http")
        async def auth_middleware(request: Request, call_next):
            if self._is_public(request.url.path):
                return await call_next(request)
            if not self._authorized(request):
                return JSONResponse({"error": "未授权"}, status_code=401)
            return await call_next(request)

        @a.get("/")
        async def index():
            return FileResponse(WEB_DIR / "index.html")

        @a.get("/manifest.json")
        async def manifest():
            return FileResponse(WEB_DIR / "manifest.json",
                                media_type="application/manifest+json")

        @a.get("/sw.js")
        async def sw_js():
            return FileResponse(WEB_DIR / "sw.js", media_type="application/javascript")

        @a.get("/{filename}")
        async def static_file(filename: str):
            # 静态资源(图标等); 不在 web/ 目录则返回 404
            target = (WEB_DIR / filename).resolve()
            if WEB_DIR.resolve() not in target.parents and target != WEB_DIR.resolve():
                return JSONResponse({"detail": "Not Found"}, status_code=404)
            if target.is_file():
                return FileResponse(target)
            return JSONResponse({"detail": "Not Found"}, status_code=404)

        @a.post("/api/login")
        async def login(body: Dict[str, Any], request: Request):
            pwd = self.settings.web_password
            ip = request.client.host if request.client else "unknown"
            self._check_login_attempts(ip)
            if not pwd:
                return {"token": self._make_token("local")}
            if body.get("password") == pwd:
                self._clear_login_attempts(ip)
                return {"token": self._make_token("admin")}
            self._record_login_attempt(ip)
            raise HTTPException(401, "密码错误")

        @a.get("/api/status")
        async def status():
            s = self.settings
            return {
                "app": C.APP_NAME,
                "version": C.APP_VERSION,
                "slogan": C.APP_SLOGAN,
                "gateway_enabled": s.gateway_enabled,
                "gateway_host": s.host,
                "gateway_port": s.port,
                "web_port": s.web_port,
                "ips": _detect_ips(),
                "load_balance_strategy": s.load_balance_strategy,
                "uptime": _uptime(),
                "stats": self.gateway._stats_body(),
            }

        @a.get("/api/keys")
        async def list_keys():
            return [k.to_dict() for k in self.gateway.key_manager.list_all()]

        @a.post("/api/keys")
        async def add_key(body: Dict[str, Any]):
            provider = (body.get("provider") or "custom").strip().lower()
            plain = (body.get("api_key") or body.get("key") or "").strip()
            if not plain:
                raise HTTPException(400, "API Key 不能为空")
            if provider not in C.PROVIDER_BASE_URLS:
                raise HTTPException(400, f"未知提供商: {provider}")
            k = self.gateway.key_manager.add_key(
                provider=provider,
                plain_key=plain,
                name=(body.get("name") or "").strip(),
                base_url=(body.get("base_url") or "").strip() or None,
                grp="",
                priority=int(body.get("priority", 100)),
                weight=int(body.get("weight", 1)),
                daily_quota=int(body.get("daily_quota", 1_000_000)),
                max_requests_per_minute=int(body.get("max_requests_per_minute", 60)),
                note=(body.get("note") or "").strip(),
                provider_id=(body.get("provider_id") or "").strip(),
            )
            return k.to_dict()

        @a.put("/api/keys/{key_id}")
        async def update_key(key_id: str, body: Dict[str, Any]):
            k = self.gateway.key_manager.get(key_id)
            if not k:
                raise HTTPException(404, "Key 不存在")
            plain = (body.get("api_key") or "").strip() or None
            for field in ("name", "note"):
                if field in body:
                    setattr(k, field, body[field] or "")
            # 仅当请求显式携带 base_url 时才更新, 避免 {status} 等局部更新清空
            if "base_url" in body:
                k.base_url = (body.get("base_url") or "") or None
            for field in ("priority", "weight", "max_requests_per_minute", "daily_quota"):
                if field in body:
                    setattr(k, field, int(body[field]))
            if body.get("status") in (KeyStatus.ACTIVE, KeyStatus.INACTIVE,
                                      KeyStatus.EXHAUSTED, KeyStatus.ERROR):
                self.gateway.key_manager.set_status(k.id, body["status"])
                # 停用/异常 Key 的模型立即从模型列表移除, 与 Key 管理页一致
                if body["status"] != KeyStatus.ACTIVE:
                    self.gateway.syncer.remove_by_key(k.id)
                else:
                    # 重新启用 Key: 后台同步拉回其模型, 立即生效
                    async def _resync():
                        try:
                            await self.gateway.syncer.sync(
                                self.gateway.key_manager.list_all())
                        except Exception:
                            pass
                    asyncio.create_task(_resync())
            self.gateway.key_manager.update_key(k, plain)
            return k.to_dict()

        @a.delete("/api/keys/{key_id}")
        async def delete_key(key_id: str):
            self.gateway.key_manager.delete_key(key_id)
            # 已删除 Key 的模型立即从模型列表移除
            self.gateway.syncer.remove_by_key(key_id)
            return {"ok": True}

        @a.get("/api/keys/{key_id}/secret")
        async def get_key_secret(key_id: str):
            """返回解密后的明文密钥(仅编辑回显用, 管理台已鉴权)。"""
            k = self.gateway.key_manager.get(key_id)
            if not k:
                raise HTTPException(404, "Key 不存在")
            return {"key_id": key_id, "key": self.gateway.key_manager.decrypt(k)}

        @a.post("/api/keys/{key_id}/test")
        async def test_key(key_id: str):
            result = await self.gateway.key_manager.test_key(
                key_id, client=self.gateway.upstream)
            if not result["ok"]:
                raise HTTPException(400, result.get("message") or "测试失败")
            return result

        @a.post("/api/keys/test-all")
        async def test_all():
            results = []
            for k in self.gateway.key_manager.list_all():
                r = await self.gateway.key_manager.test_key(
                    k.id, client=self.gateway.upstream)
                results.append({"id": k.id, "name": k.name, **r})
            return {"results": results}

        @a.get("/api/logs")
        async def logs(limit: int = Query(200, le=1000), offset: int = Query(0, ge=0),
                       provider: str = "", status: Optional[int] = None,
                       key_id: str = "", model: str = "",
                       ts_from: Optional[int] = None, ts_to: Optional[int] = None):
            rows, total = self.db.query_logs(
                limit=limit, offset=offset, provider=provider,
                status=status, key_id=key_id, model=model,
                ts_from=ts_from, ts_to=ts_to, total=True)
            return {"logs": rows, "count": len(rows), "total": total,
                    "offset": offset, "limit": limit}

        @a.delete("/api/logs")
        async def clear_logs():
            n = self.db.clear_logs()
            return {"ok": True, "deleted": n}

        @a.get("/api/stats")
        async def stats(days: int = Query(7, ge=1, le=30)):
            import datetime as _dt
            end = _dt.date.today()
            start = end - _dt.timedelta(days=days - 1)

            # 价格预估: 以 stats_daily 已落库费用为主(请求时实时估算写入),
            # 与 request_logs 是否被清理无关
            _rows = self.db.stats_range(start.isoformat(), end.isoformat())
            total_usd = round(sum(r["cost_usd"] or 0 for r in _rows), 6)
            today_usd = round(sum(r["cost_usd"] or 0 for r in _rows
                                  if r["date"] == end.isoformat()), 6)

            # 按模型拆分明细: 优先 request_logs(含 model 字段); 日志被清理时
            # 降级为 stats_daily 聚合条目, 保证图表非空且费用不丢失
            models = []
            for r in self.db.token_usage_by_model(days):
                cost, pin, pout = estimate_cost(
                    r["model"], r["prompt_tokens"], r["completion_tokens"])
                models.append({**r, "cost_usd": round(cost, 6),
                               "input_price": pin, "output_price": pout})
            if not models and total_usd > 0:
                tok = sum((r["prompt_tokens"] or 0) + (r["completion_tokens"] or 0)
                          for r in _rows)
                models.append({
                    "model": "全部模型", "prompt_tokens": tok,
                    "completion_tokens": 0, "cost_usd": total_usd,
                    "input_price": 0, "output_price": 0,
                })

            return {"summary": self.gateway.quota.summary(days),
                    "cache": self.gateway.cache.stats(),
                    "rate_limiter": self.gateway.rate_limiter.stats(),
                    "alerts": self.gateway.quota.recent_alerts(),
                    "pricing": {"total_usd": total_usd,
                                "today_usd": today_usd,
                                "models": models}}

        @a.get("/api/models")
        async def models():
            return {"models": self.gateway.syncer.all(),
                    "last_sync": self.gateway.syncer.last_sync}

        @a.put("/api/models/{model_id}")
        async def toggle_model(model_id: str, body: Dict[str, Any]):
            """切换模型开关:关闭后不再暴露给网关。"""
            enabled = bool(body.get("enabled", True))
            self.gateway.syncer.set_enabled(model_id, enabled)
            return {"model_id": model_id, "enabled": enabled,
                    "models_count": len(self.gateway.syncer.all_enabled())}

        @a.post("/api/sync")
        async def sync():
            keys = self.gateway.key_manager.list_all()
            result = await self.gateway.syncer.sync(keys)
            return result

        @a.get("/api/routes")
        async def get_routes():
            return {"routes": self.gateway.db.get_routes()}

        @a.put("/api/routes/{model}")
        async def put_route(model: str, body: Dict[str, Any]):
            key_ids = body.get("key_ids") or []
            self.gateway.db.set_route(
                model, [str(x) for x in key_ids],
                enabled=bool(body.get("enabled", True)),
                note=str(body.get("note", "")))
            return {"ok": True, "model": model}

        @a.delete("/api/routes/{model}")
        async def delete_route(model: str):
            self.gateway.db.delete_route(model)
            return {"ok": True, "model": model}

        @a.get("/api/settings")
        async def get_settings():
            return self.settings.to_dict()

        @a.put("/api/settings")
        async def put_settings(body: Dict[str, Any]):
            # 只更新提交的字段,其余保持现状
            current = self.settings.to_dict()
            current.update(body)
            # 数值字段类型校验(前端可能提交字符串)
            int_fields = ("port", "web_port", "max_concurrent",
                          "cache_ttl_seconds", "cache_max_entries",
                          "ip_rate_limit_per_minute", "global_rpm_limit",
                          "token_rate_limit_per_minute",
                          "sync_interval_minutes", "log_retention_days",
                          "max_log_entries")
            float_fields = ("quota_warn_threshold", "burst_multiplier")
            for f in int_fields:
                if f in current and not isinstance(current[f], bool):
                    try:
                        current[f] = int(current[f])
                    except (TypeError, ValueError):
                        raise HTTPException(400, f"字段 {f} 格式非法")
            for f in float_fields:
                if f in current and not isinstance(current[f], bool):
                    try:
                        current[f] = float(current[f])
                    except (TypeError, ValueError):
                        raise HTTPException(400, f"字段 {f} 格式非法")
            for f in ("port", "web_port"):
                if f in current and not (1 <= int(current[f]) <= 65535):
                    raise HTTPException(400, "端口必须为 1-65535")
            old_port = self.settings.port
            old_settings_snapshot = self.settings.to_dict()
            new = UserSettings.from_dict(current)
            self.settings.__dict__.update(new.__dict__)
            self.db.set_setting("user_settings", new.to_dict())
            # 检测哪些字段变更需要重启才能生效
            from config.settings import RESTART_FIELDS
            changed_fields = {}
            for field in RESTART_FIELDS:
                old_val = old_settings_snapshot.get(field)
                new_val = current.get(field)
                if old_val != new_val:
                    changed_fields[field] = {"old": old_val, "new": new_val}
            return {"ok": True,
                    "port_changed": old_port != new.port,
                    "needs_restart": bool(changed_fields),
                    "restart_fields": changed_fields,
                    "settings": self.settings.to_dict()}

        @a.post("/api/gateway/toggle")
        async def toggle_gateway(body: Dict[str, Any]):
            self.settings.gateway_enabled = bool(body.get("enabled", True))
            self.db.set_setting("user_settings", self.settings.to_dict())
            return {"gateway_enabled": self.settings.gateway_enabled}

        @a.post("/api/restart")
        async def restart_service():
            """请求重启服务(修改端口等需重启的字段后调用)。

            通过向自身进程发送 SIGTERM,由 run.sh 的 nohup 机制
            或进程管理器重新拉起。若不在 nohup 环境下则仅返回提示。
            """
            import os, signal, threading
            def _delayed_restart():
                import time
                time.sleep(0.5)
                os.kill(os.getpid(), signal.SIGTERM)
            threading.Thread(target=_delayed_restart, daemon=True).start()
            return {"ok": True, "message": "服务将在 0.5 秒后重启"}

        @a.get("/api/gateway-keys")
        async def get_gateway_keys():
            keys = self.db.get_setting("gateway_keys", []) or []
            # 脱敏: 不返回完整密钥(仅前4后4)
            masked = []
            for k in keys:
                kk = dict(k)
                raw = str(kk.get("key", ""))
                kk["key_masked"] = (raw[:4] + "••••" + raw[-4:]) if len(raw) > 8 else "••••"
                kk.pop("key", None)
                masked.append(kk)
            return {"keys": masked}

        @a.put("/api/gateway-keys")
        async def put_gateway_keys(body: Dict[str, Any]):
            keys = body.get("keys") or []
            clean = []
            for k in keys:
                clean.append({
                    "key": str(k.get("key", "")).strip(),
                    "name": str(k.get("name", "")).strip(),
                    "permission": k.get("permission", "full"),
                    "models": [str(m) for m in (k.get("models") or [])],
                    "enabled": bool(k.get("enabled", True)),
                    "note": str(k.get("note", "")),
                })
            self.db.set_setting("gateway_keys", [k for k in clean if k["key"]])
            try:
                self.gateway.invalidate_gateway_keys()  # 即时刷新鉴权缓存
            except Exception:
                pass
            return {"ok": True, "count": len([k for k in clean if k["key"]])}

        @a.get("/api/trend")
        async def usage_trend(days: int = Query(7, ge=1, le=90)):
            """用量趋势:按日返回请求数/Token/费用/错误数, 缺失日期补 0。"""
            import datetime as _dt
            end = _dt.date.today()
            start = end - _dt.timedelta(days=days - 1)
            rows = self.db.stats_range(start.isoformat(), end.isoformat())
            daily: Dict[str, Dict[str, float]] = {}
            for r in rows:
                agg = daily.setdefault(r["date"], {
                    "requests": 0, "errors": 0,
                    "prompt_tokens": 0, "completion_tokens": 0,
                    "cost_usd": 0.0,
                })
                agg["requests"] += r["requests"]
                agg["errors"] += r["errors"]
                agg["prompt_tokens"] += r["prompt_tokens"]
                agg["completion_tokens"] += r["completion_tokens"]
                agg["cost_usd"] += r["cost_usd"]
            series = []
            cur = start
            while cur <= end:
                d = cur.isoformat()
                agg = daily.get(d, {"requests": 0, "errors": 0,
                                    "prompt_tokens": 0, "completion_tokens": 0,
                                    "cost_usd": 0.0})
                series.append({
                    "date": d,
                    "label": cur.strftime("%m-%d"),
                    "requests": int(agg["requests"]),
                    "errors": int(agg["errors"]),
                    "tokens": int(agg["prompt_tokens"] + agg["completion_tokens"]),
                    "cost_usd": round(agg["cost_usd"], 6),
                })
                cur += _dt.timedelta(days=1)
            return {"days": days, "series": series}

        @a.post("/api/cache/clear")
        async def clear_cache():
            """清空网关响应缓存, 返回清除的条目数。"""
            cleared = self.gateway.cache.invalidate()
            return {"ok": True, "cleared": cleared,
                    "cache": self.gateway.cache.stats()}

        @a.get("/api/health")
        async def health_check():
            """网关健康 + 各 Key 上游连通性实时探测。"""
            from services.health import HealthProbe
            keys = self.gateway.key_manager.list_all()
            probe = HealthProbe()
            try:
                results = await probe.probe_all(keys)
            finally:
                await probe.aclose()
            return {"status": "ok",
                    "time": int(__import__("time").time() * 1000),
                    "summary": HealthProbe.summary(results),
                    "results": results}

        @a.get("/api/providers")
        async def providers():
            return {"providers": [{"id": p, "default_base_url": u}
                                  for p, u in _provider_defaults().items()]}

        @a.post("/api/backup")
        async def make_backup():
            """创建数据库备份(含主密钥), 返回备份信息。"""
            import shutil, subprocess
            try:
                result = subprocess.run(
                    ["./run.sh", "backup"], capture_output=True, text=True, timeout=60)
                out = (result.stdout or "") + (result.stderr or "")
                files = []
                import glob, os
                for f in sorted(glob.glob("backups/intergate-*.tgz"))[-10:]:
                    files.append({"name": os.path.basename(f),
                                  "size": os.path.getsize(f)})
                return {"ok": result.returncode == 0,
                        "message": out.strip(), "files": files}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        @a.get("/api/backup/download")
        async def download_backup(name: str = ""):
            import os
            if not name or ".." in name or "/" in name:
                return JSONResponse({"error": "非法文件名"}, status_code=400)
            path = os.path.join("backups", name)
            if not os.path.exists(path):
                return JSONResponse({"error": "备份不存在"}, status_code=404)
            from fastapi.responses import FileResponse
            return FileResponse(path, media_type="application/gzip",
                                filename=name)

        @a.get("/api/export/logs")
        async def export_logs():
            """导出全部请求日志为 CSV。"""
            import csv, io
            rows = self.db.query_logs(limit=100000)
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["ts", "path", "method", "status", "provider",
                        "key_id", "model", "latency_ms", "prompt_tokens",
                        "completion_tokens", "cached", "error"])
            for r in rows:
                w.writerow([r.get(k, "") for k in
                            ("ts", "path", "method", "status", "provider",
                             "key_id", "model", "latency_ms", "prompt_tokens",
                             "completion_tokens", "cached", "error")])
            from fastapi.responses import Response
            return Response(content=buf.getvalue(),
                            media_type="text/csv; charset=utf-8",
                            headers={"Content-Disposition":
                                     'attachment; filename="logs.csv"'})

        @a.get("/api/export/stats")
        async def export_stats():
            """导出近 30 日用量统计为 CSV。"""
            import csv, io, datetime as _dt
            today = _dt.date.today().isoformat()
            start = (_dt.date.today() - _dt.timedelta(days=30)).isoformat()
            rows = self.db.stats_range(start, today)
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["date", "provider", "key_id", "requests",
                        "errors", "prompt_tokens", "completion_tokens", "cost_usd"])
            for r in rows:
                w.writerow([r.get(k, "") for k in
                            ("date", "provider", "key_id", "requests",
                             "errors", "prompt_tokens", "completion_tokens", "cost_usd")])
            from fastapi.responses import Response
            return Response(content=buf.getvalue(),
                            media_type="text/csv; charset=utf-8",
                            headers={"Content-Disposition":
                                     'attachment; filename="stats.csv"'})

        @a.post("/api/webhook/test")
        async def test_webhook(body: Dict[str, Any]):
            from services.webhook import notify_webhook
            url = str(body.get("url", "")).strip()
            if not url:
                return {"ok": False, "error": "URL 为空"}
            ok = await notify_webhook(
                url, "InterGate 测试推送",
                "这是一条来自 InterGate 网关控制台的测试通知。",
                str(body.get("secret", "")))
            return {"ok": ok, "error": "" if ok else "推送失败(非 2xx 或网络错误)"}

        @a.get("/api/pricing")
        async def get_pricing():
            from config.pricing import MODEL_PRICES, DEFAULT_PRICE, get_overrides
            return {"rules": get_overrides(),
                    "builtin": MODEL_PRICES,
                    "default": DEFAULT_PRICE}

        @a.put("/api/pricing")
        async def put_pricing(body: Dict[str, Any]):
            from config.pricing import set_overrides
            rules = body.get("rules") or {}
            clean = {str(k): (float(v[0]), float(v[1]))
                     for k, v in rules.items()
                     if str(k).strip() and isinstance(v, (list, tuple)) and len(v) >= 2}
            set_overrides(clean)
            self.db.set_setting("pricing_rules", clean)
            return {"ok": True, "rules": clean}

    # ---------------- 鉴权 ----------------
    def _is_public(self, path: str) -> bool:
        if path == "/":
            return True
        if path.startswith("/api/login"):
            return True
        # /api/status 始终公开:登录页需要它判断是否要登录,且不含敏感数据
        if path.startswith("/api/status"):
            return True
        return False

    def _authorized(self, request: Request) -> bool:
        if not self.settings.web_password:
            # 未设置密码: 仅本机可访问, 避免局域网内任何人拿到明文 Key
            host = request.client.host if request.client else ""
            return host in ("127.0.0.1", "::1", "localhost")
        token = request.headers.get("authorization", "")
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
        return self._verify_token(token)


    # ---------------- Token 与登录限速 ----------------
    def _make_token(self, role: str) -> str:
        """生成带过期时间的签名 token(小时粒度, 24 小时有效)。"""
        secret = self.settings.web_password or "intergate-local"
        hour = int(time.time()) // 3600
        payload = f"{role}:{hour}"
        sig = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"),
                       hashlib.sha256).hexdigest()[:24]
        return base64.urlsafe_b64encode(f"{payload}.{sig}".encode("utf-8")).decode("ascii")

    def _verify_token(self, token: str) -> bool:
        """校验 token 签名与 24 小时有效期。"""
        if not token:
            return False
        try:
            raw = base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
            payload, sig = raw.rsplit(".", 1)
            role, hour_str = payload.rsplit(":", 1)
            if role not in ("admin", "local"):
                return False
            secret = self.settings.web_password or "intergate-local"
            expect = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"),
                              hashlib.sha256).hexdigest()[:24]
            if not hmac.compare_digest(sig, expect):
                return False
            age = int(time.time()) // 3600 - int(hour_str)
            return 0 <= age <= 24
        except Exception:
            return False

    def _check_login_attempts(self, ip: str) -> None:
        """检查该 IP 是否已被限速(不计数)。"""
        now = time.time()
        with _login_lock():
            lst = [t for t in _login_attempts.get(ip, [])
                   if now - t < _LOGIN_WINDOW_SECONDS]
            if len(lst) >= _LOGIN_MAX_FAILURES:
                raise HTTPException(429, "尝试次数过多, 请稍后再试")

    def _record_login_attempt(self, ip: str) -> None:
        """记录一次登录失败。"""
        now = time.time()
        with _login_lock():
            lst = [t for t in _login_attempts.get(ip, [])
                   if now - t < _LOGIN_WINDOW_SECONDS]
            lst.append(now)
            _login_attempts[ip] = lst

    def _clear_login_attempts(self, ip: str) -> None:
        with _login_lock():
            _login_attempts.pop(ip, None)


import threading as _threading
_login_lock_obj = _threading.Lock()


def _login_lock():
    """返回全局单例锁(修复原实现每次创建新锁的问题)。"""
    return _login_lock_obj


def _pricing_reload(db) -> None:
    """从数据库加载用户自定义价格覆盖。"""
    try:
        from config.pricing import set_overrides
        rules = db.get_setting("pricing_rules", {}) or {}
        set_overrides(rules)
    except Exception as e:
        log.warning("加载价格覆盖失败: %s", e)


def _uptime() -> float:
    import time
    return round(time.time() - _START_TS, 1)


_START_TS = time.time()


def create_web_app(gateway: GatewayApp, db: Optional[Database] = None) -> FastAPI:
    return WebApp(gateway, db).app
