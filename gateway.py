"""InterGate 代理网关(FastAPI):OpenAI 兼容中转。

路由 / 鉴权 / 缓存 / 限流 / 负载均衡 / 失败切换 / SSE 透传 / 用量统计。
"""
from __future__ import annotations

import logging
import asyncio
import json
import time
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from config import constants as C
from config.settings import UserSettings
from db.database import Database, get_db
from models.api_key import ApiKey, KeyStatus
from models.request_log import RequestLog
from providers.registry import (
    candidate_providers, detect_provider, endpoint_kind, resolve_base_url,
)
from providers.upstream import UpstreamClient, UpstreamError
from services.cache import ResponseCache
from services.key_manager import KeyManager
from services.load_balancer import LoadBalancer
from services.quota import QuotaMonitor
from services.rate_limiter import RateLimiter
from services.sync import ModelSyncer
from services.usage import parse_usage_from_sse_line

log = logging.getLogger("intergate.gateway")

HOP_BY_HOP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "content-length",
    "content-encoding",
}


class GatewayApp:
    def __init__(self, settings: UserSettings, db: Optional[Database] = None):
        self.settings = settings
        self.db = db or get_db()
        self.upstream = UpstreamClient(
            max_connections=settings.upstream_max_connections,
            max_keepalive=settings.upstream_max_keepalive,
        )
        self.key_manager = KeyManager(self.db)
        self.load_balancer = LoadBalancer()
        self.rate_limiter = RateLimiter(settings)
        self.cache = ResponseCache(
            enabled=settings.cache_enabled,
            ttl_seconds=settings.cache_ttl_seconds,
            max_entries=settings.cache_max_entries,
        )
        self.quota = QuotaMonitor(
            self.db,
            warn_threshold=settings.quota_warn_threshold,
            warn_enabled=settings.quota_warn_enabled,
        )
        self.syncer = ModelSyncer(self.upstream, db=self.db)
        self.quota.on_key_changed = lambda k: self.key_manager._persist_async(k)
        # 附加网关密钥缓存: 鉴权热路径避免每请求读库, web 控制台改动时失效
        self._gkeys_cache: Optional[List[Dict[str, Any]]] = None
        self._gkeys_ts: float = 0.0

        self.app = FastAPI(title="InterGate Gateway", version=C.APP_VERSION, docs_url=None)
        self._start_ts = time.time()
        self._routes()

    # ================= 路由 =================
    def _routes(self) -> None:
        a = self.app

        @a.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
        async def handle(request: Request, path: str):
            return await self._handle(request, "/" + path)

    # ================= 请求处理 =================
    async def _handle(self, request: Request, path: str) -> Response:
        if request.method == "OPTIONS":
            return Response(status_code=204, headers=_cors_headers())

        # 健康检查保持公开(不鉴权)
        if path in C.HEALTH_PATHS:
            return self._health()

        # 网关暂停开关(Web 控制台控制,只影响代理转发,不影响管理接口)
        if not self.settings.gateway_enabled:
            return self._error(503, "网关已暂停,请在 Web 控制台开启", "gateway_paused")

        # 网关鉴权(多密钥 + 权限); 管理接口同样需要鉴权
        perm = self._authorize(request)
        if perm is None:
            return self._error(401, "未授权:网关 Key 无效", "authentication_error")

        # 管理接口(需要网关 Key)
        if path == C.VERSION_PATH:
            return JSONResponse({"app": C.APP_NAME, "version": C.APP_VERSION})
        if path == C.STATS_PATH:
            return self._stats()
        if path == C.REPORT_PATH:
            return self._report()
        if path == C.CACHE_STATS_PATH:
            return JSONResponse(self.cache.stats())
        if path == C.METRICS_PATH:
            return self._metrics()

        # 提取模型与提供商(权限检查需要 model)
        body = await self._read_body(request)
        if body is None:
            return self._error(413, "请求体过大", "request_too_large")
        body_obj = _parse_json_body(body)   # 仅解析一次, 供模型/流式/token 复用
        model = _extract_model(body_obj)
        # 模型可能是对外暴露的"服务商名-真实模型名", 反解析出内部真实模型与来源条目。
        # 权限/模型路由用对外 id(model), 上游/日志/统计用真实 id(real_model)。
        entry = self.syncer.find_by_public_id(model) if model else None
        real_model = entry["id"] if entry else model
        denied = self._permission_error(perm, model, request.method)
        if denied:
            return denied

        start = time.monotonic()
        client_ip = request.client.host if request.client else ""

        # 限流(IP / 全局)
        ok, retry_after, reason = self.rate_limiter.check(client_ip, None)
        if not ok:
            return self._error(429, f"请求过于频繁({reason})", "rate_limit_exceeded",
                               retry_after=retry_after)

        header_provider = request.headers.get(C.PROVIDER_HEADER)
        provider = entry["provider"] if entry else detect_provider(path, real_model, header_provider)

        # /v1/models 优先返回本地同步列表
        if path == C.MODELS_PATH and model is None:
            local = self.syncer.all_enabled()
            if local:
                return JSONResponse({"object": "list", "data": local},
                                    headers=_cors_headers())

        # 确定候选 Key
        keys = self._pick_keys(provider, path, request, model, entry=entry)
        if not keys:
            return self._error(503, "没有可用的上游 Key", "no_available_key")

        # 缓存命中(仅幂等请求)
        cache_key = None
        if request.method in ("GET", "POST") and not _is_stream_request(body_obj):
            cache_key = self.cache.make_key(request.method, path, body)
            hit = self.cache.get(cache_key)
            if hit:
                status, headers, cached_body = hit
                self._log(request, path, real_model, status, "", cached=True,
                          latency_ms=int((time.monotonic() - start) * 1000))
                resp = Response(content=cached_body, status_code=status,
                                headers=_upstream_headers(headers))
                resp.headers[C.CACHE_HIT_HEADER] = "HIT"
                return resp

        # 逐 Key 尝试(失败切换,最多 MAX_RETRY_KEYS 次)
        last_error: Optional[UpstreamError] = None
        used_key: Optional[ApiKey] = None
        is_stream = _is_stream_request(body_obj) or request.method == "GET"
        tried: set[str] = set()

        # 若客户端用带服务商前缀的对外模型名, 上游请求必须回写真实模型名
        upstream_body = body
        if entry and real_model != model and isinstance(body_obj, dict):
            nb = dict(body_obj)
            nb["model"] = real_model
            upstream_body = json.dumps(nb, ensure_ascii=False).encode()

        for attempt in range(min(C.MAX_RETRY_KEYS, len(keys))):
            key = self.load_balancer.pick(
                [k for k in keys if k.id not in tried],
                self.settings.load_balance_strategy,
                model=model, path=path,
            )
            if key is None:
                break
            tried.add(key.id)

            # Key 级 RPM 限流
            ok, retry_after = self.rate_limiter.check_key_rpm(
                key.id, key.max_requests_per_minute)
            if not ok:
                continue

            # 自适应 TPM
            ok, wait_ms, limit = self.rate_limiter.check_tpm(
                key.id, _estimate_tokens(body_obj))
            if not ok:
                await asyncio.sleep(wait_ms / 1000.0)

            # 加密临时解密 + 构建上游请求
            plain_key = self.key_manager.decrypt(key)
            if not plain_key:
                self.key_manager.record_failure(key.id, "auth")
                continue
            base = resolve_base_url(key.provider, key.base_url)
            if not base:
                continue
            # base_url 可能已含版本前缀(如 /v1),与 path 去重拼接,避免双 /v1
            url = _join_base_and_path(base, path)
            upstream_headers = self._build_upstream_headers(request, plain_key)

            self.load_balancer.report_active(key.id, 1)
            try:
                if is_stream:
                    result = await self._forward_stream(url, upstream_headers, upstream_body)
                    if result is None:
                        continue
                    status, u_headers, gen, usage = result
                    if status >= 400:
                        err_body = await _drain(gen)
                        last_error = UpstreamError(
                            _extract_error_text(err_body), status=status,
                            category=_classify_status(status, err_body),
                            body=err_body)
                        self._after_failure(key, last_error.category)
                        continue
                    used_key = key
                    return await self._stream_response(
                        request, path, model, real_model, key, start, status,
                        u_headers, gen, usage, cache_key)
                else:
                    result = await self.upstream.forward(
                        "POST" if (body_obj or upstream_body) else request.method,
                        url, upstream_headers, upstream_body)
                    if result.ok:
                        used_key = key
                        self.key_manager.record_success(
                            key.id,
                            latency_ms=int((time.monotonic() - start) * 1000),
                        )
                        self.load_balancer.report_latency(
                            key.id, (time.monotonic() - start) * 1000)
                        if cache_key:
                            self.cache.put(cache_key, result.status,
                                           result.headers, result.body)
                        self._quota_record(key, result.status, result.usage, real_model)
                        self._log(request, path, real_model, result.status, key.id,
                                  latency_ms=int((time.monotonic() - start) * 1000),
                                  usage=result.usage)
                        resp = Response(content=result.body,
                                        status_code=result.status,
                                        headers=_upstream_headers(result.headers))
                        if cache_key:
                            resp.headers[C.CACHE_HIT_HEADER] = "MISS"
                        return resp
                    last_error = UpstreamError(
                        result.error, status=result.status,
                        category=_classify_status(result.status, result.body),
                        body=result.body)
                    self._after_failure(key, last_error.category)
                    continue
            except UpstreamError as e:
                last_error = e
                self._after_failure(key, e.category)
                continue
            finally:
                self.load_balancer.report_active(key.id, -1)

        # 全部失败
        status = last_error.status if last_error else 502
        msg = last_error.message if last_error else "上游不可用"
        category = last_error.category if last_error else "upstream"
        self._log(request, path, real_model, status, used_key.id if used_key else "",
                  latency_ms=int((time.monotonic() - start) * 1000),
                  error=msg)
        return self._error(status, msg, category, body=last_error.body if last_error else b"")

    # ================= 内部工具 =================
    async def _read_body(self, request: Request) -> Optional[bytes]:
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > C.MAX_REQUEST_BODY_BYTES:
                    return None
            except (TypeError, ValueError):
                pass  # 非法 content-length 交由实际 body 长度判断
        try:
            body = await request.body()
        except Exception:
            return b""
        if len(body) > C.MAX_REQUEST_BODY_BYTES:
            return None
        return body

    def _authorize(self, request: Request) -> Optional[Dict[str, Any]]:
        """校验网关密钥, 返回权限信息; 未授权返回 None。

        权限模型:
        - 主网关 Key(settings.gateway_key): full(全部权限)
        - 附加网关密钥(db gateway_keys): full / readonly(仅读接口) / models(限定模型)
        """
        expected = self.settings.gateway_key
        auth = request.headers.get("authorization", "")
        presented = ""
        if auth.lower().startswith("bearer "):
            presented = auth[7:].strip()
        else:
            for h in ("x-api-key", "x-gateway-key"):
                presented = request.headers.get(h, "")
                if presented:
                    break

        # 鉴权开关关闭 -> 完全放行(无论是否携带任何 Key)。
        # 此前"关闭鉴权"只放行未带 Key 的请求, 带 Key(如附加密钥)仍会
        # 落到下方权限校验, 被 readonly/models 限制拦截, 导致关掉鉴权后
        # 反而拉取不了模型。关闭开关即代表不校验任何密钥。
        if not self.settings.gateway_key_enabled:
            return {"permission": "full", "name": "", "models": []}
        # 未配置主 Key: 未带 Key 放行, 带 Key 仍可走附加密钥校验
        if not expected:
            if not presented:
                return {"permission": "full", "name": "", "models": []}
        elif presented and presented == expected:
            return {"permission": "full", "name": "主Key", "models": []}

        # 附加网关密钥(带 5s TTL 缓存, 减少热路径 DB 读; 控制台改动时即时失效)
        now_m = time.monotonic()
        if self._gkeys_cache is None or now_m - self._gkeys_ts > 5.0:
            try:
                self._gkeys_cache = self.db.get_setting("gateway_keys", []) or []
            except Exception:
                self._gkeys_cache = []
            self._gkeys_ts = now_m
        extra_keys = self._gkeys_cache
        for gk in extra_keys:
            if not gk.get("enabled", True):
                continue
            if presented and presented == gk.get("key"):
                return {
                    "permission": gk.get("permission", "full"),
                    "name": gk.get("name", ""),
                    "models": [str(m) for m in (gk.get("models") or [])],
                }
        return None

    def invalidate_gateway_keys(self) -> None:
        """Web 控制台变更附加网关密钥后调用, 立即刷新鉴权缓存(单进程内)。"""
        self._gkeys_cache = None
        self._gkeys_ts = 0.0

    def _permission_error(self, perm: Dict[str, Any], model: Optional[str],
                          method: str) -> Optional[JSONResponse]:
        """按权限检查请求是否放行, 拒绝时返回错误响应。"""
        if perm.get("permission") == "readonly":
            if method not in ("GET", "HEAD", "OPTIONS"):
                return self._error(403, "只读密钥不允许写请求", "permission_denied")
        models = perm.get("models") or []
        if models and model and model not in models:
            return self._error(403, f"密钥无权访问模型 {model}", "permission_denied")
        return None

    def _pick_keys(self, provider: str, path: str, request: Request,
                   model: Optional[str],
                   entry: Optional[Dict[str, Any]] = None) -> List[ApiKey]:
        # 客户端强制指定 Key(x-relay-key: name 或 id)
        # 同名 Key 可能有多条(相同 base_url/名称): 全部返回可用的,
        # 交由负载均衡/失败切换在它们之间选择, 避免只固定第一个。
        forced = request.headers.get(C.KEY_NAME_HEADER)
        if forced:
            matches = [k for k in self.key_manager.list_all()
                       if k.id == forced or k.name == forced]
            return [k for k in matches if k.status == KeyStatus.ACTIVE]

        # 对外模型名命中: 精确定位到提供该模型的 Key(source_keys), 
        # 实现不同 base_url / 不同服务商彼此隔离。
        if entry:
            sks = entry.get("source_keys") or []
            src = [k for k in self.key_manager.list_all()
                   if k.id in sks and k.status == KeyStatus.ACTIVE]
            if src:
                return src

        # 模型路由规则:某模型只走指定 Key(路由启用且命中时)
        routed = self._route_keys_for(model)
        if routed is not None:
            return routed

        candidates = candidate_providers(provider, path)
        keys: List[ApiKey] = []
        for p in candidates:
            keys.extend(self.key_manager.list_active(p))
        return keys

    def _route_keys_for(self, model: Optional[str]) -> Optional[List[ApiKey]]:
        """按模型路由规则返回候选 Key;无规则或未启用返回 None(走默认负载均衡)。"""
        if not model:
            return None
        try:
            routes = self.db.get_routes()
        except Exception:
            return None
        rule = routes.get(model)
        if not rule or not rule.get("enabled"):
            return None
        wanted = rule.get("key_ids") or []
        active = [k for k in self.key_manager.list_all()
                  if k.status == KeyStatus.ACTIVE and k.id in wanted]
        if not active:
            return None  # 路由的 Key 全不可用,回退默认策略
        return active

    def _build_upstream_headers(self, request: Request, plain_key: str) -> Dict[str, str]:
        headers = {}
        for name, value in request.headers.items():
            lname = name.lower()
            if lname in ("host", "authorization", "content-length", "cookie",
                         "x-relay-provider", "x-relay-key", "x-relay-cache",
                         "x-gateway-key", "x-api-key"):
                continue
            headers[name] = value
        headers["Authorization"] = f"Bearer {plain_key}"
        return headers

    async def _forward_stream(self, url: str, headers: Dict[str, str],
                              body: bytes) -> Optional[Tuple]:
        try:
            status, u_headers, gen = await self.upstream.open_stream(
                "POST", url, headers, body)
        except UpstreamError as e:
            raise e
        usage: Dict[str, Any] = {}
        return status, u_headers, gen, usage

    async def _stream_response(self, request: Request, path: str, model: str,
                               real_model: str, key: ApiKey, start: float,
                               status: int, u_headers: Dict[str, str],
                               gen: AsyncGenerator[bytes, None],
                               usage: Dict[str, Any],
                               cache_key: Optional[str]) -> Response:
        resp_headers = _upstream_headers(u_headers)
        resp_headers.setdefault("Content-Type", "text/event-stream; charset=utf-8")
        resp_headers.setdefault("Cache-Control", "no-cache")
        resp_headers.setdefault("X-Accel-Buffering", "no")

        async def response_gen() -> AsyncGenerator[bytes, None]:
            try:
                async for chunk in gen:
                    _collect_usage(usage, chunk)
                    yield chunk
            except (asyncio.CancelledError, GeneratorExit):
                # 客户端断开:终止上游流,释放连接,避免泄漏
                await gen.aclose()
                raise
            finally:
                self.key_manager.record_success(
                    key.id,
                    latency_ms=int((time.monotonic() - start) * 1000),
                )
                self.load_balancer.report_latency(
                    key.id, (time.monotonic() - start) * 1000)
                self._quota_record(key, status, usage, real_model)
                self._log(request, path, real_model, status, key.id,
                          latency_ms=int((time.monotonic() - start) * 1000),
                          usage=usage)

        return StreamingResponse(response_gen(), status_code=status,
                                 headers=resp_headers,
                                 media_type="text/event-stream")

    def _after_failure(self, key: ApiKey, category: str) -> None:
        self.key_manager.record_failure(key.id, category)
        self._quota_record(key, 500 if category != "quota" else 429, {})

    def _quota_record(self, key: ApiKey, status: int, usage: Dict[str, Any],
                      model: str = "") -> None:
        """记录用量并落库统计。cost_usd 按模型单价实时估算。"""
        cost_usd = 0.0
        if usage:
            prompt = int(usage.get("prompt_tokens", 0))
            completion = int(usage.get("completion_tokens", 0))
            if prompt or completion:
                try:
                    from config.pricing import estimate_cost
                    cost_usd, _, _ = estimate_cost(model, prompt, completion)
                except Exception as e:
                    log.debug("估算费用失败 model=%s: %s", model, e)
                    cost_usd = 0.0
        self.quota.record(key, status, usage, cost_usd=cost_usd)

    def _log(self, request: Request, path: str, model: str, status: int,
             key_id: str, cached: bool = False, latency_ms: int = 0,
             usage: Optional[Dict[str, Any]] = None, error: str = "") -> None:
        try:
            usage = usage or {}
            rl = RequestLog(
                path=path,
                method=request.method,
                status=status,
                provider=detect_provider(path, model,
                                         request.headers.get(C.PROVIDER_HEADER)),
                key_id=key_id,
                model=model,
                latency_ms=latency_ms,
                prompt_tokens=usage.get("prompt_tokens", 0),
                completion_tokens=usage.get("completion_tokens", 0),
                cached=cached,
                error=error,
            )
            self.db.enqueue_log(rl.to_dict())
        except Exception as e:
            log.warning("写入请求日志失败: %s", e)

    # ================= 管理接口 =================
    def _health(self) -> JSONResponse:
        keys = self.key_manager.list_all()
        active = [k for k in keys if k.status == KeyStatus.ACTIVE]
        return JSONResponse({
            "status": "ok",
            "app": C.APP_NAME,
            "version": C.APP_VERSION,
            "time": int(time.time() * 1000),
            "uptime_s": round(time.time() - self._start_ts, 1),
            "active_keys": len(active),
            "total_keys": len(keys),
            "models_count": len(self.syncer.all_enabled()),
            "gateway_enabled": self.settings.gateway_enabled,
            "last_sync": self.syncer.last_sync,
        })

    def _stats_body(self) -> Dict[str, Any]:
        keys = self.key_manager.list_all()
        active = [k for k in keys if k.status == KeyStatus.ACTIVE]
        return {
            "active_keys": len(active),
            "total_keys": len(keys),
            "used_today": sum(k.today_used for k in keys),
            "requests_today": sum(k.today_requests for k in keys),
            "errors_today": sum(k.today_errors for k in keys),
            "rate_limiter": self.rate_limiter.stats(),
            "cache": {**self.cache.stats(), **self.db.cache_stats_today()},
            "models_count": len(self.syncer.all_enabled()),
            "last_sync": self.syncer.last_sync,
        }

    def _stats(self) -> JSONResponse:
        return JSONResponse(self._stats_body())

    def _metrics(self) -> Response:
        """Prometheus 文本格式指标(供 Grafana/Prometheus 抓取)。

        指标为进程级快照(多 worker 下各进程独立, 抓取时需按实例聚合)。
        """
        b = self._stats_body()
        keys = self.key_manager.list_all()
        cache = self.cache.stats()
        lines = []

        def _m(name: str, val, help_text: str, typ: str = "gauge",
               labels: str = "") -> None:
            if not any(l.startswith(f"# HELP {name} ") for l in lines):
                lines.append(f"# HELP {name} {help_text}")
                lines.append(f"# TYPE {name} {typ}")
            lbl = f"{{{labels}}}" if labels else ""
            lines.append(f"{name}{lbl} {val}")

        _m("intergate_keys_total", b["total_keys"], "Key 总数")
        _m("intergate_keys_active", b["active_keys"], "可用 Key 数")
        _m("intergate_requests_today", b["requests_today"],
           "今日请求数", "counter")
        _m("intergate_errors_today", b["errors_today"],
           "今日错误数", "counter")
        _m("intergate_tokens_today", b["used_today"],
           "今日消耗 token", "counter")
        _m("intergate_models_count", b["models_count"], "已启用模型数")
        _m("intergate_cache_hits", cache.get("hits", 0), "缓存命中数", "counter")
        _m("intergate_cache_misses", cache.get("misses", 0),
           "缓存未命中数", "counter")
        _m("intergate_cache_entries", cache.get("entries", 0), "缓存条目数")
        _m("intergate_uptime_seconds", round(time.time() - self._start_ts, 1),
           "运行时长(秒)")
        # 每 Key 用量(带标签)
        for k in keys:
            safe = k.id
            _m("intergate_key_requests_today", k.today_requests,
               "各 Key 今日请求数", "counter",
               labels=f'key_id="{safe}",provider="{k.provider}"')
            _m("intergate_key_errors_today", k.today_errors,
               "各 Key 今日错误数", "counter",
               labels=f'key_id="{safe}",provider="{k.provider}"')
            _m("intergate_key_tokens_today", k.today_used,
               "各 Key 今日消耗 token", "counter",
               labels=f'key_id="{safe}",provider="{k.provider}"')

        text = "\n".join(lines) + "\n"
        return Response(content=text,
                        media_type="text/plain; version=0.0.4; charset=utf-8")

    def _report(self) -> JSONResponse:
        return JSONResponse({
            "summary": self.quota.summary(7),
            "alerts": self.quota.recent_alerts(),
            "keys": [k.to_dict() for k in self.key_manager.list_all()],
        })

    def _error(self, status: int, message: str, err_type: str,
               retry_after: Optional[int] = None,
               body: bytes = b"") -> JSONResponse:
        if body and status >= 400:
            try:
                j = json.loads(body)
                if isinstance(j, dict) and "error" in j:
                    return JSONResponse(j, status_code=status, headers=_cors_headers())
            except Exception as e:
                log.debug("解析错误响应体失败: %s", e)
        headers = _cors_headers()
        if retry_after:
            headers[C.RETRY_AFTER_HEADER] = str(retry_after)
        return JSONResponse(
            {"error": {"message": message, "type": err_type,
                       "code": status}},
            status_code=status, headers=headers,
        )


# ================= 模块级工具 =================
def _cors_headers() -> Dict[str, str]:
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
        "Access-Control-Allow-Headers": "Authorization,Content-Type,x-relay-provider,x-relay-key,x-gateway-key,x-api-key",
    }


def _upstream_headers(headers: Dict[str, str]) -> Dict[str, str]:
    out = {}
    for k, v in headers.items():
        if k.lower() in HOP_BY_HOP_HEADERS:
            continue
        out[k] = v
    out.update(_cors_headers())
    return out


def _parse_json_body(body: bytes) -> Optional[Dict[str, Any]]:
    """一次性解析请求体 JSON; 非 JSON/空体返回 None(避免多次 json.loads)。"""
    if not body:
        return None
    try:
        j = json.loads(body)
        return j if isinstance(j, dict) else None
    except Exception as e:
        log.debug("解析请求体失败: %s", e)
        return None


def _extract_model(j: Optional[Dict[str, Any]]) -> Optional[str]:
    if isinstance(j, dict):
        m = j.get("model")
        if isinstance(m, str):
            return m
    return None


def _is_stream_request(j: Optional[Dict[str, Any]]) -> bool:
    return bool(j.get("stream")) if isinstance(j, dict) else False


def _estimate_tokens(j: Optional[Dict[str, Any]]) -> int:
    """粗估请求 token 数(供 AIMD TPM 记账)。

    覆盖: messages[].content 的字符串与多模态数组(取其中 text 片段)、
    以及顶层 prompt / input 字段(completions / embeddings)。
    图片/音频等非文本片段按固定当量计入, 避免大图请求被严重低估。
    """
    if not isinstance(j, dict):
        return 1
    total_chars = 0
    extra = 0

    def _content_len(c: Any) -> int:
        nonlocal extra
        if isinstance(c, str):
            return len(c)
        n = 0
        if isinstance(c, list):
            for part in c:
                if isinstance(part, str):
                    n += len(part)
                elif isinstance(part, dict):
                    t = part.get("text")
                    if isinstance(t, str):
                        n += len(t)
                    if part.get("type") in ("image_url", "image",
                                            "input_audio", "audio"):
                        extra += 1024  # 非文本片段固定当量
        return n

    msgs = j.get("messages")
    if isinstance(msgs, list):
        for msg in msgs:
            if isinstance(msg, dict):
                total_chars += _content_len(msg.get("content"))

    for field in ("prompt", "input"):
        v = j.get(field)
        if isinstance(v, str):
            total_chars += len(v)
        elif isinstance(v, list):
            for it in v:
                if isinstance(it, str):
                    total_chars += len(it)

    return max(1, total_chars // 4 + extra)


def _classify_status(status: int, body: bytes) -> str:
    text = body.decode("utf-8", errors="replace").lower()
    if status == 429:
        # 429 一律视为临时限流(可自愈): 只做冷却计数, 不判 Key 耗尽。
        # 商汤等上游常返回 "quota exceeded/workspace quota" 的 429,
        # 那只是分钟/并发级配额, Key 本身仍有效, 不应触发 EXHAUSTED 禁用。
        return "tpm"
    # 账户级硬耗尽信号(402 payment_required / 明确 free quota 文本)优先判断,
    # 避免 403 带 quota 文本的响应被误判为 auth。
    if any(kw in text for kw in C.QUOTA_EXHAUSTED_KEYWORDS):
        return "quota"
    if status in (401, 403):
        return "auth"
    return "upstream"


def _join_base_and_path(base: str, path: str) -> str:
    """拼接上游 Base URL 与请求路径,避免版本前缀(如 /v1)重复。

    base 可能为 https://host/v1,path 为 /v1/chat/completions,
    直接拼接会产生 /v1/v1 导致上游 404。此处检测 base 尾段与 path 首段
    相同则去掉重复段; query string 单独保留。"""
    base = base.rstrip("/")
    p = path if path.startswith("/") else "/" + path
    if "?" in p:
        p_path, p_query = p.split("?", 1)
    else:
        p_path, p_query = p, ""
    parts = p_path.split("/")
    p_head = parts[1] if len(parts) > 1 else ""
    base_tail = base.rsplit("/", 1)[-1]
    if base_tail and base_tail == p_head:
        base = base[: -len(base_tail)].rstrip("/")
    result = base + p_path
    if p_query:
        result += "?" + p_query
    return result


def _extract_error_text(body: bytes) -> str:
    try:
        j = json.loads(body)
        if isinstance(j, dict):
            err = j.get("error")
            if isinstance(err, dict):
                return str(err.get("message", ""))
            if isinstance(err, str):
                return err
    except Exception as e:
        log.debug("解析错误文本失败: %s", e)
    return body.decode("utf-8", errors="replace")[:300]


def _collect_usage(usage: Dict[str, Any], chunk: bytes) -> None:
    try:
        text = chunk.decode("utf-8", errors="ignore")
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("data:") and line != "data: [DONE]":
                payload = line[5:].strip()
                if not payload:
                    continue
                u = parse_usage_from_sse_line(("data: " + payload).encode())
                if u:
                    usage.update(u)
    except Exception as e:
        log.debug("收集 SSE 用量失败: %s", e)


async def _drain(gen: AsyncGenerator[bytes, None]) -> bytes:
    chunks = []
    async for c in gen:
        chunks.append(c)
    return b"".join(chunks)


def create_gateway_app(settings: Optional[UserSettings] = None,
                       db: Optional[Database] = None) -> FastAPI:
    g = GatewayApp(settings or UserSettings(), db)
    return g.app
