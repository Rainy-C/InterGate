"""模型列表同步:拉取各提供商 /models 并合并统一格式。"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional

from models.api_key import KeyStatus
from providers.registry import resolve_base_url
from providers.upstream import UpstreamClient

log = logging.getLogger("intergate.sync")


class ModelSyncer:
    def __init__(self, client: UpstreamClient, db=None):
        self._client = client
        self._db = db
        self._models: Dict[str, Dict[str, Any]] = {}
        # 模型开关状态(model_id -> bool),独立于模型列表持久化
        self._states: Dict[str, bool] = {}
        if db is not None:
            try:
                self._states = db.get_model_states()
            except Exception as e:
                log.warning("加载模型开关状态失败: %s", e)
                self._states = {}
        self.last_sync: Optional[int] = None
        self.sync_error: str = ""
        self._public_index: Dict[str, Dict[str, Any]] = {}
        # 重启后从数据库恢复缓存的模型列表, 避免冷启动无模型可用
        self._load_cached_models()
        self._rebuild_public_index()

    def _load_cached_models(self) -> None:
        """从数据库加载缓存的模型列表(重启后立即可用)。"""
        if self._db is None:
            return
        try:
            cached = self._db.load_model_cache()
            if cached:
                for m in cached:
                    mkey = (m.get("id", ""), m.get("source", ""), m.get("base_url", ""))
                    # 用持久化的开关状态覆盖
                    m["enabled"] = self._states.get(m.get("id", ""), m.get("enabled", True))
                    self._models[mkey] = m
                log.info("从数据库恢复 %d 个缓存模型", len(cached))
            self._assign_public_ids()
        except Exception as e:
            log.warning("从数据库恢复模型缓存失败: %s", e)

    def _persist_models(self) -> None:
        """将当前模型列表持久化到数据库。"""
        if self._db is None:
            return
        try:
            models = list(self._models.values())
            self._db.save_model_cache(models)
        except Exception as e:
            log.warning("持久化模型列表失败: %s", e)

    async def sync(self, keys: List[Any]) -> Dict[str, Any]:
        """keys: KeyManager 的 ApiKey 列表。

        仅同步 active 状态的 Key(停用/异常 Key 的模型不进入列表),
        保证模型列表与 Key 管理页数据一致。
        """
        keys = [k for k in keys if k.status == KeyStatus.ACTIVE]
        merged: Dict[str, Dict[str, Any]] = {}

        async def _pull(k) -> None:
            base = resolve_base_url(k.provider, k.base_url)
            if not base:
                return
            plain = _decrypt(k)
            if not plain:
                return
            try:
                if k.provider == "anthropic":
                    return  # anthropic 无 /models 接口,使用内置列表
                url = base + "/models"
                headers = {"Authorization": f"Bearer {plain}"}
                if k.provider == "google":
                    url = base + "/v1beta/models?key=" + plain
                    headers = {}
                data = await self._client.get_json(url, headers)
                if not data:
                    return
                for m in _extract_models(data):
                    mid = m.get("id") or m.get("name")
                    if not mid:
                        continue
                    sn = k.name or k.provider
                    # 仅当模型 id + Key 名称 + base_url 三者均相同才聚合,
                    # 不同名称或不同 base_url 的同名模型各自独立显示
                    mkey = (mid, sn, base)
                    if mkey in merged:
                        # 冗余 Key(同名同 base_url)提供同一模型: 聚合来源
                        if k.id not in merged[mkey]["source_keys"]:
                            merged[mkey]["source_keys"].append(k.id)
                        if sn not in merged[mkey]["sources"]:
                            merged[mkey]["sources"].append(sn)
                        continue
                    merged[mkey] = {
                        "id": mid,
                        "name": m.get("name") or mid,
                        "provider": k.provider,
                        "source": sn,
                        "base_url": base,
                        "source_key": k.id,
                        "source_keys": [k.id],
                        "sources": [sn],
                        "owned_by": m.get("owned_by", ""),
                        "created": m.get("created"),
                        "capabilities": infer_capabilities(mid),
                        "enabled": self._states.get(mid, True),
                    }
            except Exception as e:
                self.sync_error = str(e)
                log.warning("同步 Key %s(%s) 模型失败: %s", k.id, k.provider, e)

        await asyncio.gather(*(_pull(k) for k in keys[:20]))
        self._models = merged
        self._assign_public_ids()
        self._rebuild_public_index()
        self.last_sync = int(time.time() * 1000)
        # 持久化到数据库, 重启后可立即恢复
        self._persist_models()
        return {"models": list(merged.values()), "count": len(merged),
                "error": self.sync_error, "last_sync": self.last_sync}

    def remove_by_key(self, key_id: str) -> int:
        """Key 删除/停用时移除其提供的模型, 返回移除数量。

        若同一模型由多个 Key 提供(同名/同 base_url 冗余 Key),
        只从来源列表中移除该 Key, 保留模型并提升下一个来源。
        sources 是去重后的 Key 名称(可能少于 source_keys), 不随 Key 移除而变动。
        """
        removed = 0
        for mid, m in list(self._models.items()):
            sks = m.get("source_keys") or []
            if key_id not in sks:
                continue
            sks = [x for x in sks if x != key_id]
            if not sks:
                self._models.pop(mid, None)
                removed += 1
                continue
            m["source_keys"] = sks
            m["source_key"] = sks[0]
        # 更新数据库缓存
        self._persist_models()
        self._rebuild_public_index()
        return removed

    # ---------- 对外暴露 ID ----------
    def _assign_public_ids(self) -> None:
        """为每个模型计算对外暴露 ID: {服务商名}-{真实模型名}。

        不同 base_url 的同名同服务商模型通过后缀唯一化, 保证彼此隔离且 ID 不冲突。
        """
        from collections import defaultdict
        groups: Dict[str, list] = defaultdict(list)
        for m in self._models.values():
            groups[f"{m.get('source', '')}-{m['id']}"].append(m)
        for pid, items in groups.items():
            if len(items) == 1:
                items[0]["public_id"] = pid
            else:
                items.sort(key=lambda x: (x.get("base_url") or "", x["id"]))
                for idx, m in enumerate(items, 1):
                    m["public_id"] = f"{pid}-{idx}"

    def _rebuild_public_index(self) -> None:
        self._public_index = {m.get("public_id"): m for m in self._models.values()}

    def find_by_public_id(self, public_id: str) -> Optional[Dict[str, Any]]:
        """按对外暴露 ID 查找内部模型条目(含真实 id/source/base_url/source_keys)。"""
        return self._public_index.get(public_id)

    def _public_view(self) -> List[Dict[str, Any]]:
        """对外视图: 模型 id 替换为带服务商前缀的 public_id。"""
        out = []
        for m in self._models.values():
            d = dict(m)
            d["id"] = d.get("public_id", d["id"])
            out.append(d)
        return out

    def all(self) -> List[Dict[str, Any]]:
        return self._public_view()

    def all_enabled(self) -> List[Dict[str, Any]]:
        """仅返回开启状态的模型(暴露给网关, 对外 id 带服务商前缀)。"""
        return [m for m in self._public_view() if m.get("enabled", True)]

    def set_enabled(self, model_id: str, enabled: bool) -> None:
        """切换模型开关,内存 + 持久化。

        同一模型 id 可能对应多个来源(不同名称/base_url), 全部同步更新。
        """
        matched = [m for m in self._models.values()
                   if m.get("id") == model_id or m.get("public_id") == model_id]
        real_ids = {m["id"] for m in matched}
        for rid in real_ids:
            self._states[rid] = enabled
        for m in matched:
            m["enabled"] = enabled
        if self._db is not None and real_ids:
            for rid in real_ids:
                try:
                    self._db.set_model_state(rid, enabled)
                    self._db.set_model_cache_enabled(rid, enabled)
                except Exception as e:
                    log.warning("持久化模型开关失败 model=%s: %s", rid, e)

    def get(self, model_id: str) -> Optional[Dict[str, Any]]:
        for m in self._models.values():
            if m.get("id") == model_id or m.get("public_id") == model_id:
                return m
        return None


def _decrypt(k) -> str:
    from crypto.aes import decrypt_plaintext
    try:
        return decrypt_plaintext(k.encrypted_key)
    except Exception as e:
        log.warning("解密 Key 失败: %s", e)
        return ""


def _extract_models(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    if isinstance(data.get("data"), list):
        return [m for m in data["data"] if isinstance(m, dict)]
    if isinstance(data.get("models"), list):
        return [m for m in data["models"] if isinstance(m, dict)]
    return []


def infer_capabilities(model_id: str) -> List[str]:
    m = model_id.lower()
    caps = []
    if any(x in m for x in ("embed", "ada")):
        caps.append("embedding")
    if any(x in m for x in ("dall-e", "image", "flux", "sdxl")):
        caps.append("image")
    if any(x in m for x in ("whisper", "stt", "transcri")):
        caps.append("audio")
    if any(x in m for x in ("tts", "speech")):
        caps.append("tts")
    if any(x in m for x in ("gpt", "o1", "o3", "o4", "claude", "gemini",
                            "llama", "qwen", "deepseek", "mistral", "glm", "kimi")):
        caps.append("chat")
    return caps or ["other"]
