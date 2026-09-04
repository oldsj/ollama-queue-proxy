"""Embedding response cache backed by Valkey / any RESP-compatible store."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from urllib.parse import urlsplit, urlunsplit

import redis.asyncio as aioredis

from .config import EmbeddingCacheConfig

logger = logging.getLogger(__name__)

# Endpoints whose responses are cacheable (deterministic, compact, high repeat rate)
CACHEABLE_PATHS = frozenset({"/api/embed", "/api/embeddings"})

# Log RESP errors at most once per minute to avoid spam
_ERROR_LOG_COOLDOWN = 60.0

class BoundedCounter(dict[str, int]):
    """A dict-compatible counter that caps attacker-controlled label cardinality."""

    def __init__(self, max_keys: int = 1024, overflow_key: str = "__overflow__") -> None:
        super().__init__()
        self.max_keys = max_keys
        self.overflow_key = overflow_key

    def increment(self, key: str) -> None:
        if key in self:
            self[key] += 1
        elif len(self) < self.max_keys:
            self[key] = 1
        else:
            self[self.overflow_key] = self.get(self.overflow_key, 0) + 1


# Metric counters — bounded in-place mappings read by /metrics.
hits = BoundedCounter(overflow_key="overflow,overflow,overflow")
misses = BoundedCounter(overflow_key="overflow,overflow,overflow")
errors = BoundedCounter(max_keys=64)


def _canonical_json(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def _cache_key(
    prefix: str,
    endpoint_ns: str,
    model: str,
    payload,
    tenant: str | None = None,
) -> str:
    """Build a cache key from model + normalised payload. No raw content logged."""
    preimage = _canonical_json(
        {"tenant": tenant or "anon", "model": model, "payload": payload}
    )
    digest = hashlib.sha256(preimage).hexdigest()[:32]
    return f"{prefix}v2:{endpoint_ns}:{digest}"


def _semantic_payload(body_data: dict) -> dict:
    """Return all semantic fields, excluding only Ollama residency control."""
    return {key: value for key, value in body_data.items() if key != "keep_alive"}


def _embed_key(
    prefix: str, model: str, body_data: dict, tenant: str | None = None
) -> str:
    """Cache key for /api/embed, including every semantic request option."""
    payload = _semantic_payload(body_data)
    raw_input = payload.get("input", "")
    if isinstance(raw_input, str):
        payload["input"] = [raw_input]
    return _cache_key(prefix, "embed", model, payload, tenant)


def _embeddings_key(
    prefix: str, model: str, body_data: dict, tenant: str | None = None
) -> str:
    """Cache key for /api/embeddings."""
    return _cache_key(
        prefix, "embeddings", model, _semantic_payload(body_data), tenant
    )


def _redact_url(url: str) -> str:
    parsed = urlsplit(url)
    host = parsed.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    if parsed.port:
        host = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def _metric_component(value: str, limit: int = 64) -> str:
    if len(value) <= limit:
        return value
    digest = hashlib.sha256(value.encode()).hexdigest()[:12]
    return f"{value[:limit]}~{digest}"


class EmbeddingCache:
    """
    Async embedding cache wrapping redis.asyncio.

    Startup: fail-fast if backend unreachable when enabled.
    Runtime: RESP errors log once/min and degrade gracefully — never fail a user request.
    Security (FLAG E): only key hash and model name are logged. No prompt/input content.
    """

    def __init__(self, config: EmbeddingCacheConfig) -> None:
        self._cfg = config
        self._client: aioredis.Redis | None = None
        self._last_error_log: float = 0.0
        self._enabled = config.enabled

    async def startup(self) -> None:
        """Connect and ping. Exits the process on failure when cache is enabled."""
        if not self._enabled:
            return
        import sys

        try:
            self._client = aioredis.from_url(
                self._cfg.backend,
                socket_connect_timeout=self._cfg.connect_timeout,
            )
            await self._client.ping()
            logger.info("embedding_cache.connected backend=%s", _redact_url(self._cfg.backend))
        except Exception as e:
            print(
                f"FATAL: embedding cache startup failed — could not connect to "
                f"'{_redact_url(self._cfg.backend)}': {type(e).__name__}. "
                "Fix the backend address or set embedding_cache.enabled: false.",
                file=sys.stderr,
            )
            sys.exit(1)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()

    def _metric_key(self, client_id: str | None, model: str, endpoint: str) -> str:
        return ",".join(
            (
                _metric_component(client_id or "anon"),
                _metric_component(model),
                _metric_component(endpoint),
            )
        )

    async def get(
        self,
        path: str,
        body_data: dict,
        model: str,
        client_id: str | None,
    ) -> bytes | None:
        """
        Return cached response bytes for the request, or None on miss/error.
        Logs model and cache key hash only — no request body content (FLAG E).
        """
        if not self._enabled or self._client is None:
            return None

        key = self._build_key(path, body_data, model, client_id)
        if key is None:
            return None

        mkey = self._metric_key(client_id, model, path)
        try:
            value = await self._client.get(key)
            if value is not None:
                hits.increment(mkey)
                logger.debug(
                    "embedding_cache.hit endpoint=%s model=%s key_suffix=...%s",
                    path, model, key[-8:],
                )
                return value
            misses.increment(mkey)
            return None
        except Exception as e:
            self._log_error("get", e)
            return None

    async def set(
        self,
        path: str,
        body_data: dict,
        model: str,
        response_bytes: bytes,
        client_id: str | None,
    ) -> None:
        """
        Cache a successful 2xx response. Skips if over max_entry_bytes.
        Logs model and key hash only (FLAG E).
        """
        if not self._enabled or self._client is None:
            return

        if len(response_bytes) > self._cfg.max_entry_bytes:
            logger.debug(
                "embedding_cache.skip_large endpoint=%s model=%s size=%d max=%d",
                path, model, len(response_bytes), self._cfg.max_entry_bytes,
            )
            return

        key = self._build_key(path, body_data, model, client_id)
        if key is None:
            return

        try:
            await self._client.setex(key, self._cfg.ttl, response_bytes)
            logger.debug(
                "embedding_cache.stored endpoint=%s model=%s key_suffix=...%s ttl=%d",
                path, model, key[-8:], self._cfg.ttl,
            )
        except Exception as e:
            self._log_error("set", e)

    def _build_key(
        self, path: str, body_data: dict, model: str, client_id: str | None
    ) -> str | None:
        try:
            if path == "/api/embed":
                return _embed_key(self._cfg.key_prefix, model, body_data, client_id)
            elif path == "/api/embeddings":
                return _embeddings_key(self._cfg.key_prefix, model, body_data, client_id)
            return None
        except Exception:
            return None

    def _log_error(self, op: str, exc: Exception) -> None:
        now = time.monotonic()
        kind = type(exc).__name__
        errors.increment(kind)
        if now - self._last_error_log >= _ERROR_LOG_COOLDOWN:
            self._last_error_log = now
            logger.warning(
                "embedding_cache.error op=%s kind=%s error=%s (suppressing further logs for %ds)",
                op, kind, exc, int(_ERROR_LOG_COOLDOWN),
            )
