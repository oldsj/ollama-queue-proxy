"""Configuration loading and validation for ollama-queue-proxy."""

from __future__ import annotations

import ipaddress
import os
import sys
from typing import Literal
from urllib.parse import urlsplit

import yaml
from pydantic import BaseModel, field_validator, model_validator


class HostConfig(BaseModel):
    url: str
    name: str
    weight: int = 1
    model_sync_interval: int = 30

    @field_validator("url")
    @classmethod
    def valid_url(cls, v: str) -> str:
        parsed = urlsplit(v)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("ollama host URL must be an absolute http(s) URL")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("ollama host URL must not contain credentials")
        return v.rstrip("/")

    @field_validator("weight")
    @classmethod
    def positive_weight(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"ollama.hosts[].weight must be a positive integer, got {v}")
        return v

    @field_validator("model_sync_interval")
    @classmethod
    def positive_sync_interval(cls, v: int) -> int:
        if v < 1:
            raise ValueError(
                f"ollama.hosts[].model_sync_interval must be >= 1 second, got {v}"
            )
        return v


class OllamaConfig(BaseModel):
    hosts: list[HostConfig]
    health_check_interval: int = 30
    request_timeout: int = 300

    @field_validator("health_check_interval", "request_timeout")
    @classmethod
    def positive_timeouts(cls, v: int) -> int:
        if v < 1:
            raise ValueError("Ollama intervals and timeouts must be positive")
        return v


class TierConfig(BaseModel):
    max_depth: int = 100
    max_wait: int = 300
    high_watermark_pct: int = 80

    @field_validator("max_depth", "max_wait")
    @classmethod
    def positive_values(cls, v: int) -> int:
        if v < 1:
            raise ValueError("queue depth and wait values must be positive")
        return v

    @field_validator("high_watermark_pct")
    @classmethod
    def valid_watermark(cls, v: int) -> int:
        if not 1 <= v <= 100:
            raise ValueError("high_watermark_pct must be between 1 and 100")
        return v


class QueueConfig(BaseModel):
    high: TierConfig = TierConfig(max_depth=50, max_wait=120)
    normal: TierConfig = TierConfig(max_depth=100, max_wait=300)
    low: TierConfig = TierConfig(max_depth=200, max_wait=600)
    overflow_status_code: Literal[503, 429] = 503


class WebhookConfig(BaseModel):
    enabled: bool = False
    url: str = ""
    events: list[str] = [
        "queue.full",
        "queue.high_watermark",
        "queue.drained",
        "host.unhealthy",
        "host.recovered",
    ]
    allowed_hosts: list[str] = []  # hostnames exempt from SSRF check (for internal ntfy etc.)
    max_pending: int = 100
    max_concurrent: int = 2
    delivery_timeout: int = 5

    @field_validator("max_pending", "max_concurrent", "delivery_timeout")
    @classmethod
    def positive_limits(cls, v: int) -> int:
        if v < 1:
            raise ValueError("webhook queue and timeout limits must be positive")
        return v


class ApiKeyConfig(BaseModel):
    key: str | None = None
    key_env: str | None = None
    client_id: str
    description: str | None = None
    max_priority: Literal["high", "normal", "low"] = "normal"
    management: bool = False
    observability: bool = False
    max_concurrent: int = 0  # 0 = unlimited (subject to proxy.max_concurrent)

    @field_validator("max_concurrent")
    @classmethod
    def non_negative_concurrent(cls, v: int) -> int:
        if v < 0:
            raise ValueError(
                f"auth.keys[].max_concurrent must be a non-negative integer, got {v}"
            )
        return v

    @model_validator(mode="after")
    def exactly_one_key_source(self) -> "ApiKeyConfig":
        if self.key and self.key_env:
            raise ValueError("auth key entries cannot set both key and key_env")
        if not self.key:
            raise ValueError("auth key is missing; use key or key_env")
        if "${" in self.key:
            raise ValueError(
                "unresolved ${...} auth key; use key_env to read a key from the environment"
            )
        return self


class RateLimitConfig(BaseModel):
    max_failures: int = 10
    window_seconds: int = 60
    max_sources: int = 10000

    @field_validator("max_failures", "window_seconds", "max_sources")
    @classmethod
    def positive_limits(cls, v: int) -> int:
        if v < 1:
            raise ValueError("authentication rate-limit values must be positive")
        return v


class AuthConfig(BaseModel):
    enabled: bool = False
    keys: list[ApiKeyConfig] = []
    rate_limit: RateLimitConfig = RateLimitConfig()

    @model_validator(mode="after")
    def keys_required_when_enabled(self) -> "AuthConfig":
        if self.enabled and len(self.keys) == 0:
            print(
                "FATAL: auth.enabled is true but no API keys are configured. "
                "Add at least one key to auth.keys or set auth.enabled: false.",
                file=sys.stderr,
            )
            sys.exit(1)
        return self


class LoggingConfig(BaseModel):
    level: str = "info"
    format: Literal["json", "text"] = "json"


class ProxyConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 11435
    max_concurrent: int = 2
    allow_model_management: bool = False
    drain_timeout: int = 30
    max_request_body_mb: int = 50
    max_buffered_request_mb: int = 256
    max_response_body_mb: int = 64
    max_ingress_concurrent: int = 32
    body_read_timeout: int = 30
    allow_unauthenticated_public: bool = False

    @field_validator("port")
    @classmethod
    def valid_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError(f"Invalid port: {v}")
        return v

    @field_validator(
        "max_concurrent",
        "drain_timeout",
        "max_request_body_mb",
        "max_buffered_request_mb",
        "max_response_body_mb",
        "max_ingress_concurrent",
        "body_read_timeout",
    )
    @classmethod
    def positive_limits(cls, v: int) -> int:
        if v < 1:
            raise ValueError("proxy limits and timeouts must be positive")
        return v


# ---------------------------------------------------------------------------
# v0.2.0 — new config sections
# ---------------------------------------------------------------------------


class InjectionListenerConfig(BaseModel):
    listen_port: int
    inject_as: str  # must match an auth.keys[].client_id
    bind: str = "127.0.0.1"

    @field_validator("listen_port")
    @classmethod
    def valid_listen_port(cls, v: int) -> int:
        if not (1024 <= v <= 65535):
            raise ValueError(
                f"client_injection.listeners[].listen_port must be in 1024–65535, got {v}"
            )
        return v


class ClientInjectionConfig(BaseModel):
    listeners: list[InjectionListenerConfig] = []
    allow_public_injection: bool = False


class RoutingConfig(BaseModel):
    strategy: Literal["model_aware", "round_robin"] = "round_robin"
    fallback: Literal["any_healthy"] = "any_healthy"
    model_poll_timeout: int = 3

    @field_validator("model_poll_timeout")
    @classmethod
    def positive_timeout(cls, v: int) -> int:
        if v < 1:
            raise ValueError("routing.model_poll_timeout must be positive")
        return v


class EmbeddingCacheConfig(BaseModel):
    enabled: bool = False
    backend: str = "redis://localhost:6379/0"
    ttl: int = 86400
    max_entry_bytes: int = 32768
    key_prefix: str = "oqp:embed:"
    connect_timeout: int = 2

    @field_validator("ttl", "max_entry_bytes", "connect_timeout")
    @classmethod
    def positive_limits(cls, v: int) -> int:
        if v < 1:
            raise ValueError("embedding cache limits must be positive")
        return v


class KeepAliveConfig(BaseModel):
    default: str = "5m"
    override: bool = False


class Config(BaseModel):
    proxy: ProxyConfig = ProxyConfig()
    ollama: OllamaConfig
    queue: QueueConfig = QueueConfig()
    webhooks: WebhookConfig = WebhookConfig()
    auth: AuthConfig = AuthConfig()
    logging: LoggingConfig = LoggingConfig()
    # v0.2.0 sections
    client_injection: ClientInjectionConfig = ClientInjectionConfig()
    routing: RoutingConfig = RoutingConfig()
    embedding_cache: EmbeddingCacheConfig = EmbeddingCacheConfig()
    keep_alive: KeepAliveConfig = KeepAliveConfig()

    @model_validator(mode="after")
    def validate_v2_constraints(self) -> "Config":
        self._validate_injection_ports()
        self._validate_inject_as_refs()
        self._validate_client_max_concurrent()
        self._validate_public_injection_bind()
        self._warn_public_injection_no_auth()
        self._validate_unique_identities()
        self._validate_public_auth()
        return self

    def _validate_unique_identities(self) -> None:
        keys = [k.key for k in self.auth.keys]
        client_ids = [k.client_id for k in self.auth.keys]
        if len(keys) != len(set(keys)):
            raise ValueError("auth.keys contains duplicate API keys")
        if len(client_ids) != len(set(client_ids)):
            raise ValueError("auth.keys contains duplicate client_id values")
        host_names = [host.name for host in self.ollama.hosts]
        if len(host_names) != len(set(host_names)):
            raise ValueError("ollama.hosts contains duplicate name values")

    def _validate_public_auth(self) -> None:
        try:
            is_loopback = ipaddress.ip_address(self.proxy.host).is_loopback
        except ValueError:
            is_loopback = self.proxy.host == "localhost"
        if (
            not is_loopback
            and not self.auth.enabled
            and not self.proxy.allow_unauthenticated_public
        ):
            raise ValueError(
                "refusing unauthenticated non-loopback proxy bind; enable auth or set "
                "proxy.allow_unauthenticated_public: true as an explicit exception"
            )

    def _validate_injection_ports(self) -> None:
        seen: set[int] = {self.proxy.port}
        for listener in self.client_injection.listeners:
            if listener.listen_port in seen:
                print(
                    f"FATAL: client_injection.listeners[].listen_port {listener.listen_port} "
                    f"conflicts with another port (proxy.port or another injection listener).",
                    file=sys.stderr,
                )
                sys.exit(1)
            seen.add(listener.listen_port)

    def _validate_inject_as_refs(self) -> None:
        known_ids = {k.client_id for k in self.auth.keys}
        for listener in self.client_injection.listeners:
            if listener.inject_as not in known_ids:
                print(
                    f"FATAL: client_injection.listeners[].inject_as '{listener.inject_as}' "
                    f"does not match any auth.keys[].client_id. Known IDs: {sorted(known_ids)}",
                    file=sys.stderr,
                )
                sys.exit(1)

    def _validate_client_max_concurrent(self) -> None:
        global_cap = self.proxy.max_concurrent
        for key in self.auth.keys:
            if key.max_concurrent > global_cap:
                print(
                    f"FATAL: auth.keys[client_id={key.client_id}].max_concurrent "
                    f"({key.max_concurrent}) exceeds proxy.max_concurrent ({global_cap}). "
                    f"Set max_concurrent <= {global_cap} or increase proxy.max_concurrent.",
                    file=sys.stderr,
                )
                sys.exit(1)

    def _validate_public_injection_bind(self) -> None:
        loopback = {"127.0.0.1", "localhost", "::1"}
        for listener in self.client_injection.listeners:
            if listener.bind in loopback:
                continue
            if not self.client_injection.allow_public_injection:
                print(
                    f"FATAL: client_injection.listeners[listen_port={listener.listen_port}].bind "
                    f"is '{listener.bind}' (non-loopback) but allow_public_injection is false. "
                    "Set allow_public_injection: true to confirm exposing an unauthenticated "
                    "injection port on the network, or change bind to 127.0.0.1.",
                    file=sys.stderr,
                )
                sys.exit(1)

    def _warn_public_injection_no_auth(self) -> None:
        loopback = {"127.0.0.1", "localhost", "::1"}
        has_non_loopback = any(
            listener.bind not in loopback
            for listener in self.client_injection.listeners
        )
        if self.client_injection.allow_public_injection and not self.auth.enabled:
            print(
                "WARNING: allow_public_injection is true AND auth.enabled is false. "
                "Injection ports will bind on all interfaces with no credential check — "
                "any host on the network can consume queue slots under an injected identity. "
                "Set auth.enabled: true or restrict allow_public_injection: false.",
                file=sys.stderr,
            )
        elif has_non_loopback:
            print(
                "WARNING: one or more client_injection.listeners bind to a non-loopback "
                "address. Injection ports bypass Bearer auth by design — any host able to "
                "reach that port can consume queue slots under the injected client identity. "
                "Restrict access at the firewall / reverse proxy layer.",
                file=sys.stderr,
            )


def _apply_env_overrides(data: dict, prefix: str = "OQP") -> dict:
    """Apply OQP_ env var overrides onto the raw config dict using __ nesting."""
    for key, value in os.environ.items():
        if not key.startswith(prefix + "_"):
            continue
        parts = key[len(prefix) + 1 :].lower().split("__")
        # List-index overrides (e.g. OQP_OLLAMA__HOSTS__0__URL) are not
        # supported — skip the entire key if any component is a digit.
        if any(p.isdigit() for p in parts):
            continue
        target = data
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        leaf = parts[-1]
        # Attempt type coercion for booleans and integers
        if value.lower() in ("true", "false"):
            target[leaf] = value.lower() == "true"
        elif value.isdigit():
            target[leaf] = int(value)
        else:
            target[leaf] = value
    return data


def load_config(path: str | None = None) -> Config:
    """Load configuration from YAML file with env var overrides."""
    config_path = path or os.environ.get("OQP_CONFIG", "./config.yml")
    try:
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}
    except FileNotFoundError:
        print(
            f"FATAL: Config file not found: {config_path}. "
            "Copy config.example.yml to config.yml and edit it.",
            file=sys.stderr,
        )
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"FATAL: Config file parse error: {e}", file=sys.stderr)
        sys.exit(1)

    raw = _apply_env_overrides(raw)
    for entry in raw.get("auth", {}).get("keys", []):
        key_env = entry.get("key_env")
        if key_env:
            if entry.get("key"):
                print(
                    "FATAL: an auth key entry cannot set both key and key_env",
                    file=sys.stderr,
                )
                sys.exit(1)
            value = os.environ.get(key_env)
            if not value:
                print(
                    f"FATAL: auth key environment variable {key_env!r} is unset or empty",
                    file=sys.stderr,
                )
                sys.exit(1)
            entry["key"] = value
            entry.pop("key_env", None)
    try:
        return Config.model_validate(raw)
    except Exception as e:
        print(f"FATAL: Config validation error: {e}", file=sys.stderr)
        sys.exit(1)
