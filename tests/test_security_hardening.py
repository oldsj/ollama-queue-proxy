"""Regression tests for security review findings."""

from __future__ import annotations

import asyncio
import socket
import time
from types import SimpleNamespace

import anyio
import httpx
import pytest
import yaml
from fastapi import Request
from starlette.requests import ClientDisconnect

from ollama_queue_proxy.auth import AuthManager
from ollama_queue_proxy.cache import BoundedCounter, _embed_key, _redact_url
from ollama_queue_proxy.concurrency import ClientConcurrencyManager
from ollama_queue_proxy.config import (
    ApiKeyConfig,
    AuthConfig,
    QueueConfig,
    RateLimitConfig,
    TierConfig,
    WebhookConfig,
    load_config,
)
from ollama_queue_proxy.main import _inject_keep_alive, _read_reserved_body
from ollama_queue_proxy.proxy import (
    _close_stream_response,
    is_model_management_request,
    model_management_error,
)
from ollama_queue_proxy.queue import (
    PriorityQueueManager,
    QueueFull,
    QueueItem,
    QueuePaused,
)
from ollama_queue_proxy.webhooks import WebhookManager, validate_webhook_url
from tests.conftest import make_config


def _write_config(tmp_path, data: dict) -> str:
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump(data))
    return str(path)


def _request(key: str | None, client: str = "10.0.0.2") -> Request:
    headers = []
    if key is not None:
        headers.append((b"authorization", f"Bearer {key}".encode()))
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/chat",
            "query_string": b"",
            "headers": headers,
            "client": (client, 1234),
        }
    )
    request.state.request_id = "security-test"
    return request


def test_key_env_is_resolved_without_literal_interpolation(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OQP_ADMIN_KEY", "actual-secret-key")
    path = _write_config(
        tmp_path,
        {
            "ollama": {"hosts": [{"url": "http://ollama:11434", "name": "primary"}]},
            "auth": {
                "enabled": True,
                "keys": [{"key_env": "TEST_OQP_ADMIN_KEY", "client_id": "admin"}],
            },
        },
    )
    assert load_config(path).auth.keys[0].key == "actual-secret-key"


def test_unresolved_shell_style_key_is_rejected(tmp_path):
    path = _write_config(
        tmp_path,
        {
            "ollama": {"hosts": [{"url": "http://ollama:11434", "name": "primary"}]},
            "auth": {
                "enabled": True,
                "keys": [{"key": "${ADMIN_API_KEY}", "client_id": "admin"}],
            },
        },
    )
    with pytest.raises(SystemExit):
        load_config(path)


def test_unauthenticated_public_bind_requires_explicit_exception(tmp_path):
    path = _write_config(
        tmp_path,
        {
            "proxy": {"host": "0.0.0.0"},
            "ollama": {"hosts": [{"url": "http://ollama:11434", "name": "primary"}]},
        },
    )
    with pytest.raises(SystemExit):
        load_config(path)


@pytest.mark.asyncio
async def test_valid_key_bypasses_shared_source_failure_bucket():
    key = ApiKeyConfig(key="valid-secret", client_id="owner")
    manager = AuthManager(
        AuthConfig(
            enabled=True,
            keys=[key],
            rate_limit=RateLimitConfig(max_failures=3, window_seconds=60),
        )
    )
    for _ in range(4):
        await manager.authenticate(_request("wrong"))
    key_cfg, error = await manager.authenticate(_request("valid-secret"))
    assert error is None
    assert key_cfg is key


@pytest.mark.asyncio
async def test_pause_race_releases_body_reservation():
    manager = PriorityQueueManager(QueueConfig(), max_concurrent=1)
    reservation = await manager.reserve("low", 1024)
    manager.pause("low")

    async def dispatch():
        return None

    item = QueueItem(
        tier="low",
        enqueue_time=time.monotonic(),
        request_id="paused",
        future=asyncio.get_running_loop().create_future(),
        dispatch_fn=dispatch,
    )
    with pytest.raises(QueuePaused):
        await manager.enqueue(item, reservation)
    assert manager.buffered_body_bytes() == 0


@pytest.mark.asyncio
async def test_disconnect_before_stream_handoff_aborts_and_releases_resources():
    manager = PriorityQueueManager(QueueConfig(), max_concurrent=1)
    started = asyncio.Event()
    finish_dispatch = asyncio.Event()
    aborted = asyncio.Event()
    completion = asyncio.get_running_loop().create_future()

    class StreamingResult:
        _oqp_completion = completion

        async def _oqp_abort(self):
            aborted.set()
            if not completion.done():
                completion.set_result(None)

    async def dispatch():
        started.set()
        await finish_dispatch.wait()
        return StreamingResult()

    future = asyncio.get_running_loop().create_future()
    item = QueueItem(
        tier="low",
        enqueue_time=time.monotonic(),
        request_id="disconnected-stream",
        future=future,
        dispatch_fn=dispatch,
        body_size=100,
    )
    await manager.enqueue(item)
    manager.start_workers()
    try:
        await started.wait()
        future.cancel()
        finish_dispatch.set()
        await asyncio.wait_for(aborted.wait(), timeout=1)
        while manager.active_count():
            await asyncio.sleep(0)
        assert manager.buffered_body_bytes() == 0
    finally:
        await manager.stop_workers()


@pytest.mark.asyncio
async def test_midstream_cancellation_still_closes_and_completes():
    close_started = asyncio.Event()
    allow_close = asyncio.Event()
    close_finished = asyncio.Event()
    completion = asyncio.get_running_loop().create_future()

    class SlowResponse:
        async def aclose(self):
            close_started.set()
            await allow_close.wait()
            close_finished.set()

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(_close_stream_response, SlowResponse(), completion)
        await close_started.wait()
        tasks.cancel_scope.cancel()
        with anyio.CancelScope(shield=True):
            await asyncio.sleep(0)
            assert not completion.done()
            allow_close.set()
            await asyncio.wait_for(close_finished.wait(), timeout=1)

    assert completion.done()


@pytest.mark.asyncio
async def test_partial_upload_disconnect_releases_reservation():
    messages = iter(
        (
            {"type": "http.request", "body": b"partial", "more_body": True},
            {"type": "http.disconnect"},
        )
    )

    async def receive():
        return next(messages)

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/chat",
            "query_string": b"",
            "headers": [],
        },
        receive,
    )
    manager = PriorityQueueManager(QueueConfig(), max_concurrent=1)
    reservation = await manager.reserve("low", 1024)
    state = SimpleNamespace(
        ingress_semaphore=asyncio.Semaphore(1),
        queue_manager=manager,
        config=SimpleNamespace(
            proxy=SimpleNamespace(max_request_body_mb=1, body_read_timeout=1)
        ),
    )

    with pytest.raises(ClientDisconnect):
        await _read_reserved_body(request, state, reservation, "upload-disconnect")
    assert manager.buffered_body_bytes() == 0


def test_blob_upload_is_model_management_and_requires_management_key():
    assert is_model_management_request("POST", "/api/blobs/sha256:deadbeef")
    assert is_model_management_request("DELETE", "/api/delete")
    assert is_model_management_request("POST", "/api/signout")
    assert is_model_management_request("DELETE", "/api/user/keys/example")
    assert is_model_management_request("POST", "/api/experimental/web_search")
    assert is_model_management_request("POST", "/api/experimental/web_fetch")
    assert is_model_management_request("POST", "/api/me")
    assert not is_model_management_request("POST", "/api/delete")
    cfg = make_config(auth_enabled=True, keys=[ApiKeyConfig(key="k", client_id="user")])
    cfg.proxy.allow_model_management = True
    assert model_management_error("POST", "/api/blobs/x", cfg, False, "r") is not None
    assert model_management_error("POST", "/api/blobs/x", cfg, True, "r") is None
    assert not is_model_management_request("GET", "/api/tags")


def test_non_management_keep_alive_override_is_restricted():
    body = b'{"model":"shared","keep_alive":0}'
    updated = _inject_keep_alive(body, "5m", False, 1, restrict_override=True)
    assert b'"keep_alive":"5m"' in updated


def test_embedding_cache_key_includes_semantic_options_and_tenant():
    base = {"model": "embed", "input": "hello", "dimensions": 128}
    assert _embed_key("p:", "embed", base, "alice") != _embed_key(
        "p:", "embed", {**base, "dimensions": 256}, "alice"
    )
    assert _embed_key("p:", "embed", base, "alice") != _embed_key(
        "p:", "embed", base, "bob"
    )
    assert _embed_key("p:", "embed", base, "alice") == _embed_key(
        "p:", "embed", {**base, "keep_alive": "5m"}, "alice"
    )


def test_metric_counter_cardinality_is_bounded():
    counter = BoundedCounter(max_keys=2)
    for key in ("a", "b", "c", "d"):
        counter.increment(key)
    assert len(counter) <= 3
    assert counter["__overflow__"] == 2


def test_redis_url_redaction_removes_credentials_and_query():
    rendered = _redact_url("redis://user:password@cache.internal:6379/2?secret=x")
    assert rendered == "redis://cache.internal:6379/2"
    assert "password" not in rendered


@pytest.mark.parametrize(
    "url",
    ["http://0.0.0.0/hook", "http://[::ffff:127.0.0.1]/hook", "http://100.64.0.1/hook"],
)
def test_webhook_rejects_all_non_global_addresses(url):
    with pytest.raises(ValueError, match="non-global"):
        validate_webhook_url(url)


@pytest.mark.asyncio
async def test_webhook_delivery_connects_to_validated_address(monkeypatch):
    def public_address(host, port, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", public_address)
    observed = {}

    async def handler(request: httpx.Request):
        observed["url"] = str(request.url)
        observed["host"] = request.headers["host"]
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        manager = WebhookManager(
            WebhookConfig(enabled=True, url="https://hooks.example.test/events"), client
        )
        await manager._deliver("queue.full", "low")

    assert observed["url"].startswith("https://93.184.216.34/")
    assert observed["host"] == "hooks.example.test"


@pytest.mark.asyncio
async def test_buffer_budget_is_reserved_before_body_read():
    manager = PriorityQueueManager(QueueConfig(), 1, max_buffered_body_bytes=10)
    reservation = await manager.reserve("normal", 8)
    with pytest.raises(QueueFull):
        await manager.reserve("normal", 4)
    manager.release_reservation(reservation)
    assert manager.buffered_body_bytes() == 0


def _queued_item(
    tier: str,
    request_id: str,
    client_id: str,
    dispatch,
    concurrency: ClientConcurrencyManager,
) -> QueueItem:
    future = asyncio.get_running_loop().create_future()
    return QueueItem(
        tier=tier,
        enqueue_time=time.monotonic(),
        request_id=request_id,
        future=future,
        dispatch_fn=dispatch,
        client_id=client_id,
        try_acquire=lambda: concurrency.try_acquire(client_id),
        release=lambda: concurrency.release(client_id),
    )


@pytest.mark.asyncio
async def test_capped_low_request_does_not_occupy_worker_needed_by_high_priority():
    config = QueueConfig(
        high=TierConfig(max_depth=5, max_wait=10),
        normal=TierConfig(max_depth=5, max_wait=10),
        low=TierConfig(max_depth=5, max_wait=10),
    )
    manager = PriorityQueueManager(config, max_concurrent=2)
    concurrency = ClientConcurrencyManager(
        [
            ApiKeyConfig(key="low", client_id="friend", max_concurrent=1),
            ApiKeyConfig(key="high", client_id="owner", max_concurrent=0),
        ]
    )
    release_first = asyncio.Event()
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    high_started = asyncio.Event()

    async def first():
        first_started.set()
        await release_first.wait()
        return "first"

    async def second():
        second_started.set()
        return "second"

    async def high():
        high_started.set()
        return "high"

    manager.start_workers()
    first_item = _queued_item("low", "low-1", "friend", first, concurrency)
    await manager.enqueue(first_item)
    await asyncio.wait_for(first_started.wait(), timeout=1)
    second_item = _queued_item("low", "low-2", "friend", second, concurrency)
    await manager.enqueue(second_item)
    await asyncio.sleep(0.05)
    assert not second_started.is_set()
    high_item = _queued_item("high", "high", "owner", high, concurrency)
    await manager.enqueue(high_item)
    await asyncio.wait_for(high_started.wait(), timeout=1)
    assert not second_started.is_set()
    release_first.set()
    await asyncio.wait_for(second_started.wait(), timeout=1)
    await manager.stop_workers()
