"""Tests for keep_alive injection and per-client concurrency caps."""

from __future__ import annotations

import asyncio
import json

import pytest

from ollama_queue_proxy.config import ApiKeyConfig
from ollama_queue_proxy.concurrency import ClientConcurrencyManager
from ollama_queue_proxy.main import _inject_keep_alive


# ---------------------------------------------------------------------------
# keep_alive injection
# ---------------------------------------------------------------------------


def _body(**kwargs) -> bytes:
    return json.dumps(kwargs, separators=(",", ":")).encode()


def _parsed(b: bytes) -> dict:
    return json.loads(b)


def test_inject_keep_alive_when_missing():
    body = _body(model="llama3", prompt="hello")
    result = _inject_keep_alive(body, "5m", override=False, max_body_mb=50)
    data = _parsed(result)
    assert data["keep_alive"] == "5m"
    assert data["model"] == "llama3"  # existing fields preserved


def test_inject_keep_alive_respected_when_present_no_override():
    body = _body(model="llama3", prompt="hello", keep_alive="10m")
    result = _inject_keep_alive(body, "5m", override=False, max_body_mb=50)
    data = _parsed(result)
    assert data["keep_alive"] == "10m"  # client value preserved


def test_inject_keep_alive_replaced_when_override_true():
    body = _body(model="llama3", prompt="hello", keep_alive="10m")
    result = _inject_keep_alive(body, "5m", override=True, max_body_mb=50)
    data = _parsed(result)
    assert data["keep_alive"] == "5m"  # proxy default wins


def test_inject_keep_alive_non_json_passthrough():
    body = b"not json at all"
    result = _inject_keep_alive(body, "5m", override=False, max_body_mb=50)
    assert result == body


def test_inject_keep_alive_empty_body_passthrough():
    result = _inject_keep_alive(b"", "5m", override=False, max_body_mb=50)
    assert result == b""


def test_inject_keep_alive_oversized_body_skipped():
    big_body = json.dumps({"model": "llama3", "data": "x" * 1000}).encode()
    result = _inject_keep_alive(big_body, "5m", override=False, max_body_mb=0)
    # max_body_mb=0 means 0 bytes threshold → body not mutated
    assert result == big_body


def test_inject_keep_alive_non_dict_json_passthrough():
    body = json.dumps([1, 2, 3]).encode()
    result = _inject_keep_alive(body, "5m", override=False, max_body_mb=50)
    assert result == body


# ---------------------------------------------------------------------------
# ClientConcurrencyManager — unlimited client
# ---------------------------------------------------------------------------


def make_key(client_id: str, max_concurrent: int = 0) -> ApiKeyConfig:
    return ApiKeyConfig(key="k", client_id=client_id, max_concurrent=max_concurrent)


def test_unlimited_client_never_blocks():
    mgr = ClientConcurrencyManager([make_key("svc", max_concurrent=0)])
    # Should return immediately without blocking
    for _ in range(10):
        assert mgr.try_acquire("svc")
    assert mgr.inflight_counts()["svc"] == 10
    for _ in range(10):
        mgr.release("svc")
    assert mgr.inflight_counts()["svc"] == 0


def test_unknown_client_acquire_no_error():
    mgr = ClientConcurrencyManager([])
    assert mgr.try_acquire("ghost")
    mgr.release("ghost")


# ---------------------------------------------------------------------------
# ClientConcurrencyManager — capped client
# ---------------------------------------------------------------------------


def test_capped_client_rejects_nth_immediate_acquire():
    mgr = ClientConcurrencyManager([make_key("batch", max_concurrent=2)])

    assert mgr.try_acquire("batch")
    assert mgr.try_acquire("batch")
    assert not mgr.try_acquire("batch")
    mgr.release("batch")
    assert mgr.try_acquire("batch")


def test_cap_waiting_is_set_by_scheduler():
    mgr = ClientConcurrencyManager([make_key("batch", max_concurrent=1)])
    mgr.set_waiting_counts({"batch": 2})
    assert mgr.cap_waiting_counts()["batch"] == 2


def test_release_decrements_inflight():
    mgr = ClientConcurrencyManager([make_key("svc", max_concurrent=3)])
    assert mgr.try_acquire("svc")
    assert mgr.try_acquire("svc")
    assert mgr.inflight_counts()["svc"] == 2
    mgr.release("svc")
    assert mgr.inflight_counts()["svc"] == 1


# ---------------------------------------------------------------------------
# Caps are strict
# ---------------------------------------------------------------------------


def test_cap_has_no_fairness_bypass():
    mgr = ClientConcurrencyManager([make_key("batch", max_concurrent=1)])
    assert mgr.try_acquire("batch")
    assert not mgr.try_acquire("batch")


# ---------------------------------------------------------------------------
# Priority isolation: different clients don't share semaphores
# ---------------------------------------------------------------------------


def test_different_clients_independent_caps():
    mgr = ClientConcurrencyManager([
        make_key("batch", max_concurrent=1),
        make_key("interactive", max_concurrent=2),
    ])

    assert mgr.try_acquire("batch")
    assert not mgr.try_acquire("batch")
    assert mgr.try_acquire("interactive")
