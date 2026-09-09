"""Failover must preserve model identity and never replay a partial stream."""

import json
from contextlib import aclosing

import httpx
import pytest
from fastapi import Request

from ollama_queue_proxy.config import HostConfig, OllamaConfig, RoutingConfig
from ollama_queue_proxy.hosts import HostManager
from ollama_queue_proxy.proxy import dispatch_request
from ollama_queue_proxy.routing import RoutingTable
from tests.conftest import make_config


async def dispatch(handler, models, *, fallback="any_healthy", stream=False):
    config = make_config()
    config.ollama = OllamaConfig(hosts=[
        HostConfig(url=f"http://{name}:11434", name=name, weight=3)
        for name in models
    ])
    config.routing = RoutingConfig(strategy="model_aware", fallback=fallback)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    hosts = HostManager(config.ollama)
    table = RoutingTable(config.ollama, config.routing, client)
    for name, inventory in models.items():
        table._states[name].loaded_models = set(inventory)
    request = Request({"type": "http", "method": "POST", "path": "/v1/chat/completions",
                       "query_string": b"", "headers": []})
    request.state.request_id = "test-failover"
    response = await dispatch_request(
        request=request, body=json.dumps({"model": "coder", "stream": stream}).encode(),
        client_id=None, config=config, host_manager=hosts, client=client, routing_table=table,
    )
    return response, client


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError])
async def test_timeout_does_not_fall_back_to_host_without_model(caplog, error_type):
    calls = []

    def handler(request):
        calls.append(request.url.host)
        if request.url.host == "primary":
            raise error_type("", request=request)
        return httpx.Response(404, json={"error": "model not found"})

    response, client = await dispatch(handler, {"primary": ["coder"], "other": ["different"]})
    async with aclosing(client):
        assert response.status_code == 503
        assert response.headers["retry-after"] == "5"
        assert calls == ["primary"]
        assert error_type.__name__ in caplog.text


@pytest.mark.asyncio
async def test_failover_uses_second_matching_host_despite_weight():
    calls = []

    def handler(request):
        calls.append(request.url.host)
        if request.url.host == "primary":
            raise httpx.ConnectError("unavailable", request=request)
        return httpx.Response(200, json={"result": "ok"})

    response, client = await dispatch(handler, {"primary": ["coder"], "second": ["coder"]})
    async with aclosing(client):
        assert response.status_code == 200
        assert calls == ["primary", "second"]
        assert response.headers["x-failover-host"] == "second"


@pytest.mark.asyncio
async def test_strict_routing_does_not_contact_incompatible_host():
    calls = []

    def handler(request):
        calls.append(request.url.host)
        return httpx.Response(404)

    response, client = await dispatch(handler, {"other": ["different"]}, fallback="error")
    async with aclosing(client):
        assert response.status_code == 503
        assert calls == []


@pytest.mark.asyncio
async def test_legacy_initial_fallback_remains_available():
    response, client = await dispatch(lambda _: httpx.Response(200, json={"ok": True}),
                                      {"unknown": []})
    async with aclosing(client):
        assert response.status_code == 200


@pytest.mark.asyncio
async def test_partial_stream_timeout_is_not_replayed_and_closes(caplog):
    class BrokenStream(httpx.AsyncByteStream):
        closed = False

        async def __aiter__(self):
            yield b'data: {"partial":true}\n\n'
            raise httpx.ReadTimeout("stalled stream")

        async def aclose(self):
            self.closed = True

    stream = BrokenStream()
    calls = []

    def handler(request):
        calls.append(request.url.host)
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)

    response, client = await dispatch(handler, {"primary": ["coder"], "second": ["coder"]},
                                      stream=True)
    async with aclosing(client):
        chunks = []
        with pytest.raises(httpx.ReadTimeout):
            async for chunk in response.body_iterator:
                chunks.append(chunk)
        assert chunks == [b'data: {"partial":true}\n\n']
        assert calls == ["primary"]
        assert stream.closed
        assert response._oqp_completion.done()
        assert "proxy.stream_failed" in caplog.text
        assert "ReadTimeout" in caplog.text


def test_round_robin_excludes_unreachable_and_already_attempted_hosts():
    config = OllamaConfig(hosts=[HostConfig(url=f"http://{name}", name=name, weight=3)
                                for name in ["a", "b", "c"]])
    table = RoutingTable(config, RoutingConfig(strategy="round_robin"), None)
    table._states["a"].reachable = False
    assert table.pick(None, exclude={"b"}).name == "c"
