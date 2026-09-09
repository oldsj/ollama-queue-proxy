"""Core async HTTP proxy with streaming, body buffering, and failover."""

from __future__ import annotations

import asyncio
import json
import logging

import anyio
import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from .config import Config
from .hosts import HostManager, OllamaHost
from .routing import RoutingTable

logger = logging.getLogger(__name__)


async def _close_stream_response(
    response: httpx.Response, completion: asyncio.Future[None]
) -> None:
    """Close an upstream stream despite downstream cancellation, then release its slot."""
    try:
        with anyio.move_on_after(5, shield=True):
            await response.aclose()
    finally:
        if not completion.done():
            completion.set_result(None)

# Ollama model management endpoints — blocked by default
_MODEL_MANAGEMENT_PATHS = {
    "/api/pull",
    "/api/push",
    "/api/delete",
    "/api/create",
    "/api/copy",
    "/api/signout",
    "/api/experimental/web_search",
    "/api/experimental/web_fetch",
}


def is_model_management_request(method: str, path: str) -> bool:
    """Return whether an Ollama request mutates model/blob state."""
    normalized = "/" + path.lstrip("/").rstrip("/")
    method = method.upper()
    if normalized == "/api/delete":
        return method == "DELETE"
    if normalized.startswith("/api/user/keys/"):
        return method == "DELETE"
    if method == "POST" and normalized == "/api/me":
        return True
    return method == "POST" and (
        normalized in _MODEL_MANAGEMENT_PATHS
        or normalized.startswith("/api/blobs/")
    )

# Headers to strip before forwarding to Ollama
_STRIP_REQUEST_HEADERS = {
    "x-queue-priority",
    "authorization",  # proxy auth must NOT reach Ollama
    "host",
    "content-length",  # httpx will recalculate
    "transfer-encoding",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "upgrade",
}

# Headers to strip from upstream before building a non-streaming JSONResponse.
# JSONResponse re-serialises the body (dropping any trailing newline Ollama appends),
# so it must compute content-length itself — passing the upstream value causes an
# off-by-one. transfer-encoding is hop-by-hop and invalid on a buffered response.
_STRIP_RESPONSE_HEADERS = {
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "upgrade",
}


def _connection_tokens(headers: httpx.Headers) -> set[str]:
    tokens: set[str] = set()
    for value in headers.get_list("connection"):
        tokens.update(part.strip().lower() for part in value.split(",") if part.strip())
    return tokens


def model_management_error(
    method: str,
    path: str,
    config: Config,
    management: bool,
    request_id: str,
) -> JSONResponse | None:
    if not is_model_management_request(method, path):
        return None
    if config.proxy.allow_model_management and config.auth.enabled and management:
        return None
    return JSONResponse(
        status_code=403,
        content={
            "error": "model management requires both server enablement and a management key",
            "request_id": request_id,
        },
    )


def extract_model(body: bytes) -> str | None:
    """Extract the 'model' field from a JSON request body."""
    if not body:
        return None
    try:
        data = json.loads(body)
        return data.get("model") if isinstance(data, dict) else None
    except (json.JSONDecodeError, ValueError):
        return None


async def read_body(request: Request, max_mb: int) -> tuple[bytes, JSONResponse | None]:
    """
    Buffer the full request body. Returns (body_bytes, None) or (b'', error_response).
    Checks Content-Length first; falls back to incremental read with size check.
    """
    max_bytes = max_mb * 1024 * 1024
    request_id = getattr(request.state, "request_id", "unknown")

    content_length = request.headers.get("Content-Length")
    if content_length:
        try:
            cl = int(content_length)
            if cl < 0:
                raise ValueError
            if cl > max_bytes:
                return b"", JSONResponse(
                    status_code=413,
                    content={"error": "request body too large", "request_id": request_id},
                )
        except ValueError:
            return b"", JSONResponse(
                status_code=400,
                content={"error": "invalid content-length", "request_id": request_id},
            )

    chunks = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > max_bytes:
            return b"", JSONResponse(
                status_code=413,
                content={"error": "request body too large", "request_id": request_id},
            )
        chunks.append(chunk)
    return b"".join(chunks), None


async def dispatch_request(
    request: Request,
    body: bytes,
    client_id: str | None,
    config: Config,
    host_manager: HostManager,
    client: httpx.AsyncClient,
    routing_table: RoutingTable | None = None,
    path_override: str | None = None,
    management: bool = False,
) -> Response:
    """
    Dispatch a buffered request to the appropriate Ollama host with failover.
    Failover only applies before any response bytes are sent to the client.
    """
    request_id = getattr(request.state, "request_id", "unknown")
    path = path_override if path_override is not None else request.url.path
    query = request.url.query
    method = request.method

    management_err = model_management_error(
        method, path, config, management=management, request_id=request_id
    )
    if management_err:
        return management_err

    # Build forwarded headers — strip proxy-specific ones
    forward_headers: dict[str, str] = {}
    request_connection_tokens = {
        part.strip().lower()
        for value in request.headers.getlist("connection")
        for part in value.split(",")
        if part.strip()
    }
    for k, v in request.headers.items():
        if k.lower() not in _STRIP_REQUEST_HEADERS | request_connection_tokens:
            forward_headers[k.lower()] = v
    if client_id:
        forward_headers["x-client-id"] = client_id

    model = extract_model(body)

    # Build candidate host list — routing table (model_aware) or HostManager fallback
    def _next_host() -> OllamaHost | None:
        if routing_table is not None:
            # Never retry a model request on an incompatible backend after failure.
            rt_state = routing_table.pick(
                model, exclude=attempted, allow_fallback=not attempted,
            )
            if rt_state is None:
                return None
            # Map routing state back to OllamaHost object for failover tracking
            for h in host_manager.hosts:
                if h.name == rt_state.name:
                    return h
            return None
        # Default: first healthy host (HostManager order, v0.1.x behaviour)
        for h in host_manager.hosts:
            if not h.healthy or h.name in attempted:
                continue
            if model and h.models and model not in h.models:
                continue
            return h
        return None

    last_error: str | None = None
    attempted: set[str] = set()

    while True:
        host = _next_host()
        if host is None or host.name in attempted:
            break
        attempted.add(host.name)

        resp: httpx.Response | None = None
        try:
            upstream_request = client.build_request(
                method=method,
                url=f"{host.url}{path}" + (f"?{query}" if query else ""),
                headers=forward_headers,
                content=body or None,
                timeout=config.ollama.request_timeout,
            )
            resp = await client.send(upstream_request, stream=True)
            host.requests_handled += 1

            # Check if this is a streaming response.
            # Ollama uses application/x-ndjson for streaming generate/chat and
            # text/event-stream for some endpoints. Chunked transfer-encoding is
            # a transport concern and is NOT a reliable streaming indicator —
            # /api/embed returns application/json with chunked TE even though the
            # response is a single JSON object. Only the content-type identifies
            # true streaming responses.
            content_type = resp.headers.get("content-type", "")
            is_streaming = (
                "text/event-stream" in content_type
                or "application/x-ndjson" in content_type
            )

            response_connection_tokens = _connection_tokens(resp.headers)
            passthrough_headers = {
                k: v
                for k, v in resp.headers.items()
                if k.lower() not in _STRIP_RESPONSE_HEADERS | response_connection_tokens
            }
            response_headers = {"X-Failover-Host": host.name}

            if is_streaming:
                completion = asyncio.get_running_loop().create_future()

                async def close_stream(r=resp):
                    await _close_stream_response(r, completion)

                async def stream_gen(r=resp, stream_host=host):
                    try:
                        async for chunk in r.aiter_bytes():
                            yield chunk
                    except httpx.TransportError as exc:
                        # Headers/data may already be sent. Replaying would duplicate
                        # generated content or tool calls; leave the stream failed.
                        logger.warning(
                            "proxy.stream_failed host=%s request_id=%s error_type=%s",
                            stream_host.name, request_id, type(exc).__name__,
                        )
                        raise
                    finally:
                        # Close the httpx response explicitly — if the client
                        # disconnects mid-stream the generator is abandoned and
                        # GC may never run, leaking the underlying connection.
                        await close_stream(r)

                response = StreamingResponse(
                    stream_gen(),
                    status_code=resp.status_code,
                    headers={**passthrough_headers, **response_headers},
                    media_type=resp.headers.get("content-type"),
                )
                response._oqp_completion = completion  # type: ignore[attr-defined]
                response._oqp_abort = close_stream  # type: ignore[attr-defined]
                return response
            else:
                max_response_bytes = config.proxy.max_response_body_mb * 1024 * 1024
                chunks: list[bytes] = []
                size = 0
                async for chunk in resp.aiter_bytes():
                    size += len(chunk)
                    if size > max_response_bytes:
                        await resp.aclose()
                        return JSONResponse(
                            status_code=502,
                            content={
                                "error": "upstream response body too large",
                                "request_id": request_id,
                            },
                        )
                    chunks.append(chunk)
                raw_body = b"".join(chunks)
                await resp.aclose()

                # Fast-path routing invalidation after safely reading a bounded body.
                if resp.status_code == 404 and model and routing_table is not None:
                    try:
                        err_body = json.loads(raw_body)
                        if "not found" in err_body.get("error", "").lower():
                            routing_table.invalidate(host.name, model)
                    except (json.JSONDecodeError, AttributeError):
                        pass

                ct = resp.headers.get("content-type", "")
                if ct.startswith("application/json"):
                    try:
                        content = json.loads(raw_body)
                    except json.JSONDecodeError:
                        return Response(
                            content=raw_body,
                            status_code=resp.status_code,
                            headers={**passthrough_headers, **response_headers},
                            media_type=ct,
                        )
                    return JSONResponse(
                        status_code=resp.status_code,
                        content=content,
                        headers={**passthrough_headers, **response_headers},
                    )
                return Response(
                    content=raw_body,
                    status_code=resp.status_code,
                    headers={**passthrough_headers, **response_headers},
                    media_type=ct or None,
                )

        except (httpx.TransportError, httpx.HTTPStatusError) as e:
            if resp is not None:
                await resp.aclose()
            last_error = f"{type(e).__name__}: {e}"
            host_manager.mark_unhealthy(host, last_error)
            if routing_table is not None:
                # Mark host unreachable in routing table too
                rt_state = routing_table._states.get(host.name)
                if rt_state:
                    rt_state.reachable = False
            logger.warning(
                "proxy.failover host=%s error=%s trying_next=true", host.name, last_error
            )
            continue

    return JSONResponse(
        status_code=503,
        content={"error": "all upstream hosts failed", "request_id": request_id},
        headers={"X-Failover-Exhausted": "true", "Retry-After": "5"},
    )
