"""Webhook delivery: fire-and-forget event notifications."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx

from .config import WebhookConfig

logger = logging.getLogger(__name__)

def _check_private(addr: ipaddress.IPv4Address | ipaddress.IPv6Address, label: str) -> None:
    """Raise unless addr is globally routable, including mapped-address handling."""
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
        addr = addr.ipv4_mapped
    if not addr.is_global:
        raise ValueError(
            f"Webhook URL resolves to a private/non-global address "
            f"({label} -> {addr}). This is an SSRF risk. Use a public URL."
        )


def _resolve_public_addresses(host: str) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        try:
            results = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
        except socket.gaierror as e:
            raise ValueError(f"Webhook URL hostname cannot be resolved: {host} ({e})") from e
        addresses = list({ipaddress.ip_address(result[4][0]) for result in results})
    else:
        addresses = [literal]
    if not addresses:
        raise ValueError(f"Webhook URL hostname resolved to no addresses: {host}")
    for addr in addresses:
        _check_private(addr, host)
    return addresses


def validate_webhook_url(url: str, allowed_hosts: list[str] | None = None) -> None:
    """
    Validate webhook URL at startup.
    Resolves hostnames to IP addresses and rejects RFC 1918, loopback,
    link-local (169.254/fe80), and non-http(s) schemes.
    Hosts in allowed_hosts bypass the private-IP check (for internal services).
    Raises ValueError with a descriptive message if invalid.
    """
    if not url:
        return
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(
            f"Webhook URL must use http or https scheme, got: {parsed.scheme!r}"
        )
    host = parsed.hostname
    if not host:
        raise ValueError("Webhook URL has no hostname")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Webhook URL must not contain credentials")

    # Allowlisted hosts bypass the private-IP SSRF check.
    if allowed_hosts and host in allowed_hosts:
        logger.info("webhook.ssrf_check_bypassed host=%s (in allowed_hosts)", host)
        return

    _resolve_public_addresses(host)


class WebhookManager:
    def __init__(self, config: WebhookConfig, client: httpx.AsyncClient) -> None:
        self._config = config
        self._client = client
        self._queue: asyncio.Queue[tuple[str, str | None, dict]] = asyncio.Queue(
            maxsize=config.max_pending
        )
        self._workers: list[asyncio.Task] = []
        self._pending: set[tuple[str, str | None]] = set()

    def start(self) -> None:
        if not self._config.enabled or self._workers:
            return
        for _ in range(self._config.max_concurrent):
            self._workers.append(asyncio.create_task(self._worker()))

    async def stop(self) -> None:
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

    async def fire(self, event: str, tier: str | None = None, **kwargs) -> None:
        if not self._config.enabled:
            return
        if event not in self._config.events:
            return
        if not self._workers:
            self.start()
        key = (event, tier)
        if key in self._pending:
            return
        try:
            self._queue.put_nowait((event, tier, kwargs))
            self._pending.add(key)
        except asyncio.QueueFull:
            logger.warning("webhook.queue_full event=%s tier=%s", event, tier)

    async def _worker(self) -> None:
        while True:
            event, tier, kwargs = await self._queue.get()
            try:
                await self._deliver(event, tier, **kwargs)
            finally:
                self._pending.discard((event, tier))

    async def _deliver(self, event: str, tier: str | None, **kwargs) -> None:
        payload = {
            "event": event,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if tier:
            payload["tier"] = tier
        payload.update(kwargs)
        try:
            parsed = urlparse(self._config.url)
            host = parsed.hostname or ""
            allowed = host in self._config.allowed_hosts
            if allowed:
                target_url = self._config.url
                host_header = None
            else:
                # Resolve and validate again for every delivery, then connect to the
                # validated address so DNS cannot rebind between validation and use.
                addr = _resolve_public_addresses(host)[0]
                rendered_addr = f"[{addr}]" if addr.version == 6 else str(addr)
                port = f":{parsed.port}" if parsed.port else ""
                target_url = parsed._replace(netloc=f"{rendered_addr}{port}").geturl()
                host_header = host + port
            request = self._client.build_request(
                "POST",
                target_url,
                json=payload,
                headers={"host": host_header} if host_header else None,
                timeout=self._config.delivery_timeout,
            )
            if parsed.scheme == "https" and not allowed:
                request.extensions["sni_hostname"] = host.encode("ascii")
            response = await self._client.send(request, stream=False)
            await response.aclose()
        except Exception as e:
            logger.warning(
                "webhook.delivery_failed event=%s error_kind=%s", event, type(e).__name__
            )
