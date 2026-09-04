"""Per-client concurrency caps with fairness bound."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .config import ApiKeyConfig

logger = logging.getLogger(__name__)

@dataclass
class ClientState:
    client_id: str
    cap: int  # 0 = unlimited
    inflight: int = 0
    cap_waiting: int = 0

    @property
    def is_capped(self) -> bool:
        return self.cap > 0

    def try_acquire(self) -> bool:
        if self.is_capped and self.inflight >= self.cap:
            return False
        self.inflight += 1
        return True

    def release(self) -> None:
        self.inflight = max(0, self.inflight - 1)


class ClientConcurrencyManager:
    """
    Tracks per-client concurrency without making global queue workers wait.

    Clients with max_concurrent=0 (unlimited) are tracked for metrics but never blocked.
    Clients with max_concurrent>0 are blocked at the per-client cap, which must be ≤
    proxy.max_concurrent (validated at config load time).

    The queue scheduler calls try_acquire before dispatch. Requests at their client
    cap stay queued, allowing dispatchable requests from other clients to proceed.
    """

    def __init__(self, key_configs: list[ApiKeyConfig]) -> None:
        self._states: dict[str, ClientState] = {}
        for key in key_configs:
            self._states[key.client_id] = ClientState(
                client_id=key.client_id,
                cap=key.max_concurrent,
            )

    def get_state(self, client_id: str | None) -> ClientState | None:
        if client_id is None:
            return None
        return self._states.get(client_id)

    def try_acquire(self, client_id: str | None) -> bool:
        """Acquire immediately if the client is below its cap."""
        state = self.get_state(client_id)
        if state is None:
            return True
        return state.try_acquire()

    def release(self, client_id: str | None) -> None:
        state = self.get_state(client_id)
        if state is None:
            return
        state.release()

    def inflight_counts(self) -> dict[str, int]:
        return {cid: s.inflight for cid, s in self._states.items()}

    def cap_waiting_counts(self) -> dict[str, int]:
        return {cid: s.cap_waiting for cid, s in self._states.items()}

    def is_at_cap(self, client_id: str | None) -> bool:
        """Returns True if the client currently has no available semaphore slots."""
        state = self.get_state(client_id)
        if state is None or not state.is_capped:
            return False
        return state.inflight >= state.cap

    def set_waiting_counts(self, counts: dict[str, int]) -> None:
        for client_id, state in self._states.items():
            state.cap_waiting = counts.get(client_id, 0)
