from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import redis


@dataclass
class Redlock:
    masters: List[redis.Redis]
    resource: str
    ttl_ms: int = 10000
    # Clock drift factor, see https://redis.io/docs/manual/patterns/distributed-locks/
    clock_drift_factor: float = 0.01

    key: str = field(init=False)
    owner: str = field(init=False)
    quorum: int = field(init=False)
    _unlock_lua_scripts: List[redis.client.Script] = field(init=False)

    def __post_init__(self) -> None:
        self.key = f"lock:{self.resource}"
        self.owner = str(uuid.uuid4())
        self.quorum = len(self.masters) // 2 + 1

        # Load Lua script for each master
        lua_path = Path(__file__).with_name('lua') / 'unlock_if_owner.lua'
        lua_script = lua_path.read_text()
        self._unlock_lua_scripts = [
            master.register_script(lua_script) for master in self.masters
        ]

    def acquire(self) -> bool:
        start_time = time.monotonic()
        acquired_count = 0

        for master in self.masters:
            try:
                if master.set(self.key, self.owner, nx=True, px=self.ttl_ms):
                    acquired_count += 1
            except redis.exceptions.RedisError:
                # Ignore nodes that are down
                continue

        elapsed_ms = (time.monotonic() - start_time) * 1000
        # Add a drift factor for clock differences between nodes
        drift = (self.ttl_ms * self.clock_drift_factor) + 2
        validity_ms = self.ttl_ms - elapsed_ms - drift

        if acquired_count >= self.quorum and validity_ms > 0:
            # Lock acquired successfully
            return True
        else:
            # Failed to acquire lock, release on all nodes
            self.release()
            return False

    def release(self) -> None:
        for i, master in enumerate(self.masters):
            try:
                self._unlock_lua_scripts[i](keys=[self.key], args=[self.owner])
            except redis.exceptions.RedisError:
                # Ignore nodes that are down
                continue

    def __enter__(self):
        if not self.acquire():
            raise RuntimeError("Failed to acquire Redlock")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
