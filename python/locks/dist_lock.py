from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from _common import get_redis_client


@dataclass
class DistLock:
    resource: str
    ttl_ms: int = 8000
    owner: Optional[str] = None
    url: Optional[str] = None

    def __post_init__(self) -> None:
        self.r = get_redis_client(self.url)
        self.key = f"lock:{self.resource}"
        self.owner = self.owner or str(uuid.uuid4())
        self._renew_thread: Optional[threading.Thread] = None
        self._renew_stop = threading.Event()
        # Load Lua scripts
        base = Path(__file__).with_name('lua')
        self._unlock_lua = self.r.register_script((base / 'unlock_if_owner.lua').read_text())
        self._pexpire_lua = self.r.register_script((base / 'pexpire_if_owner.lua').read_text())

    # Acquire lock once (non-blocking)
    def try_acquire(self) -> bool:
        ok = self.r.set(self.key, self.owner, nx=True, px=self.ttl_ms)
        return bool(ok)

    # Blocking acquire with timeout and backoff
    def acquire(self, timeout_ms: int = 0, retry_ms: int = 100, jitter_ms: int = 50) -> bool:
        if timeout_ms <= 0:
            return self.try_acquire()
        deadline = time.time() + (timeout_ms / 1000.0)
        import random
        while time.time() < deadline:
            if self.try_acquire():
                return True
            sleep_ms = retry_ms + random.randint(0, jitter_ms)
            time.sleep(sleep_ms / 1000.0)
        return False

    # Release only if owner matches (atomic Lua)
    def release(self) -> bool:
        try:
            res = self._unlock_lua(keys=[self.key], args=[self.owner])
            return int(res) == 1
        finally:
            self.stop_renew()

    # Extend TTL if still owner
    def renew(self, ttl_ms: Optional[int] = None) -> bool:
        ttl = str(ttl_ms or self.ttl_ms)
        res = self._pexpire_lua(keys=[self.key], args=[self.owner, ttl])
        return int(res) == 1

    # Background watchdog: periodically renew until stopped
    def start_renew(self, every_ms: Optional[int] = None) -> None:
        if self._renew_thread and self._renew_thread.is_alive():
            return
        period = (every_ms or max(1000, int(self.ttl_ms * 0.5))) / 1000.0
        self._renew_stop.clear()

        def _loop():
            while not self._renew_stop.is_set():
                ok = self.renew()
                if not ok:
                    # Lost ownership or key expired
                    break
                self._renew_stop.wait(period)

        self._renew_thread = threading.Thread(target=_loop, name=f"DistLockRenew-{self.resource}", daemon=True)
        self._renew_thread.start()

    def stop_renew(self) -> None:
        if self._renew_thread and self._renew_thread.is_alive():
            self._renew_stop.set()
            self._renew_thread.join(timeout=1.0)

    # Context manager helpers
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False
