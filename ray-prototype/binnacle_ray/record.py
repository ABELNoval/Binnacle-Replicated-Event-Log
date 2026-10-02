from __future__ import annotations

import time
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Record:
    """Same fields as `Record` in proto/broker.proto, so the contract does not change between phases."""

    key: bytes
    value: bytes
    timestamp: int = 0  # epoch millis; 0 means "not set by the producer"

    def __post_init__(self) -> None:
        if not isinstance(self.key, bytes) or not isinstance(self.value, bytes):
            raise TypeError("key and value must be bytes")

    def stamped(self) -> Record:
        if self.timestamp:
            return self
        return replace(self, timestamp=time.time_ns() // 1_000_000)
