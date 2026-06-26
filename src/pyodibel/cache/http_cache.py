from __future__ import annotations

import base64
import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any

from pyodibel.cache.rocksdb_store import RocksDbCache


@dataclass(frozen=True)
class CachedHttpResponse:
    url: str
    status_code: int
    content_type: str
    body: bytes
    fetched_at: float

    def is_cacheable(self) -> bool:
        return self.status_code == 200 and bool(self.body)

    def to_bytes(self) -> bytes:
        payload = {
            "url": self.url,
            "status_code": self.status_code,
            "content_type": self.content_type,
            "body": base64.b64encode(self.body).decode("ascii"),
            "fetched_at": self.fetched_at,
        }
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> CachedHttpResponse:
        payload: dict[str, Any] = json.loads(data.decode("utf-8"))
        return cls(
            url=payload["url"],
            status_code=int(payload["status_code"]),
            content_type=str(payload.get("content_type", "")),
            body=base64.b64decode(payload["body"].encode("ascii")),
            fetched_at=float(payload["fetched_at"]),
        )


class HttpFetchCache:
    """HTTP response cache backed by RocksDB."""

    def __init__(self, cache: RocksDbCache) -> None:
        self._cache = cache

    @staticmethod
    def cache_key(url: str) -> bytes:
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return f"fetch:{digest}".encode("ascii")

    def get(self, url: str) -> CachedHttpResponse | None:
        raw = self._cache.get(self.cache_key(url))
        if raw is None:
            return None
        return CachedHttpResponse.from_bytes(raw)

    def put(self, response: CachedHttpResponse) -> None:
        if not response.is_cacheable():
            return
        self._cache.put(self.cache_key(response.url), response.to_bytes())

    @staticmethod
    def build_response(
        url: str,
        *,
        status_code: int,
        content_type: str,
        body: bytes,
        fetched_at: float | None = None,
    ) -> CachedHttpResponse:
        return CachedHttpResponse(
            url=url,
            status_code=status_code,
            content_type=content_type,
            body=body,
            fetched_at=fetched_at if fetched_at is not None else time.time(),
        )
