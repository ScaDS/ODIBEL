from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rocksdict import Rdict


class RocksDbCache:
    """Thin wrapper around RocksDB via rocksdict (raw bytes mode)."""

    def __init__(self, path: str | Path, *, read_only: bool = False) -> None:
        from rocksdict import AccessType, Options, Rdict

        self._path = Path(path)
        self._path.mkdir(parents=True, exist_ok=True)
        access_type = AccessType.read_only() if read_only else AccessType.read_write()
        self._db: Rdict = Rdict(
            str(self._path),
            options=Options(raw_mode=True),
            access_type=access_type,
        )

    @property
    def path(self) -> Path:
        return self._path

    def get(self, key: bytes) -> bytes | None:
        try:
            value = self._db.get(key)
        except KeyError:
            return None
        return value

    def put(self, key: bytes, value: bytes) -> None:
        self._db.put(key, value)

    def contains(self, key: bytes) -> bool:
        return self.get(key) is not None

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> RocksDbCache:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
