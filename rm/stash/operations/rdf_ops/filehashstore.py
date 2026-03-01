import hashlib
import os
from rdflib import Graph
import logging


logger = logging.getLogger(__name__)

def hash_uri(uri: str) -> str:
    """Hash a URI to create a filename-safe string."""
    return hashlib.md5(uri.encode('utf-8')).hexdigest()


class FileHashStore:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def store(self, uri: str, graph: Graph) -> None:
        """Store data in the file hash store."""
        hash_value = hash_uri(uri)
        file_path = os.path.join(self.base_dir, hash_value+".nt")
        graph.serialize(destination=file_path, format='nt')

    def retrieve(self, uri: str) -> Graph:
        """Retrieve data from the file hash store."""
        hash_value = hash_uri(uri)
        file_path = os.path.join(self.base_dir, hash_value+"/data.nt")
        try:
            return Graph().parse(file_path, format='nt')
        except Exception as e:
            logger.error(f"File not found: {uri} {file_path} {e}")
            return Graph()

# class FileHashStore2:
#     """
#     Without data.nt file
#     """
#     def __init__(self, base_dir: str):
#         self.base_dir = base_dir
#         if not os.path.exists(self.base_dir):
#             os.makedirs(self.base_dir)

#     def store(self, uri: str, graph: Graph) -> None:
#         """Store data in the file hash store."""
#         hash_value = hash_uri(uri)
#         file_path = os.path.join(self.base_dir, hash_value+".nt")
#         graph.serialize(destination=file_path, format='nt')

#     def retrieve(self, uri: str) -> Graph:
#         """Retrieve data from the file hash store."""
#         hash_value = hash_uri(uri)
#         file_path = os.path.join(self.base_dir, hash_value+".nt")
#         try:
#             return Graph().parse(file_path, format='nt')
#         except Exception as e:
#             logger.error(f"File not found: {uri} {file_path} {e}")
#             # print(f"File not found: {uri} {file_path} {e}")
#             return Graph()

#     def exists(self, uri: str) -> bool:
#         """Check if the data exists in the file hash store."""
#         hash_value = hash_uri(uri)
#         file_path = os.path.join(self.base_dir, hash_value+".nt")
#         return os.path.exists(file_path)

import os
import time
import threading
from collections import OrderedDict

class _LRUWithTTLAndMTime:
    """LRU cache storing (graph, mtime, ts). Validates by mtime and optional TTL."""
    def __init__(self, maxsize: int = 200_000, ttl: float | None = None):
        self.maxsize = maxsize
        self.ttl = ttl
        self._data: "OrderedDict[str, tuple[Graph, float, float]]" = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: str, current_mtime: float) -> Graph | None:
        now = time.time()
        with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            graph, cached_mtime, ts = item
            # TTL expired?
            if self.ttl is not None and (now - ts) > self.ttl:
                self._data.pop(key, None)
                return None
            # File changed?
            if cached_mtime != current_mtime:
                self._data.pop(key, None)
                return None
            # Mark as recently used
            self._data.move_to_end(key, last=True)
            return graph

    def put(self, key: str, graph: Graph, current_mtime: float) -> None:
        with self._lock:
            if key in self._data:
                self._data.pop(key, None)
            elif len(self._data) >= self.maxsize:
                self._data.popitem(last=False)  # evict LRU
            self._data[key] = (graph, current_mtime, time.time())

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

class FileHashStore2:
    """
    File-backed graph store (N-Triples) with optional read cache.
    """
    def __init__(
        self,
        base_dir: str,
        *,
        cache_enabled: bool = False,
        cache_maxsize: int = 200_000,
        cache_ttl: float | None = None,
        cache_return_copy: bool = False,   # return a new Graph to avoid mutating cached one
    ):
        self.base_dir = base_dir
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)
        self.cache_enabled = cache_enabled
        self.cache_return_copy = cache_return_copy
        self._cache = _LRUWithTTLAndMTime(cache_maxsize, cache_ttl)
        self._path_lock = threading.RLock()  # guards store() + mtime reads

    def _path_for(self, uri: str) -> str:
        return os.path.join(self.base_dir, f"{hash_uri(uri)}.nt")

    def store(self, uri: str, graph: Graph) -> None:
        """Store data; invalidates cache for that URI atomically."""
        file_path = self._path_for(uri)
        tmp_path = f"{file_path}.tmp"
        # write atomically to avoid readers seeing partial files
        with self._path_lock:
            graph.serialize(destination=tmp_path, format="nt")
            os.replace(tmp_path, file_path)  # atomic on POSIX
            # bust cache entry if present
            if self.cache_enabled:
                self._cache.invalidate(file_path)

    def retrieve(self, uri: str, *, use_cache: bool = True) -> Graph:
        """Retrieve data, optionally using the read cache."""
        file_path = self._path_for(uri)
        try:
            # get current mtime once (0 if missing)
            with self._path_lock:
                mtime = os.path.getmtime(file_path) if os.path.exists(file_path) else 0.0

            if self.cache_enabled and use_cache and mtime > 0:
                g = self._cache.get(file_path, mtime)
                if g is not None:
                    return self._copy_if_needed(g)

            # cache miss or no file: parse (or return empty)
            if mtime == 0.0:
                g = Graph()
            else:
                g = Graph().parse(file_path, format="nt")

            if self.cache_enabled and mtime > 0 and use_cache:
                self._cache.put(file_path, g, mtime)

            return self._copy_if_needed(g)

        except Exception as e:
            # log and return empty graph
            logger.error(f"Retrieve failed: {uri} {file_path} {e}")
            return Graph()

    def exists(self, uri: str) -> bool:
        file_path = self._path_for(uri)
        return os.path.exists(file_path)

    def clear_cache(self) -> None:
        self._cache.clear()

    def _copy_if_needed(self, g: Graph) -> Graph:
        if not self.cache_return_copy:
            return g
        # cheap-ish shallow copy: new Graph sharing the same store
        # (adjust if you require deep copy)
        return Graph(g.store, g.identifier)