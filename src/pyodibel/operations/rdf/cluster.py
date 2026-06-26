from __future__ import annotations

from typing import Dict, Optional, Set


class MatchCluster:
    """Store clusters of equivalent URIs for namespace-aware replacement."""

    def __init__(self) -> None:
        self.uri_to_cluster: Dict[str, int] = {}
        self.clusters: Dict[int, Set[str]] = {}
        self.next_cluster_id = 0

    def add_match(self, uri1: str, uri2: str) -> None:
        cluster1 = self.uri_to_cluster.get(uri1)
        cluster2 = self.uri_to_cluster.get(uri2)

        if cluster1 is None and cluster2 is None:
            cluster_id = self.next_cluster_id
            self.next_cluster_id += 1
            self.clusters[cluster_id] = {uri1, uri2}
            self.uri_to_cluster[uri1] = cluster_id
            self.uri_to_cluster[uri2] = cluster_id
        elif cluster1 is None:
            if cluster2 is not None:
                self.clusters[cluster2].add(uri1)
                self.uri_to_cluster[uri1] = cluster2
        elif cluster2 is None:
            if cluster1 is not None:
                self.clusters[cluster1].add(uri2)
                self.uri_to_cluster[uri2] = cluster1
        elif cluster1 != cluster2:
            self._merge_clusters(cluster1, cluster2)

    def _merge_clusters(self, cluster1_id: int, cluster2_id: int) -> None:
        if cluster1_id == cluster2_id:
            return

        keep_id = min(cluster1_id, cluster2_id)
        remove_id = max(cluster1_id, cluster2_id)

        uris_to_move = self.clusters[remove_id]
        self.clusters[keep_id].update(uris_to_move)
        for uri in uris_to_move:
            self.uri_to_cluster[uri] = keep_id
        del self.clusters[remove_id]

    def get_cluster(self, uri: str) -> Optional[Set[str]]:
        cluster_id = self.uri_to_cluster.get(uri)
        if cluster_id is None:
            return None
        return self.clusters[cluster_id]

    def has_match_to_namespace(self, uri: str, target_ns: str) -> Optional[str]:
        cluster = self.get_cluster(uri)
        if cluster is None:
            return None
        for cluster_uri in cluster:
            if cluster_uri.startswith(target_ns):
                return cluster_uri
        return None
