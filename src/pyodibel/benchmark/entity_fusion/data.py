"""
Data artifacts for entity fusion benchmark task.

Describes the data structure and artifacts required for entity fusion
benchmarks, including entity clusters, fusion rules, and evaluation data.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from pyodibel.api.benchmark import Benchmark, BenchmarkConfig, BenchmarkSplit, SplitType
from pyodibel.api.entity import Entity


@dataclass
class EntityCluster:
    """Represents a cluster of entities to be fused."""
    cluster_id: str
    entities: List[Entity]
    fused_entity: Optional[Entity] = None
    fusion_rules: Optional[List[Dict[str, Any]]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FusionRule:
    """Represents a rule for fusing entity attributes."""
    rule_id: str
    rule_type: str  # e.g., "max", "min", "concat", "average"
    attribute: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EntityFusionData:
    """Data artifacts for entity fusion benchmark."""
    clusters: List[EntityCluster]
    fusion_rules: Optional[List[FusionRule]] = None
    fused_entities: Optional[List[Entity]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_cluster_by_id(self, cluster_id: str) -> Optional[EntityCluster]:
        """Get a cluster by its ID."""
        for cluster in self.clusters:
            if cluster.cluster_id == cluster_id:
                return cluster
        return None
    
    def get_clusters_with_fused(self) -> List[EntityCluster]:
        """Get clusters that have fused entities."""
        return [c for c in self.clusters if c.fused_entity is not None]
    
    def get_clusters_without_fused(self) -> List[EntityCluster]:
        """Get clusters without fused entities."""
        return [c for c in self.clusters if c.fused_entity is None]


class EntityFusionBenchmark(Benchmark):
    """
    Benchmark for entity fusion tasks.
    
    Extends the base Benchmark with entity fusion-specific data
    structures and methods.
    """
    
    def __init__(self, config: BenchmarkConfig):
        super().__init__(config)
        self.fusion_data: Dict[SplitType, EntityFusionData] = {}
    
    def add_fusion_data(
        self,
        split_type: SplitType,
        fusion_data: EntityFusionData
    ) -> None:
        """
        Add entity fusion data for a specific split.
        
        Args:
            split_type: Type of split
            fusion_data: EntityFusionData to add
        """
        self.fusion_data[split_type] = fusion_data
    
    def get_fusion_data(
        self,
        split_type: Optional[SplitType] = None
    ) -> Optional[EntityFusionData]:
        """
        Get entity fusion data for a split.
        
        Args:
            split_type: Optional split type, returns all if None
            
        Returns:
            EntityFusionData or None
        """
        if split_type is None:
            # Return combined data from all splits
            all_clusters = []
            for data in self.fusion_data.values():
                all_clusters.extend(data.clusters)
            return EntityFusionData(clusters=all_clusters)
        return self.fusion_data.get(split_type)
    
    def get_ground_truth(self, split_type: Optional[SplitType] = None) -> Dict[str, Any]:
        """Get ground-truth fused entities."""
        if split_type is None:
            ground_truth = {}
            for data in self.fusion_data.values():
                for cluster in data.clusters:
                    if cluster.fused_entity:
                        ground_truth[cluster.cluster_id] = cluster.fused_entity.identifier
            return ground_truth
        
        data = self.fusion_data.get(split_type)
        if data is None:
            return {}
        
        ground_truth = {}
        for cluster in data.clusters:
            if cluster.fused_entity:
                ground_truth[cluster.cluster_id] = cluster.fused_entity.identifier
        return ground_truth
    
    def get_splits(self) -> Dict[SplitType, BenchmarkSplit]:
        """Get all splits with entity fusion data."""
        splits = {}
        for split_type, data in self.fusion_data.items():
            # Collect all entities from clusters
            entities = []
            for cluster in data.clusters:
                entities.extend(cluster.entities)
                if cluster.fused_entity:
                    entities.append(cluster.fused_entity)
            
            ground_truth = self.get_ground_truth(split_type)
            split = BenchmarkSplit(
                split_type=split_type,
                entities=entities,
                ground_truth=ground_truth
            )
            splits[split_type] = split
        return splits
    
    def get_split(self, split_type: SplitType) -> Optional[BenchmarkSplit]:
        """Get a specific split."""
        splits = self.get_splits()
        return splits.get(split_type)
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get benchmark metadata."""
        metadata = {
            "name": self.config.name,
            "description": self.config.description,
            "domain": self.config.domain,
            "version": self.config.version,
            "num_splits": len(self.fusion_data),
            "total_clusters": sum(len(data.clusters) for data in self.fusion_data.values())
        }
        metadata.update(self.config.metadata)
        return metadata
    
    def add_split(self, split: BenchmarkSplit) -> None:
        """Add a split to the benchmark."""
        pass

