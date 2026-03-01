"""
Data artifacts for entity resolution benchmark task.

Describes the data structure and artifacts required for entity resolution
benchmarks, including entity pairs, matching labels, and evaluation data.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from pyodibel.api.entity import Entity
from pyodibel.api.benchmark import Benchmark, BenchmarkConfig, BenchmarkSplit, SplitType


@dataclass
class EntityPair:
    """Represents a pair of entities for resolution."""
    entity1: Entity
    entity2: Entity
    is_match: Optional[bool] = None
    confidence: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self) -> int:
        return hash((self.entity1.identifier, self.entity2.identifier))
    
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, EntityPair):
            return False
        return (self.entity1.identifier == other.entity1.identifier and
                self.entity2.identifier == other.entity2.identifier)


@dataclass
class EntityResolutionData:
    """Data artifacts for entity resolution benchmark."""
    entity_pairs: List[EntityPair]
    entity_clusters: Optional[Dict[str, Set[str]]] = None
    features: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_matches(self) -> List[EntityPair]:
        """Get all matching entity pairs."""
        return [pair for pair in self.entity_pairs if pair.is_match is True]
    
    def get_non_matches(self) -> List[EntityPair]:
        """Get all non-matching entity pairs."""
        return [pair for pair in self.entity_pairs if pair.is_match is False]
    
    def get_unlabeled(self) -> List[EntityPair]:
        """Get all unlabeled entity pairs."""
        return [pair for pair in self.entity_pairs if pair.is_match is None]


class EntityResolutionBenchmark(Benchmark):
    """
    Benchmark for entity resolution tasks.
    
    Extends the base Benchmark with entity resolution-specific data
    structures and methods.
    """
    
    def __init__(self, config: BenchmarkConfig):
        super().__init__(config)
        self.resolution_data: Dict[SplitType, EntityResolutionData] = {}
    
    def add_resolution_data(
        self,
        split_type: SplitType,
        resolution_data: EntityResolutionData
    ) -> None:
        """
        Add entity resolution data for a specific split.
        
        Args:
            split_type: Type of split
            resolution_data: EntityResolutionData to add
        """
        self.resolution_data[split_type] = resolution_data
    
    def get_resolution_data(
        self,
        split_type: Optional[SplitType] = None
    ) -> Optional[EntityResolutionData]:
        """
        Get entity resolution data for a split.
        
        Args:
            split_type: Optional split type, returns all if None
            
        Returns:
            EntityResolutionData or None
        """
        if split_type is None:
            # Return combined data from all splits
            all_pairs = []
            for data in self.resolution_data.values():
                all_pairs.extend(data.entity_pairs)
            return EntityResolutionData(entity_pairs=all_pairs)
        return self.resolution_data.get(split_type)
    
    def get_ground_truth(self, split_type: Optional[SplitType] = None) -> Dict[str, Any]:
        """Get ground-truth labels for entity pairs."""
        if split_type is None:
            # Combine ground truth from all splits
            ground_truth = {}
            for split, data in self.resolution_data.items():
                for pair in data.entity_pairs:
                    key = (pair.entity1.identifier, pair.entity2.identifier)
                    ground_truth[str(key)] = pair.is_match
            return ground_truth
        
        data = self.resolution_data.get(split_type)
        if data is None:
            return {}
        
        ground_truth = {}
        for pair in data.entity_pairs:
            key = (pair.entity1.identifier, pair.entity2.identifier)
            ground_truth[str(key)] = pair.is_match
        return ground_truth
    
    def get_splits(self) -> Dict[SplitType, BenchmarkSplit]:
        """Get all splits with entity resolution data."""
        splits = {}
        for split_type, data in self.resolution_data.items():
            entities = []
            for pair in data.entity_pairs:
                if pair.entity1 not in entities:
                    entities.append(pair.entity1)
                if pair.entity2 not in entities:
                    entities.append(pair.entity2)
            
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
            "num_splits": len(self.resolution_data),
            "total_pairs": sum(len(data.entity_pairs) for data in self.resolution_data.values())
        }
        metadata.update(self.config.metadata)
        return metadata
    
    def add_split(self, split: BenchmarkSplit) -> None:
        """Add a split to the benchmark."""
        # Extract entity pairs from split if possible
        # This is a placeholder - actual implementation would depend on split structure
        pass

