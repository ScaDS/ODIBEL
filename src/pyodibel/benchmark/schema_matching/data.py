"""
Data artifacts for schema matching benchmark task.

Describes the data structure and artifacts required for schema matching
benchmarks, including schemas, attribute mappings, and evaluation data.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from pyodibel.api.benchmark import Benchmark, BenchmarkConfig, BenchmarkSplit, SplitType
from pyodibel.api.entity import Entity


@dataclass
class Schema:
    """Represents a schema with attributes."""
    name: str
    attributes: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AttributeMapping:
    """Represents a mapping between attributes from different schemas."""
    source_attribute: str
    target_attribute: str
    is_match: Optional[bool] = None
    confidence: Optional[float] = None
    similarity_score: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SchemaMatchingData:
    """Data artifacts for schema matching benchmark."""
    schemas: List[Schema]
    mappings: List[AttributeMapping]
    schema_pairs: Optional[List[Tuple[Schema, Schema]]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_matches(self) -> List[AttributeMapping]:
        """Get all matching attribute mappings."""
        return [mapping for mapping in self.mappings if mapping.is_match is True]
    
    def get_non_matches(self) -> List[AttributeMapping]:
        """Get all non-matching attribute mappings."""
        return [mapping for mapping in self.mappings if mapping.is_match is False]
    
    def get_unlabeled(self) -> List[AttributeMapping]:
        """Get all unlabeled attribute mappings."""
        return [mapping for mapping in self.mappings if mapping.is_match is None]


class SchemaMatchingBenchmark(Benchmark):
    """
    Benchmark for schema matching tasks.
    
    Extends the base Benchmark with schema matching-specific data
    structures and methods.
    """
    
    def __init__(self, config: BenchmarkConfig):
        super().__init__(config)
        self.matching_data: Dict[SplitType, SchemaMatchingData] = {}
    
    def add_matching_data(
        self,
        split_type: SplitType,
        matching_data: SchemaMatchingData
    ) -> None:
        """
        Add schema matching data for a specific split.
        
        Args:
            split_type: Type of split
            matching_data: SchemaMatchingData to add
        """
        self.matching_data[split_type] = matching_data
    
    def get_matching_data(
        self,
        split_type: Optional[SplitType] = None
    ) -> Optional[SchemaMatchingData]:
        """
        Get schema matching data for a split.
        
        Args:
            split_type: Optional split type, returns all if None
            
        Returns:
            SchemaMatchingData or None
        """
        if split_type is None:
            # Return combined data from all splits
            all_schemas = []
            all_mappings = []
            for data in self.matching_data.values():
                all_schemas.extend(data.schemas)
                all_mappings.extend(data.mappings)
            return SchemaMatchingData(schemas=all_schemas, mappings=all_mappings)
        return self.matching_data.get(split_type)
    
    def get_ground_truth(self, split_type: Optional[SplitType] = None) -> Dict[str, Any]:
        """Get ground-truth labels for attribute mappings."""
        if split_type is None:
            ground_truth = {}
            for data in self.matching_data.values():
                for mapping in data.mappings:
                    key = (mapping.source_attribute, mapping.target_attribute)
                    ground_truth[str(key)] = mapping.is_match
            return ground_truth
        
        data = self.matching_data.get(split_type)
        if data is None:
            return {}
        
        ground_truth = {}
        for mapping in data.mappings:
            key = (mapping.source_attribute, mapping.target_attribute)
            ground_truth[str(key)] = mapping.is_match
        return ground_truth
    
    def get_splits(self) -> Dict[SplitType, BenchmarkSplit]:
        """Get all splits with schema matching data."""
        splits = {}
        for split_type, data in self.matching_data.items():
            # Convert schemas to entities for split representation
            entities = []
            # This is a placeholder - actual implementation would convert schemas
            # to entities or handle them differently
            
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
            "num_splits": len(self.matching_data),
            "total_schemas": sum(len(data.schemas) for data in self.matching_data.values()),
            "total_mappings": sum(len(data.mappings) for data in self.matching_data.values())
        }
        metadata.update(self.config.metadata)
        return metadata
    
    def add_split(self, split: BenchmarkSplit) -> None:
        """Add a split to the benchmark."""
        pass

