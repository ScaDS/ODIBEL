"""
Benchmark interface for PyODIBEL.

The Benchmark interface defines and describes benchmark data, including
datasets, splits, ground-truth annotations, and metadata.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum

from pyodibel.api.entity import Entity
from pyodibel.api.source import Source


class SplitType(Enum):
    """Types of data splits for benchmarks."""
    TRAIN = "train"
    VALIDATION = "validation"
    TEST = "test"
    DEV = "dev"


@dataclass
class BenchmarkConfig:
    """Configuration for a benchmark."""
    name: str
    description: str
    domain: Optional[str] = None
    version: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BenchmarkSplit:
    """Represents a split of benchmark data."""
    split_type: SplitType
    entities: List[Entity]
    ground_truth: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class Benchmark(ABC):
    """
    Interface to define and describe benchmark data.
    
    Benchmarks provide structured datasets with splits and ground-truth
    annotations for evaluating data integration tasks.
    """
    
    def __init__(self, config: BenchmarkConfig):
        """
        Initialize a benchmark.
        
        Args:
            config: Benchmark configuration
        """
        self.config = config
        self._splits: Dict[SplitType, BenchmarkSplit] = {}
    
    @abstractmethod
    def get_splits(self) -> Dict[SplitType, BenchmarkSplit]:
        """
        Get all data splits for the benchmark.
        
        Returns:
            Dictionary mapping split types to BenchmarkSplit objects
        """
        pass
    
    @abstractmethod
    def get_split(self, split_type: SplitType) -> Optional[BenchmarkSplit]:
        """
        Get a specific split by type.
        
        Args:
            split_type: Type of split to retrieve
            
        Returns:
            BenchmarkSplit or None if split doesn't exist
        """
        pass
    
    @abstractmethod
    def get_ground_truth(self, split_type: Optional[SplitType] = None) -> Dict[str, Any]:
        """
        Get ground-truth annotations for the benchmark.
        
        Args:
            split_type: Optional split type to filter ground truth
            
        Returns:
            Dictionary of ground-truth annotations
        """
        pass
    
    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get benchmark metadata.
        
        Returns:
            Dictionary of benchmark metadata
        """
        pass
    
    @abstractmethod
    def add_split(self, split: BenchmarkSplit) -> None:
        """
        Add a split to the benchmark.
        
        Args:
            split: BenchmarkSplit to add
        """
        pass
    
    def get_config(self) -> BenchmarkConfig:
        """Get the benchmark configuration."""
        return self.config
    
    def __repr__(self) -> str:
        return f"Benchmark(name={self.config.name}, domain={self.config.domain})"


class BenchmarkBuilder(ABC):
    """
    Interface for building benchmarks from data sources.
    
    Provides methods to construct benchmarks by ingesting data from
    various sources and creating splits with ground-truth annotations.
    """
    
    @abstractmethod
    def from_sources(self, sources: List[Source]) -> Benchmark:
        """
        Build a benchmark from a list of data sources.
        
        Args:
            sources: List of Source objects to ingest
            
        Returns:
            Constructed Benchmark
        """
        pass
    
    @abstractmethod
    def create_splits(
        self,
        benchmark: Benchmark,
        split_ratios: Dict[SplitType, float]
    ) -> Benchmark:
        """
        Create data splits for a benchmark.
        
        Args:
            benchmark: Benchmark to split
            split_ratios: Dictionary mapping split types to ratios (must sum to 1.0)
            
        Returns:
            Benchmark with splits created
        """
        pass
    
    @abstractmethod
    def add_ground_truth(
        self,
        benchmark: Benchmark,
        ground_truth: Dict[str, Any],
        split_type: Optional[SplitType] = None
    ) -> Benchmark:
        """
        Add ground-truth annotations to a benchmark.
        
        Args:
            benchmark: Benchmark to annotate
            ground_truth: Ground-truth annotations
            split_type: Optional split type to associate with
            
        Returns:
            Benchmark with ground truth added
        """
        pass

