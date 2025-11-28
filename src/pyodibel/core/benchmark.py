"""
Abstract interfaces for benchmark construction.

Benchmark construction involves creating reproducible datasets with
splits, ground-truth annotations, and metadata for evaluation.
"""

from abc import ABC, abstractmethod
from typing import Any, Iterator, Optional, Dict, List, Protocol
from pathlib import Path
from pydantic import BaseModel
from enum import Enum


class SplitType(str, Enum):
    """Types of dataset splits."""
    TRAIN = "train"
    TEST = "test"
    VALIDATION = "validation"
    DEV = "dev"


class BenchmarkConfig(BaseModel):
    """Base configuration for benchmark construction."""
    
    name: str
    description: Optional[str] = None
    version: Optional[str] = None
    metadata: Dict[str, Any] = {}


class SplitConfig(BaseModel):
    """Configuration for dataset splitting."""
    
    train_ratio: float = 0.8
    test_ratio: float = 0.2
    validation_ratio: float = 0.0
    seed: Optional[int] = None
    strategy: str = "random"  # random, stratified, temporal, etc.


class GroundTruthConfig(BaseModel):
    """Configuration for ground-truth generation."""
    
    method: str  # entity_matching, entity_linking, etc.
    parameters: Dict[str, Any] = {}


class BenchmarkBuilder(ABC):
    """
    Abstract base class for benchmark construction.
    
    Benchmark builders create reproducible benchmark datasets with:
    - Dataset splitting (train/test/validation)
    - Ground-truth generation
    - Metadata and provenance tracking
    """
    
    def __init__(self, config: BenchmarkConfig):
        """
        Initialize the benchmark builder.
        
        Args:
            config: Configuration for the benchmark
        """
        self.config = config
    
    @abstractmethod
    def create_split(self, data: Any, split_config: SplitConfig, **kwargs) -> Dict[SplitType, Any]:
        """
        Create dataset splits.
        
        Args:
            data: Input data to split
            split_config: Configuration for splitting
            **kwargs: Additional splitting parameters
            
        Returns:
            Dictionary mapping split types to split data
        """
        pass
    
    @abstractmethod
    def generate_ground_truth(self, data: Any, gt_config: GroundTruthConfig, **kwargs) -> Any:
        """
        Generate ground-truth annotations.
        
        Args:
            data: Input data
            gt_config: Configuration for ground-truth generation
            **kwargs: Additional parameters
            
        Returns:
            Ground-truth annotations
        """
        pass
    
    @abstractmethod
    def build(self, data: Any, split_config: SplitConfig, 
              gt_config: Optional[GroundTruthConfig] = None, **kwargs) -> "Benchmark":
        """
        Build a complete benchmark.
        
        Args:
            data: Input data
            split_config: Configuration for splitting
            gt_config: Optional configuration for ground-truth generation
            **kwargs: Additional parameters
            
        Returns:
            Complete benchmark instance
        """
        pass
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the benchmark.
        
        Returns:
            Dictionary containing metadata
        """
        return {
            "name": self.config.name,
            "description": self.config.description,
            "version": self.config.version,
            **self.config.metadata
        }


class Benchmark:
    """
    Represents a complete benchmark dataset.
    
    A benchmark contains:
    - Split datasets (train, test, validation)
    - Ground-truth annotations
    - Metadata and provenance information
    """
    
    def __init__(self, name: str, root_path: Path, splits: Dict[SplitType, Any],
                 ground_truth: Optional[Any] = None, metadata: Optional[Dict[str, Any]] = None):
        """
        Initialize the benchmark.
        
        Args:
            name: Benchmark name
            root_path: Root path for benchmark data
            splits: Dictionary of split datasets
            ground_truth: Optional ground-truth annotations
            metadata: Optional metadata dictionary
        """
        self.name = name
        self.root_path = root_path
        self.splits = splits
        self.ground_truth = ground_truth
        self.metadata = metadata or {}
    
    def get(self, split: SplitType, **kwargs) -> Any:
        """
        Get data for a specific split.
        
        This method provides a Dataset.get(...) interface for accessing
        benchmark data, as mentioned in the requirements.
        
        Args:
            split: Split type to retrieve
            **kwargs: Additional parameters for data access
            
        Returns:
            Data for the specified split
        """
        if split not in self.splits:
            raise ValueError(f"Split '{split}' not found in benchmark")
        return self.splits[split]
    
    def save(self, path: Optional[Path] = None) -> Path:
        """
        Save the benchmark to disk.
        
        Args:
            path: Optional path to save to (defaults to root_path)
            
        Returns:
            Path where benchmark was saved
        """
        save_path = path or self.root_path
        save_path.mkdir(parents=True, exist_ok=True)
        # Implementation would save splits, ground truth, and metadata
        return save_path
    
    def load(self, path: Path) -> "Benchmark":
        """
        Load a benchmark from disk.
        
        Args:
            path: Path to load benchmark from
            
        Returns:
            Loaded benchmark instance
        """
        # Implementation would load splits, ground truth, and metadata
        # This is a placeholder - actual implementation would be in subclasses
        raise NotImplementedError("Load implementation should be provided by subclasses")

