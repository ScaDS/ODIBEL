"""
Evaluation interface for PyODIBEL.

The Evaluation interface provides an evaluation suite for datasets or
benchmark data to derive characteristics such as size, missing values,
statistics, etc. This evaluates the data itself, not task results.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from pyodibel.api.benchmark import Benchmark, BenchmarkSplit
from pyodibel.api.entity import Entity


@dataclass
class EvaluationResult:
    """Result of an evaluation operation."""
    metrics: Dict[str, Any] = field(default_factory=dict)
    statistics: Dict[str, Any] = field(default_factory=dict)
    characteristics: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EvaluationConfig:
    """Configuration for an evaluation."""
    include_statistics: bool = True
    include_characteristics: bool = True
    include_quality_metrics: bool = True
    parameters: Dict[str, Any] = field(default_factory=dict)


class Evaluator(ABC):
    """
    Interface for evaluating datasets or benchmark data.
    
    Evaluators derive characteristics and metrics about the data itself,
    such as size, missing values, distributions, etc.
    """
    
    def __init__(self, config: Optional[EvaluationConfig] = None):
        """
        Initialize an evaluator.
        
        Args:
            config: Optional evaluation configuration
        """
        self.config = config or EvaluationConfig()
    
    @abstractmethod
    def evaluate_benchmark(self, benchmark: Benchmark) -> EvaluationResult:
        """
        Evaluate a benchmark dataset.
        
        Args:
            benchmark: Benchmark to evaluate
            
        Returns:
            EvaluationResult with metrics and characteristics
        """
        pass
    
    @abstractmethod
    def evaluate_split(self, split: BenchmarkSplit) -> EvaluationResult:
        """
        Evaluate a specific benchmark split.
        
        Args:
            split: BenchmarkSplit to evaluate
            
        Returns:
            EvaluationResult with metrics and characteristics
        """
        pass
    
    @abstractmethod
    def evaluate_entities(self, entities: List[Entity]) -> EvaluationResult:
        """
        Evaluate a collection of entities.
        
        Args:
            entities: List of entities to evaluate
            
        Returns:
            EvaluationResult with metrics and characteristics
        """
        pass
    
    @abstractmethod
    def get_statistics(self, entities: List[Entity]) -> Dict[str, Any]:
        """
        Get statistical information about entities.
        
        Args:
            entities: List of entities to analyze
            
        Returns:
            Dictionary of statistical metrics
        """
        pass
    
    @abstractmethod
    def get_characteristics(self, entities: List[Entity]) -> Dict[str, Any]:
        """
        Get characteristics of entities (e.g., missing values, distributions).
        
        Args:
            entities: List of entities to analyze
            
        Returns:
            Dictionary of characteristics
        """
        pass
    
    def get_config(self) -> EvaluationConfig:
        """Get the evaluation configuration."""
        return self.config
    
    def __repr__(self) -> str:
        return f"Evaluator(config={self.config})"

