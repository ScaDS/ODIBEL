"""
Evaluation functions for schema matching benchmark data.

Enables evaluation of benchmark data artifacts for schema matching tasks,
creating metrics about the data itself (not task results).
"""

from typing import Any, Dict, List

from pyodibel.api.evaluation import Evaluator, EvaluationConfig, EvaluationResult
from pyodibel.benchmark.schema_matching.data import (
    SchemaMatchingBenchmark,
    SchemaMatchingData,
    Schema,
    AttributeMapping
)


class SchemaMatchingEvaluator(Evaluator):
    """
    Evaluator for schema matching benchmark data.
    
    Creates metrics about the data characteristics, such as:
    - Distribution of matches vs non-matches
    - Schema statistics
    - Attribute mapping distributions
    - Data quality metrics
    """
    
    def evaluate_benchmark(self, benchmark: SchemaMatchingBenchmark) -> EvaluationResult:
        """Evaluate a schema matching benchmark."""
        metrics = {}
        statistics = {}
        characteristics = {}
        
        # Aggregate statistics across all splits
        total_schemas = 0
        total_mappings = 0
        total_matches = 0
        total_non_matches = 0
        total_unlabeled = 0
        
        for split_type, data in benchmark.matching_data.items():
            schemas = data.schemas
            mappings = data.mappings
            matches = len(data.get_matches())
            non_matches = len(data.get_non_matches())
            unlabeled = len(data.get_unlabeled())
            
            total_schemas += len(schemas)
            total_mappings += len(mappings)
            total_matches += matches
            total_non_matches += non_matches
            total_unlabeled += unlabeled
            
            statistics[f"{split_type.value}_schemas"] = len(schemas)
            statistics[f"{split_type.value}_mappings"] = len(mappings)
            statistics[f"{split_type.value}_matches"] = matches
            statistics[f"{split_type.value}_non_matches"] = non_matches
            statistics[f"{split_type.value}_unlabeled"] = unlabeled
        
        metrics["total_schemas"] = total_schemas
        metrics["total_mappings"] = total_mappings
        metrics["total_matches"] = total_matches
        metrics["total_non_matches"] = total_non_matches
        metrics["total_unlabeled"] = total_unlabeled
        
        if total_mappings > 0:
            metrics["match_ratio"] = total_matches / total_mappings
            metrics["non_match_ratio"] = total_non_matches / total_mappings
            metrics["unlabeled_ratio"] = total_unlabeled / total_mappings
        
        characteristics["num_splits"] = len(benchmark.matching_data)
        characteristics["avg_attributes_per_schema"] = (
            sum(len(s.attributes) for data in benchmark.matching_data.values() for s in data.schemas)
            / total_schemas if total_schemas > 0 else 0
        )
        
        return EvaluationResult(
            metrics=metrics,
            statistics=statistics,
            characteristics=characteristics,
            metadata={"benchmark_name": benchmark.config.name}
        )
    
    def evaluate_split(self, split: Any) -> EvaluationResult:
        """Evaluate a benchmark split (placeholder)."""
        return EvaluationResult()
    
    def evaluate_entities(self, entities: List[Any]) -> EvaluationResult:
        """Evaluate a collection of entities (placeholder for schema matching)."""
        return EvaluationResult()
    
    def get_statistics(self, entities: List[Any]) -> Dict[str, Any]:
        """Get statistical information (placeholder)."""
        return {}
    
    def get_characteristics(self, entities: List[Any]) -> Dict[str, Any]:
        """Get characteristics (placeholder)."""
        return {}
    
    def evaluate_matching_data(self, data: SchemaMatchingData) -> EvaluationResult:
        """
        Evaluate schema matching data directly.
        
        Args:
            data: SchemaMatchingData to evaluate
            
        Returns:
            EvaluationResult with metrics
        """
        schemas = data.schemas
        mappings = data.mappings
        matches = data.get_matches()
        non_matches = data.get_non_matches()
        unlabeled = data.get_unlabeled()
        
        metrics = {
            "total_schemas": len(schemas),
            "total_mappings": len(mappings),
            "matches": len(matches),
            "non_matches": len(non_matches),
            "unlabeled": len(unlabeled)
        }
        
        if len(mappings) > 0:
            metrics["match_ratio"] = len(matches) / len(mappings)
            metrics["non_match_ratio"] = len(non_matches) / len(mappings)
            metrics["unlabeled_ratio"] = len(unlabeled) / len(mappings)
        
        if len(schemas) > 0:
            total_attributes = sum(len(s.attributes) for s in schemas)
            metrics["avg_attributes_per_schema"] = total_attributes / len(schemas)
        
        characteristics = {
            "has_schema_pairs": data.schema_pairs is not None,
            "num_schema_pairs": len(data.schema_pairs) if data.schema_pairs else 0
        }
        
        return EvaluationResult(
            metrics=metrics,
            characteristics=characteristics,
            metadata=data.metadata
        )

