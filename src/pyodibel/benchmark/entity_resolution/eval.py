"""
Evaluation functions for entity resolution benchmark data.

Enables evaluation of benchmark data artifacts for entity resolution tasks,
creating metrics about the data itself (not task results).
"""

from typing import Any, Dict, List

from pyodibel.api.entity import Entity
from pyodibel.api.evaluation import Evaluator, EvaluationConfig, EvaluationResult
from pyodibel.benchmark.entity_resolution.data import (
    EntityResolutionBenchmark,
    EntityResolutionData,
    EntityPair
)


class EntityResolutionEvaluator(Evaluator):
    """
    Evaluator for entity resolution benchmark data.
    
    Creates metrics about the data characteristics, such as:
    - Distribution of matches vs non-matches
    - Entity pair statistics
    - Feature distributions
    - Data quality metrics
    """
    
    def evaluate_benchmark(self, benchmark: EntityResolutionBenchmark) -> EvaluationResult:
        """Evaluate an entity resolution benchmark."""
        metrics = {}
        statistics = {}
        characteristics = {}
        
        # Aggregate statistics across all splits
        total_pairs = 0
        total_matches = 0
        total_non_matches = 0
        total_unlabeled = 0
        
        for split_type, data in benchmark.resolution_data.items():
            pairs = data.entity_pairs
            matches = len(data.get_matches())
            non_matches = len(data.get_non_matches())
            unlabeled = len(data.get_unlabeled())
            
            total_pairs += len(pairs)
            total_matches += matches
            total_non_matches += non_matches
            total_unlabeled += unlabeled
            
            statistics[f"{split_type.value}_pairs"] = len(pairs)
            statistics[f"{split_type.value}_matches"] = matches
            statistics[f"{split_type.value}_non_matches"] = non_matches
            statistics[f"{split_type.value}_unlabeled"] = unlabeled
        
        metrics["total_pairs"] = total_pairs
        metrics["total_matches"] = total_matches
        metrics["total_non_matches"] = total_non_matches
        metrics["total_unlabeled"] = total_unlabeled
        
        if total_pairs > 0:
            metrics["match_ratio"] = total_matches / total_pairs
            metrics["non_match_ratio"] = total_non_matches / total_pairs
            metrics["unlabeled_ratio"] = total_unlabeled / total_pairs
        
        characteristics["num_splits"] = len(benchmark.resolution_data)
        characteristics["has_clusters"] = any(
            data.entity_clusters is not None
            for data in benchmark.resolution_data.values()
        )
        characteristics["has_features"] = any(
            data.features is not None
            for data in benchmark.resolution_data.values()
        )
        
        return EvaluationResult(
            metrics=metrics,
            statistics=statistics,
            characteristics=characteristics,
            metadata={"benchmark_name": benchmark.config.name}
        )
    
    def evaluate_split(self, split: Any) -> EvaluationResult:
        """Evaluate a benchmark split (placeholder - needs proper split type)."""
        # This would need to be adapted based on how splits are structured
        # For now, return empty result
        return EvaluationResult()
    
    def evaluate_entities(self, entities: List[Entity]) -> EvaluationResult:
        """Evaluate a collection of entities."""
        statistics = self.get_statistics(entities)
        characteristics = self.get_characteristics(entities)
        return EvaluationResult(
            statistics=statistics,
            characteristics=characteristics
        )
    
    def get_statistics(self, entities: List[Entity]) -> Dict[str, Any]:
        """Get statistical information about entities."""
        if not entities:
            return {}
        
        # Count entities with properties
        entities_with_props = sum(1 for e in entities if e.get_properties())
        
        return {
            "total_entities": len(entities),
            "entities_with_properties": entities_with_props,
            "entities_without_properties": len(entities) - entities_with_props
        }
    
    def get_characteristics(self, entities: List[Entity]) -> Dict[str, Any]:
        """Get characteristics of entities."""
        if not entities:
            return {}
        
        # Collect property names
        all_properties = set()
        for entity in entities:
            all_properties.update(entity.get_properties().keys())
        
        # Count missing values per property
        missing_counts = {}
        for prop in all_properties:
            missing = sum(1 for e in entities if not e.has_property(prop))
            missing_counts[prop] = missing
        
        return {
            "unique_properties": len(all_properties),
            "property_names": list(all_properties),
            "missing_value_counts": missing_counts
        }
    
    def evaluate_resolution_data(self, data: EntityResolutionData) -> EvaluationResult:
        """
        Evaluate entity resolution data directly.
        
        Args:
            data: EntityResolutionData to evaluate
            
        Returns:
            EvaluationResult with metrics
        """
        pairs = data.entity_pairs
        matches = data.get_matches()
        non_matches = data.get_non_matches()
        unlabeled = data.get_unlabeled()
        
        metrics = {
            "total_pairs": len(pairs),
            "matches": len(matches),
            "non_matches": len(non_matches),
            "unlabeled": len(unlabeled)
        }
        
        if len(pairs) > 0:
            metrics["match_ratio"] = len(matches) / len(pairs)
            metrics["non_match_ratio"] = len(non_matches) / len(pairs)
            metrics["unlabeled_ratio"] = len(unlabeled) / len(pairs)
        
        characteristics = {
            "has_clusters": data.entity_clusters is not None,
            "has_features": data.features is not None,
            "num_clusters": len(data.entity_clusters) if data.entity_clusters else 0
        }
        
        return EvaluationResult(
            metrics=metrics,
            characteristics=characteristics,
            metadata=data.metadata
        )

