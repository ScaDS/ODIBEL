"""
Evaluation functions for entity fusion benchmark data.

Enables evaluation of benchmark data artifacts for entity fusion tasks,
creating metrics about the data itself (not task results).
"""

from typing import Any, Dict, List

from pyodibel.api.evaluation import Evaluator, EvaluationConfig, EvaluationResult
from pyodibel.benchmark.entity_fusion.data import (
    EntityFusionBenchmark,
    EntityFusionData,
    EntityCluster
)


class EntityFusionEvaluator(Evaluator):
    """
    Evaluator for entity fusion benchmark data.
    
    Creates metrics about the data characteristics, such as:
    - Cluster statistics
    - Fusion rule coverage
    - Entity distribution across clusters
    - Data quality metrics
    """
    
    def evaluate_benchmark(self, benchmark: EntityFusionBenchmark) -> EvaluationResult:
        """Evaluate an entity fusion benchmark."""
        metrics = {}
        statistics = {}
        characteristics = {}
        
        # Aggregate statistics across all splits
        total_clusters = 0
        total_entities = 0
        total_fused = 0
        cluster_sizes = []
        
        for split_type, data in benchmark.fusion_data.items():
            clusters = data.clusters
            entities_in_split = sum(len(c.entities) for c in clusters)
            fused_in_split = len(data.get_clusters_with_fused())
            
            total_clusters += len(clusters)
            total_entities += entities_in_split
            total_fused += fused_in_split
            
            for cluster in clusters:
                cluster_sizes.append(len(cluster.entities))
            
            statistics[f"{split_type.value}_clusters"] = len(clusters)
            statistics[f"{split_type.value}_entities"] = entities_in_split
            statistics[f"{split_type.value}_fused"] = fused_in_split
        
        metrics["total_clusters"] = total_clusters
        metrics["total_entities"] = total_entities
        metrics["total_fused"] = total_fused
        
        if cluster_sizes:
            metrics["avg_cluster_size"] = sum(cluster_sizes) / len(cluster_sizes)
            metrics["min_cluster_size"] = min(cluster_sizes)
            metrics["max_cluster_size"] = max(cluster_sizes)
        
        if total_clusters > 0:
            metrics["fusion_ratio"] = total_fused / total_clusters
        
        characteristics["num_splits"] = len(benchmark.fusion_data)
        characteristics["has_fusion_rules"] = any(
            data.fusion_rules is not None
            for data in benchmark.fusion_data.values()
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
        """Evaluate a collection of entities (placeholder)."""
        return EvaluationResult()
    
    def get_statistics(self, entities: List[Any]) -> Dict[str, Any]:
        """Get statistical information (placeholder)."""
        return {}
    
    def get_characteristics(self, entities: List[Any]) -> Dict[str, Any]:
        """Get characteristics (placeholder)."""
        return {}
    
    def evaluate_fusion_data(self, data: EntityFusionData) -> EvaluationResult:
        """
        Evaluate entity fusion data directly.
        
        Args:
            data: EntityFusionData to evaluate
            
        Returns:
            EvaluationResult with metrics
        """
        clusters = data.clusters
        fused_clusters = data.get_clusters_with_fused()
        unfused_clusters = data.get_clusters_without_fused()
        
        cluster_sizes = [len(c.entities) for c in clusters]
        total_entities = sum(cluster_sizes)
        
        metrics = {
            "total_clusters": len(clusters),
            "total_entities": total_entities,
            "fused_clusters": len(fused_clusters),
            "unfused_clusters": len(unfused_clusters)
        }
        
        if cluster_sizes:
            metrics["avg_cluster_size"] = sum(cluster_sizes) / len(cluster_sizes)
            metrics["min_cluster_size"] = min(cluster_sizes)
            metrics["max_cluster_size"] = max(cluster_sizes)
        
        if len(clusters) > 0:
            metrics["fusion_ratio"] = len(fused_clusters) / len(clusters)
        
        characteristics = {
            "has_fusion_rules": data.fusion_rules is not None,
            "num_fusion_rules": len(data.fusion_rules) if data.fusion_rules else 0,
            "has_fused_entities": data.fused_entities is not None,
            "num_fused_entities": len(data.fused_entities) if data.fused_entities else 0
        }
        
        return EvaluationResult(
            metrics=metrics,
            characteristics=characteristics,
            metadata=data.metadata
        )

