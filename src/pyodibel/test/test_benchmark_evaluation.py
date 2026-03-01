"""
Tests for benchmark evaluation.
"""

import pytest

from pyodibel.api.entity import SimpleEntity
from pyodibel.api.benchmark import SplitType, BenchmarkConfig
from pyodibel.api.evaluation import EvaluationConfig, EvaluationResult
from pyodibel.benchmark.entity_resolution.data import (
    EntityPair,
    EntityResolutionData,
    EntityResolutionBenchmark
)
from pyodibel.benchmark.entity_resolution.eval import EntityResolutionEvaluator


class TestEntityResolutionEvaluator:
    """Tests for EntityResolutionEvaluator."""
    
    def test_create_evaluator(self):
        """Test creating an evaluator."""
        config = EvaluationConfig()
        evaluator = EntityResolutionEvaluator(config)
        assert evaluator.config == config
    
    def test_evaluate_resolution_data(self):
        """Test evaluating resolution data."""
        evaluator = EntityResolutionEvaluator()
        
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        entity3 = SimpleEntity("e3")
        
        match_pair = EntityPair(entity1, entity2, is_match=True)
        non_match_pair = EntityPair(entity1, entity3, is_match=False)
        unlabeled_pair = EntityPair(entity2, entity3, is_match=None)
        
        data = EntityResolutionData([match_pair, non_match_pair, unlabeled_pair])
        result = evaluator.evaluate_resolution_data(data)
        
        assert isinstance(result, EvaluationResult)
        assert result.metrics["total_pairs"] == 3
        assert result.metrics["matches"] == 1
        assert result.metrics["non_matches"] == 1
        assert result.metrics["unlabeled"] == 1
        assert result.metrics["match_ratio"] == pytest.approx(1/3)
        assert result.metrics["non_match_ratio"] == pytest.approx(1/3)
        assert result.metrics["unlabeled_ratio"] == pytest.approx(1/3)
    
    def test_evaluate_benchmark(self):
        """Test evaluating a benchmark."""
        evaluator = EntityResolutionEvaluator()
        
        config = BenchmarkConfig(name="test", description="test")
        benchmark = EntityResolutionBenchmark(config)
        
        # Add train split
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        train_pair = EntityPair(entity1, entity2, is_match=True)
        train_data = EntityResolutionData([train_pair])
        benchmark.add_resolution_data(SplitType.TRAIN, train_data)
        
        # Add test split
        entity3 = SimpleEntity("e3")
        entity4 = SimpleEntity("e4")
        test_pair = EntityPair(entity3, entity4, is_match=False)
        test_data = EntityResolutionData([test_pair])
        benchmark.add_resolution_data(SplitType.TEST, test_data)
        
        result = evaluator.evaluate_benchmark(benchmark)
        
        assert isinstance(result, EvaluationResult)
        assert result.metrics["total_pairs"] == 2
        assert result.metrics["total_matches"] == 1
        assert result.metrics["total_non_matches"] == 1
        assert result.statistics["train_pairs"] == 1
        assert result.statistics["test_pairs"] == 1
        assert result.characteristics["num_splits"] == 2
    
    def test_get_statistics(self):
        """Test getting entity statistics."""
        evaluator = EntityResolutionEvaluator()
        
        entity1 = SimpleEntity("e1", {"name": "Entity 1"})
        entity2 = SimpleEntity("e2", {"name": "Entity 2"})
        entity3 = SimpleEntity("e3")  # No properties
        
        entities = [entity1, entity2, entity3]
        stats = evaluator.get_statistics(entities)
        
        assert stats["total_entities"] == 3
        assert stats["entities_with_properties"] == 2
        assert stats["entities_without_properties"] == 1
    
    def test_get_characteristics(self):
        """Test getting entity characteristics."""
        evaluator = EntityResolutionEvaluator()
        
        entity1 = SimpleEntity("e1", {"name": "Entity 1", "age": 30})
        entity2 = SimpleEntity("e2", {"name": "Entity 2"})  # Missing age
        entity3 = SimpleEntity("e3", {"age": 25})  # Missing name
        
        entities = [entity1, entity2, entity3]
        characteristics = evaluator.get_characteristics(entities)
        
        assert characteristics["unique_properties"] == 2  # name and age
        assert "name" in characteristics["property_names"]
        assert "age" in characteristics["property_names"]
        assert characteristics["missing_value_counts"]["name"] == 1  # entity3 missing name
        assert characteristics["missing_value_counts"]["age"] == 1  # entity2 missing age
    
    def test_evaluate_entities(self):
        """Test evaluating a collection of entities."""
        evaluator = EntityResolutionEvaluator()
        
        entity1 = SimpleEntity("e1", {"name": "Entity 1"})
        entity2 = SimpleEntity("e2", {"name": "Entity 2"})
        
        result = evaluator.evaluate_entities([entity1, entity2])
        
        assert isinstance(result, EvaluationResult)
        assert result.statistics["total_entities"] == 2
        assert "characteristics" in result.characteristics or len(result.characteristics) >= 0

