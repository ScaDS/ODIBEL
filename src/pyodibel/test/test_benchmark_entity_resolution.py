"""
Tests for entity resolution benchmark.
"""

import pytest

from pyodibel.api.entity import SimpleEntity, EntityMetadata
from pyodibel.api.benchmark import SplitType
from pyodibel.benchmark.entity_resolution.data import (
    EntityPair,
    EntityResolutionData,
    EntityResolutionBenchmark,
    BenchmarkConfig
)


class TestEntityPair:
    """Tests for EntityPair."""
    
    def test_create_entity_pair(self):
        """Test creating an entity pair."""
        entity1 = SimpleEntity("e1", {"name": "Entity 1"})
        entity2 = SimpleEntity("e2", {"name": "Entity 2"})
        pair = EntityPair(entity1, entity2)
        
        assert pair.entity1 == entity1
        assert pair.entity2 == entity2
        assert pair.is_match is None
        assert pair.confidence is None
    
    def test_entity_pair_with_match(self):
        """Test entity pair with match label."""
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        pair = EntityPair(entity1, entity2, is_match=True, confidence=0.95)
        
        assert pair.is_match is True
        assert pair.confidence == 0.95
    
    def test_entity_pair_equality(self):
        """Test entity pair equality."""
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        pair1 = EntityPair(entity1, entity2)
        pair2 = EntityPair(entity1, entity2)
        pair3 = EntityPair(entity2, entity1)  # Different order
        
        assert pair1 == pair2
        assert pair1 != pair3
    
    def test_entity_pair_hash(self):
        """Test entity pair hashing."""
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        pair1 = EntityPair(entity1, entity2)
        pair2 = EntityPair(entity1, entity2)
        
        assert hash(pair1) == hash(pair2)


class TestEntityResolutionData:
    """Tests for EntityResolutionData."""
    
    def test_create_resolution_data(self):
        """Test creating resolution data."""
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        pair = EntityPair(entity1, entity2, is_match=True)
        
        data = EntityResolutionData([pair])
        assert len(data.entity_pairs) == 1
        assert data.entity_clusters is None
        assert data.features is None
    
    def test_get_matches(self):
        """Test getting matching pairs."""
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        entity3 = SimpleEntity("e3")
        
        match_pair = EntityPair(entity1, entity2, is_match=True)
        non_match_pair = EntityPair(entity1, entity3, is_match=False)
        unlabeled_pair = EntityPair(entity2, entity3, is_match=None)
        
        data = EntityResolutionData([match_pair, non_match_pair, unlabeled_pair])
        
        matches = data.get_matches()
        assert len(matches) == 1
        assert matches[0] == match_pair
    
    def test_get_non_matches(self):
        """Test getting non-matching pairs."""
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        entity3 = SimpleEntity("e3")
        
        match_pair = EntityPair(entity1, entity2, is_match=True)
        non_match_pair = EntityPair(entity1, entity3, is_match=False)
        unlabeled_pair = EntityPair(entity2, entity3, is_match=None)
        
        data = EntityResolutionData([match_pair, non_match_pair, unlabeled_pair])
        
        non_matches = data.get_non_matches()
        assert len(non_matches) == 1
        assert non_matches[0] == non_match_pair
    
    def test_get_unlabeled(self):
        """Test getting unlabeled pairs."""
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        entity3 = SimpleEntity("e3")
        
        match_pair = EntityPair(entity1, entity2, is_match=True)
        non_match_pair = EntityPair(entity1, entity3, is_match=False)
        unlabeled_pair = EntityPair(entity2, entity3, is_match=None)
        
        data = EntityResolutionData([match_pair, non_match_pair, unlabeled_pair])
        
        unlabeled = data.get_unlabeled()
        assert len(unlabeled) == 1
        assert unlabeled[0] == unlabeled_pair


class TestEntityResolutionBenchmark:
    """Tests for EntityResolutionBenchmark."""
    
    def test_create_benchmark(self):
        """Test creating an entity resolution benchmark."""
        config = BenchmarkConfig(
            name="test_benchmark",
            description="Test benchmark",
            domain="test"
        )
        benchmark = EntityResolutionBenchmark(config)
        
        assert benchmark.config.name == "test_benchmark"
        assert len(benchmark.resolution_data) == 0
    
    def test_add_resolution_data(self):
        """Test adding resolution data to a split."""
        config = BenchmarkConfig(name="test", description="test")
        benchmark = EntityResolutionBenchmark(config)
        
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        pair = EntityPair(entity1, entity2, is_match=True)
        data = EntityResolutionData([pair])
        
        benchmark.add_resolution_data(SplitType.TRAIN, data)
        assert SplitType.TRAIN in benchmark.resolution_data
        assert benchmark.resolution_data[SplitType.TRAIN] == data
    
    def test_get_resolution_data(self):
        """Test getting resolution data for a split."""
        config = BenchmarkConfig(name="test", description="test")
        benchmark = EntityResolutionBenchmark(config)
        
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        pair = EntityPair(entity1, entity2, is_match=True)
        data = EntityResolutionData([pair])
        
        benchmark.add_resolution_data(SplitType.TRAIN, data)
        
        retrieved_data = benchmark.get_resolution_data(SplitType.TRAIN)
        assert retrieved_data == data
        
        # Test getting all data
        all_data = benchmark.get_resolution_data()
        assert all_data is not None
        assert len(all_data.entity_pairs) == 1
    
    def test_get_ground_truth(self):
        """Test getting ground truth labels."""
        config = BenchmarkConfig(name="test", description="test")
        benchmark = EntityResolutionBenchmark(config)
        
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        entity3 = SimpleEntity("e3")
        
        pair1 = EntityPair(entity1, entity2, is_match=True)
        pair2 = EntityPair(entity1, entity3, is_match=False)
        
        data = EntityResolutionData([pair1, pair2])
        benchmark.add_resolution_data(SplitType.TRAIN, data)
        
        ground_truth = benchmark.get_ground_truth(SplitType.TRAIN)
        assert len(ground_truth) == 2
        assert str((entity1.identifier, entity2.identifier)) in ground_truth
        assert ground_truth[str((entity1.identifier, entity2.identifier))] is True
    
    def test_get_splits(self):
        """Test getting benchmark splits."""
        config = BenchmarkConfig(name="test", description="test")
        benchmark = EntityResolutionBenchmark(config)
        
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        pair = EntityPair(entity1, entity2, is_match=True)
        data = EntityResolutionData([pair])
        
        benchmark.add_resolution_data(SplitType.TRAIN, data)
        
        splits = benchmark.get_splits()
        assert SplitType.TRAIN in splits
        split = splits[SplitType.TRAIN]
        assert split.split_type == SplitType.TRAIN
        assert len(split.entities) == 2  # Both entities from the pair
    
    def test_get_metadata(self):
        """Test getting benchmark metadata."""
        config = BenchmarkConfig(
            name="test_benchmark",
            description="Test description",
            domain="test_domain",
            version="1.0"
        )
        benchmark = EntityResolutionBenchmark(config)
        
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        pair = EntityPair(entity1, entity2)
        data = EntityResolutionData([pair])
        
        benchmark.add_resolution_data(SplitType.TRAIN, data)
        benchmark.add_resolution_data(SplitType.TEST, data)
        
        metadata = benchmark.get_metadata()
        assert metadata["name"] == "test_benchmark"
        assert metadata["description"] == "Test description"
        assert metadata["domain"] == "test_domain"
        assert metadata["version"] == "1.0"
        assert metadata["num_splits"] == 2
        assert metadata["total_pairs"] == 2

