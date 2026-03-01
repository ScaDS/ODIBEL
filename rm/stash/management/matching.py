"""
Abstract interfaces for entity matching.

Entity matching identifies and links entities across different sources,
creating same-as links or match pairs for evaluation.
"""

from abc import ABC, abstractmethod
from typing import Any, Iterator, Optional, Dict, List, Tuple
from pydantic import BaseModel


class MatchingConfig(BaseModel):
    """Configuration for entity matching."""
    
    name: str
    description: Optional[str] = None
    parameters: Dict[str, Any] = {}


class MatchResult(BaseModel):
    """Result of an entity match."""
    
    source_id: str
    target_id: str
    confidence: float
    match_type: str  # exact, variant, broader, narrower, related
    metadata: Dict[str, Any] = {}


class EntityMatching(ABC):
    """
    Abstract base class for entity matching strategies.
    
    Entity matching identifies entities across different sources that
    refer to the same real-world entity, creating links for evaluation.
    """
    
    def __init__(self, config: MatchingConfig):
        """
        Initialize the matching strategy.
        
        Args:
            config: Configuration for matching
        """
        self.config = config
    
    @abstractmethod
    def match(self, source_entities: List[Any], target_entities: List[Any], **kwargs) -> List[MatchResult]:
        """
        Match entities between source and target datasets.
        
        Args:
            source_entities: List of entities from source dataset
            target_entities: List of entities from target dataset
            **kwargs: Additional matching parameters
            
        Returns:
            List of match results
        """
        pass
    
    @abstractmethod
    def find_matches(self, entity: Any, candidates: List[Any], **kwargs) -> List[MatchResult]:
        """
        Find matches for a single entity in a candidate set.
        
        Args:
            entity: Entity to find matches for
            candidates: List of candidate entities
            **kwargs: Additional matching parameters
            
        Returns:
            List of match results
        """
        pass
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the matching strategy.
        
        Returns:
            Dictionary containing metadata
        """
        return {
            "name": self.config.name,
            "description": self.config.description,
            "parameters": self.config.parameters
        }

