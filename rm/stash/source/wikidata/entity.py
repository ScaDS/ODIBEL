"""
Wikidata entity models.

Pydantic models for representing Wikidata entities from JSON dumps.
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class WikidataLabel(BaseModel):
    """Wikidata label in a specific language."""
    language: str
    value: str


class WikidataDescription(BaseModel):
    """Wikidata description in a specific language."""
    language: str
    value: str


class WikidataAlias(BaseModel):
    """Wikidata alias in a specific language."""
    language: str
    value: str  # In Wikidata dumps, alias value is a string, not a list


class WikidataSnak(BaseModel):
    """Wikidata snak (statement value)."""
    snaktype: str
    property: str
    datatype: Optional[str] = None
    datavalue: Optional[Dict[str, Any]] = None


class WikidataClaim(BaseModel):
    """Wikidata claim (statement)."""
    mainsnak: WikidataSnak
    type: Optional[str] = None
    rank: Optional[str] = None
    qualifiers: Optional[Dict[str, List[WikidataSnak]]] = None
    references: Optional[List[Dict[str, Any]]] = None


class WikidataSiteLink(BaseModel):
    """Wikidata site link."""
    site: str
    title: str
    badges: Optional[List[str]] = None


class WikidataEntity(BaseModel):
    """
    Wikidata entity from JSON dump.
    
    Represents a single Wikidata entity (item or property) as it appears
    in Wikidata JSON dumps.
    """
    
    id: str = Field(..., description="Entity ID (e.g., 'Q123' or 'P31')")
    type: str = Field(..., description="Entity type ('item' or 'property')")
    
    labels: Optional[Dict[str, WikidataLabel]] = Field(
        None,
        description="Labels in different languages"
    )
    
    descriptions: Optional[Dict[str, WikidataDescription]] = Field(
        None,
        description="Descriptions in different languages"
    )
    
    aliases: Optional[Dict[str, List[WikidataAlias]]] = Field(
        None,
        description="Aliases in different languages"
    )
    
    claims: Optional[Dict[str, List[WikidataClaim]]] = Field(
        None,
        description="Claims (statements) keyed by property ID"
    )
    
    sitelinks: Optional[Dict[str, WikidataSiteLink]] = Field(
        None,
        description="Site links keyed by site identifier"
    )
    
    lastrevid: Optional[int] = Field(None, description="Last revision ID")
    
    modified: Optional[str] = Field(None, description="Last modification timestamp")
    
    def get_label(self, language: str = "en") -> Optional[str]:
        """Get label in specified language."""
        if self.labels and language in self.labels:
            return self.labels[language].value
        return None
    
    def get_description(self, language: str = "en") -> Optional[str]:
        """Get description in specified language."""
        if self.descriptions and language in self.descriptions:
            return self.descriptions[language].value
        return None
    
    def get_claims(self, property_id: str) -> List[WikidataClaim]:
        """Get all claims for a specific property."""
        if self.claims and property_id in self.claims:
            return self.claims[property_id]
        return []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return self.model_dump(exclude_none=True)

class ExternalIDClaim(BaseModel):
    id: str
    type: str
    property: str
    value: str