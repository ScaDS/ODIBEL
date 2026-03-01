import os
import yaml
from typing import List, Dict, Optional
from pydantic import BaseModel, Field

class PropertyModel(BaseModel):
    property: str
    equal: List[str]
    range: Optional[str] = None
    as_: Optional[str] = Field(default=None, alias='as')

    def should_fetch_specific_property(self) -> bool:
        """Check if this property should fetch a specific property instead of full resource."""
        return self.as_ is not None

    def get_fetch_property(self) -> Optional[str]:
        """Get the specific property to fetch (e.g., rdfs:label)."""
        if not self.as_:
            return None
        # Parse property and language from as field (e.g., "rdfs:label@en" -> "rdfs:label")
        return self.as_.split('@')[0]

    def get_fetch_language(self) -> Optional[str]:
        """Get the language code from the as field (e.g., "rdfs:label@en" -> "en")."""
        if not self.as_ or '@' not in self.as_:
            return None
        return self.as_.split('@')[1]

class OntologyClassModel(BaseModel):
    class_: str = Field(alias='class')
    properties: List[PropertyModel]

class OntologyConfigModel(BaseModel):
    name: str
    prefixes: List[Dict[str, str]]
    ontology: List[OntologyClassModel]

# --- Prefix expansion utilities ---
def build_prefix_map(prefixes: List[Dict[str, str]]) -> Dict[str, str]:
    """Build a prefix map from the config."""
    return {p['prefix']: p['uri'] for p in prefixes}

def expand_prefixed_name(name: str, prefix_map: Dict[str, str]) -> str:
    """Expand a prefixed name (e.g., dbo:birthDate) to a full URI."""
    if ':' in name:
        prefix, local = name.split(':', 1)
        if prefix in prefix_map:
            return prefix_map[prefix] + local
    return name  # Already a full URI or unknown prefix

def expand_config_uris(config: OntologyConfigModel) -> OntologyConfigModel:
    """Expand all class/property URIs in the config to full URIs."""
    prefix_map = build_prefix_map(config.prefixes)
    for cls in config.ontology:
        # Expand class name
        cls.class_ = expand_prefixed_name(cls.class_, prefix_map)
        for prop in cls.properties:
            prop.property = expand_prefixed_name(prop.property, prefix_map)
            if prop.range:
                prop.range = expand_prefixed_name(prop.range, prefix_map)
            if prop.as_:
                prop.as_ = expand_prefixed_name(prop.as_, prefix_map)
            # Also expand 'equal' list
            prop.equal = [expand_prefixed_name(eq, prefix_map) for eq in prop.equal]
    return config

def load_config(config_path: str) -> OntologyConfigModel:
    """
    Loads and parses any ontology configuration file as a Pydantic model.
    Expands all class/property URIs to full URIs.
    Args:
        config_path: Path to the configuration file
    Returns:
        OntologyConfigModel: Generic ontology configuration.
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    model = OntologyConfigModel(**config)
    return expand_config_uris(model)

def get_movie_ontology() -> OntologyConfigModel:
    """
    Loads and parses the movie ontology configuration.
    Returns:
        OntologyConfigModel: Movie ontology configuration.
    """
    # Go up from src/pyodibel/acquisition_simple/ to project root
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'movie.conf')
    return load_config(config_path) 