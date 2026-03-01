from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from pyodibel.api.source import Source, SourceConfig
from pyodibel.source.gradoop.entity import GradoopEntity
from pyodibel.operations.gradoop import reader
from pyodibel.operations.gradoop.cluster import Cluster
from pyodibel.operations.gradoop.entity import Entity as LegacyEntity


class GradoopSource(Source):
    """
    Data source wrapper around the legacy Gradoop reader utilities.

    This class exposes a thin, typed interface for loading entities and
    clusters that were exported by Gradoop/FAMER either as JSONL or CSV
    (metadata + vertices/edges). It reuses the existing functions in
    ``pyodibel.operations.gradoop.reader`` to keep behaviour identical to
    the earlier implementation while providing a modern data source API.
    """

    SUPPORTED_FORMATS = {"gradoop", "gradoop-json", "gradoop-csv", "json", "csv"}

    def __init__(self, config: SourceConfig):
        """
        Args:
            config: Source configuration. The `location` field should point to
                   the folder containing the Gradoop export. Optional `format`
                   can be "json" or "csv" for explicit format selection.
        """
        super().__init__(config)
        self._clusters: List[Cluster] = []
        self._cluster_graphs: Dict = {}

    def read_entities(self, source_pair: Optional[Tuple[str, str]] = None) -> Iterator[GradoopEntity]:
        """
        Read Gradoop entities from the source.
        
        Args:
            source_pair: Optional pair of source names; when set, only entities
                         belonging to those sources are loaded.
        
        Yields:
            GradoopEntity objects representing entities from the Gradoop export.
        """
        folder = self._resolve_folder()
        fmt = self.config.format
        if fmt and not self.supports_format(fmt):
            raise ValueError(f"Unsupported format '{fmt}'. Supported: {sorted(self.SUPPORTED_FORMATS)}")

        # Choose the appropriate reader based on format and filtering needs.
        if source_pair:
            if fmt == "csv":
                entities, cluster_graphs, clusters = reader.read_csv_famer_data_for_pairs(str(folder), source_pair)
            elif fmt == "json":
                entities, cluster_graphs, clusters = reader.read_json_famer_data_source_pairs(str(folder), source_pair)
            else:
                entities, cluster_graphs, clusters = reader.read_data_for_pairs(str(folder), source_pair)
        else:
            if fmt == "csv":
                entities, cluster_graphs, clusters = reader.read_csv_famer_data(str(folder))
            elif fmt == "json":
                entities, cluster_graphs, clusters = reader.read_json_famer_data(str(folder))
            else:
                entities, cluster_graphs, clusters = reader.read_data(str(folder))

        self._clusters = clusters
        self._cluster_graphs = cluster_graphs

        # Convert legacy Entity objects to GradoopEntity
        for legacy_entity in entities.values():
            gradoop_entity = self._convert_to_gradoop_entity(legacy_entity, clusters)
            yield gradoop_entity
    
    def read_raw(self) -> Iterator[Any]:
        """
        Read raw data from the source.
        
        Yields:
            Raw legacy Entity objects from the Gradoop export.
        """
        folder = self._resolve_folder()
        entities, cluster_graphs, clusters = reader.read_data(str(folder))
        self._clusters = clusters
        self._cluster_graphs = cluster_graphs
        yield from entities.values()
    
    def get_schema(self) -> Dict[str, Any]:
        """
        Get the schema/structure of the Gradoop data source.
        
        Returns:
            Dictionary describing the schema
        """
        return {
            "type": "gradoop",
            "format": self.config.format or "auto",
            "entity_type": "GradoopEntity",
            "fields": ["id", "source", "iri", "resource", "cluster_id", "properties"]
        }
    
    def validate(self) -> bool:
        """
        Validate that the source is accessible and properly configured.
        
        Returns:
            True if source is valid, False otherwise
        """
        try:
            folder = self._resolve_folder()
            return folder.exists()
        except (ValueError, FileNotFoundError):
            return False
    
    def get_clusters(self) -> List[Cluster]:
        """Get the clusters loaded from the last read operation."""
        return self._clusters
    
    def _convert_to_gradoop_entity(self, legacy_entity: LegacyEntity, clusters: List[Cluster]) -> GradoopEntity:
        """
        Convert a legacy Entity object to a GradoopEntity.
        
        Args:
            legacy_entity: Legacy Entity object from reader
            clusters: List of clusters to find cluster_id
            
        Returns:
            GradoopEntity object
        """
        # Find cluster_id by searching through clusters
        cluster_id = None
        if legacy_entity.cluster_id:
            cluster_id = legacy_entity.cluster_id
        else:
            # Try to find the cluster containing this entity
            for cluster in clusters:
                if legacy_entity.iri in cluster.entities:
                    cluster_id = str(cluster.id)
                    break
        
        return GradoopEntity(
            id=legacy_entity.iri,
            source=legacy_entity.resource,
            iri=legacy_entity.iri,
            resource=legacy_entity.resource,
            cluster_id=cluster_id,
            properties=legacy_entity.properties or {}
        )

    def supports_format(self, format: str) -> bool:
        """
        Check if this source understands the requested format.

        Accepted identifiers cover both the raw "json"/"csv" hints and the
        more explicit "gradoop-*" markers.
        """
        return format.lower() in self.SUPPORTED_FORMATS

    def _resolve_folder(self) -> Path:
        """Resolve the folder from config or raise a helpful error."""
        location = self.config.location
        if not location:
            raise ValueError("No location configured for GradoopSource.")
        folder = Path(location)
        if not folder.exists():
            raise FileNotFoundError(f"Gradoop input folder does not exist: {folder}")
        return folder
