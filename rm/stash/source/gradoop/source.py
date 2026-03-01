from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from pyodibel.api.data_source import DataSource, DataSourceConfig
from pyodibel.api.entity import DataEntity
from pyodibel.source.gradoop.entity import GradoopEntity
from pyodibel.operations.gradoop import reader
from pyodibel.operations.gradoop.cluster import Cluster
from pyodibel.operations.gradoop.entity import Entity as LegacyEntity


class GradoopDataSource(DataSource):
    """
    Data source wrapper around the legacy Gradoop reader utilities.

    This class exposes a thin, typed interface for loading entities and
    clusters that were exported by Gradoop/FAMER either as JSONL or CSV
    (metadata + vertices/edges). It reuses the existing functions in
    ``pyodibel.operations.gradoop.reader`` to keep behaviour identical to
    the earlier implementation while providing a modern data source API.
    """

    SUPPORTED_FORMATS = {"gradoop", "gradoop-json", "gradoop-csv", "json", "csv"}

    def __init__(self, config: DataSourceConfig, default_path: Optional[str] = None, default_format: Optional[str] = None):
        """
        Args:
            config: Generic data source configuration.
            default_path: Optional folder containing the Gradoop export
                         (used when no path is passed to ``fetch``).
            default_format: Optional explicit format hint ("json" or "csv").
                            If not provided, ``fetch`` will auto-detect via
                            ``reader.read_data``.
        """
        super().__init__(config)
        self.default_path = Path(default_path) if default_path else None
        self.default_format = default_format.lower() if default_format else None

    def fetch(
        self,
        input_folder: Optional[str | Path] = None,
        source_pair: Optional[Tuple[str, str]] = None,
        format: Optional[str] = None,
        **kwargs: Any,
    ) -> Iterator[GradoopEntity]:
        """
        Fetch Gradoop entities from the source.
        
        Args:
            input_folder: Path to the folder with metadata/vertices/edges.
                          Falls back to ``default_path`` or the ``input_folder``
                          entry in ``config.metadata``.
            source_pair: Optional pair of source names; when set, only entities
                         belonging to those sources are loaded.
            format: Optional explicit format hint ("json" or "csv"). If omitted,
                    auto-detection is used.
            **kwargs: Additional parameters (ignored for now).
        
        Yields:
            GradoopEntity objects representing entities from the Gradoop export.
        """
        folder = self._resolve_folder(input_folder)
        fmt = (format or self.default_format)
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

        # Convert legacy Entity objects to GradoopEntity
        for legacy_entity in entities.values():
            gradoop_entity = self._convert_to_gradoop_entity(legacy_entity, clusters)
            yield gradoop_entity
    
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
            id=legacy_entity.iri,  # Set id explicitly
            source=legacy_entity.resource,  # Set source explicitly
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

    def _resolve_folder(self, input_folder: Optional[str | Path]) -> Path:
        """Resolve the folder argument or raise a helpful error."""
        candidate = input_folder or self.default_path or self.config.metadata.get("input_folder")
        if candidate is None:
            raise ValueError("No input_folder provided and no default_path configured for GradoopDataSource.")
        folder = Path(candidate)
        if not folder.exists():
            raise FileNotFoundError(f"Gradoop input folder does not exist: {folder}")
        return folder