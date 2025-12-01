from click import group, option, pass_context
import click
from pyodibel.source.wikidata.mapping import map_external_ids
from pyodibel.source.wikidata import WikidataDumpSource
from pyodibel.source.wikidata.entity import WikidataEntity, ExternalIDClaim, WikidataClaim
from pyodibel.core.data_source import DataSourceConfig
from pyodibel.core.backend import BackendConfig, BackendType
from typing import Dict, Any

@group()
@pass_context
def wikidata_cmd(ctx: click.Context):
    pass

@wikidata_cmd.command("extract-external-ids")
@option("--output-path", type=click.Path(), required=True)
@option("--input-path", type=click.Path(), required=True)
@pass_context
def wikidata_extract_external_ids(ctx: click.Context, output_path: str, input_path: str):
    """Test extracting external ID claims using backend-native operations."""
    from pyspark.core.rdd import PipelinedRDD
    from pyodibel.source.wikidata import WikidataDumpSource

    wikidata_source = WikidataDumpSource(DataSourceConfig(
        name="test_wikidata_dump",
        description="Test Wikidata dump source",
        metadata={"test": True}
    ))

    # Get backend-native RDD (lazy, not collected)
    entities_rdd: PipelinedRDD = wikidata_source.fetch_backend(input_path, limit=5)
    
    # Get Spark session for DataFrame operations
    backend = wikidata_source._get_backend()
    spark = backend.get_session()
    
    # Convert dict to WikidataEntity, then extract external IDs
    # map_external_ids returns a list, so we use flatMap
    def process_entity(entity_dict: Dict[str, Any]):
        """Convert dict to entity and extract external IDs."""
        try:
            # Create WikidataEntity from dict (without validation for speed)
            entity = WikidataEntity.model_construct(**entity_dict)
            # Extract external IDs (returns list)
            return map_external_ids(entity)
        except Exception as e:
            # Skip invalid entities
            print(f"Error processing entity: {e}")
            return []
    
    # Process entities and flatten results
    external_ids_rdd = entities_rdd.flatMap(process_entity)
    
    # Convert to DataFrame - need to convert Pydantic models to dicts first
    def to_dict(claim):
        """Convert ExternalIDClaim to dict for DataFrame."""
        if hasattr(claim, 'model_dump'):
            return claim.model_dump()
        elif hasattr(claim, 'dict'):
            return claim.dict()
        else:
            return {"id": claim.id, "property": claim.property, "value": str(claim.value)}
    
    # Convert to dicts and create DataFrame
    external_ids_dicts = external_ids_rdd.map(to_dict)
    
    # Create DataFrame from RDD of dicts
    from pyspark.sql import Row, DataFrame
    
    def dict_to_row(claim_dict):
        """Convert dict to Spark Row."""
        return Row(**claim_dict)
    
    rows_rdd = external_ids_dicts.map(dict_to_row)
    
    # Create DataFrame
    df: DataFrame = spark.createDataFrame(rows_rdd)
    df.write.csv(output_path)
    