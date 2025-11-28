# Wikidata Dump Ingestion

This guide explains how to ingest Wikidata JSON dumps using PyODIBEL with Spark.

## Overview

Wikidata dumps are large files (often GBs or TBs) containing JSON data where each line or element represents a Wikidata entity. PyODIBEL provides a `WikidataDumpSource` that uses Spark for efficient distributed processing of these large files.

## Wikidata Dump Formats

Wikidata dumps come in two main formats:

1. **JSONL (JSON Lines)**: One JSON object per line - most common format
2. **JSON Array**: Single JSON array containing all entities

PyODIBEL automatically detects the format, but you can also specify it explicitly.

## Installation

Make sure you have PySpark installed:

```bash
pip install pyspark
```

## Basic Usage

### Simple Ingestion

```python
from pyodibel.core.data_source import DataSourceConfig
from pyodibel.source import WikidataDumpSource

# Configure the source
source_config = DataSourceConfig(
    name="wikidata_dump",
    description="Wikidata JSON dump"
)

# Create source (uses Spark by default)
source = WikidataDumpSource(source_config)

# Path to your Wikidata dump file
dump_path = "/path/to/wikidata-latest-all.json"

# Fetch entities (limit for testing)
entities = source.fetch(
    dump_path,
    limit=100,  # Limit to first 100 entities
    jsonl=True  # Specify format (optional, auto-detected)
)

# Process entities
for entity in entities:
    entity_id = entity.get("id")
    labels = entity.get("labels", {})
    print(f"Entity: {entity_id}, Labels: {labels}")
```

### Streaming Ingestion

For large dumps, use streaming to process in chunks:

```python
# Process in chunks of 1000 entities
for chunk in source.fetch_stream(
    dump_path,
    chunk_size=1000,
    jsonl=True
):
    print(f"Processing chunk with {len(chunk)} entities...")
    
    for entity in chunk:
        # Your processing logic here
        entity_id = entity.get("id")
        # Process entity...
```

### Filtered Ingestion

Filter entities during ingestion:

```python
entities = source.fetch(
    dump_path,
    filters={
        "labels.en": {"$exists": True},  # Only entities with English labels
        "type": "item"  # Only items (not properties)
    },
    select_fields=["id", "labels", "descriptions", "claims"],
    limit=1000
)
```

### Custom Spark Configuration

Configure Spark for optimal performance:

```python
from pyodibel.core.backend import BackendConfig, BackendType

# Custom Spark configuration
backend_config = BackendConfig(
    backend_type=BackendType.SPARK,
    config={
        "app_name": "WikidataIngestion",
        "master": "local[*]",  # or "yarn", "spark://host:port", etc.
        "spark_config": {
            "spark.driver.memory": "16g",
            "spark.executor.memory": "16g",
            "spark.sql.shuffle.partitions": "400",
            "spark.default.parallelism": "200",
        }
    }
)

source = WikidataDumpSource(source_config, backend_config=backend_config)
```

## Filter Operators

The `filters` parameter supports various operators:

- `{"$eq": value}`: Equal to
- `{"$ne": value}`: Not equal to
- `{"$in": [values]}`: In list
- `{"$gt": value}`: Greater than
- `{"$lt": value}`: Less than
- `{"$exists": True/False}`: Field exists/doesn't exist

Example:

```python
filters = {
    "labels.en": {"$exists": True},
    "claims.P31": {"$exists": True},  # Has instance of property
}
```

## Entity Structure

Wikidata entities have the following structure:

```json
{
  "id": "Q123",
  "type": "item",
  "labels": {
    "en": {"language": "en", "value": "Entity Name"}
  },
  "descriptions": {
    "en": {"language": "en", "value": "Description"}
  },
  "claims": {
    "P31": [  // Property ID
      {
        "mainsnak": {
          "datavalue": {
            "value": {"id": "Q5"}  // Human
          }
        }
      }
    ]
  },
  "aliases": {...},
  "sitelinks": {...}
}
```

## Saving Processed Data

You can save processed data using the Spark backend:

```python
from pyodibel.core.backend import BackendConfig, BackendType

source = WikidataDumpSource(source_config)
backend = source._get_backend()

# Load dump
df = backend.load_data(dump_path, format="json", jsonl=True)

# Process/transform
processed_df = df.select("id", "labels", "descriptions", "claims")

# Save as Parquet (efficient format)
backend.save_data(
    processed_df,
    "/path/to/output",
    format="parquet",
    mode="overwrite"
)
```

## Performance Tips

1. **Use Parquet**: Convert JSON to Parquet for faster subsequent reads
2. **Repartition**: Adjust partitions based on data size
3. **Memory**: Increase Spark memory for large dumps
4. **Selective Fields**: Use `select_fields` to only load needed data
5. **Filter Early**: Apply filters before processing to reduce data size

## Example: Extract Specific Entity Types

```python
# Extract only human entities (P31=Q5)
entities = source.fetch(
    dump_path,
    filters={
        "claims.P31": {
            "$in": [{"mainsnak": {"datavalue": {"value": {"id": "Q5"}}}}]
        }
    },
    select_fields=["id", "labels", "descriptions"],
    jsonl=True
)
```

## Downloading Wikidata Dumps

Wikidata dumps can be downloaded from:

- **Latest All**: https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz
- **Latest Entities**: https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2

Use tools like `wget` or `curl` to download:

```bash
wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz
gunzip latest-all.json.gz
```

## Troubleshooting

### Out of Memory Errors

Increase Spark memory:

```python
backend_config = BackendConfig(
    backend_type=BackendType.SPARK,
    config={
        "spark_config": {
            "spark.driver.memory": "32g",
            "spark.executor.memory": "32g",
        }
    }
)
```

### Slow Processing

1. Increase partitions: `spark.sql.shuffle.partitions`
2. Use Parquet instead of JSON
3. Filter data early
4. Process in smaller chunks

### Format Detection Issues

Explicitly specify format:

```python
source.fetch(dump_path, jsonl=True)  # or jsonl=False for JSON array
```

## See Also

- [Operations Examples](OPERATIONS_EXAMPLES.md)
- [Backend and Format Design](BACKEND_AND_FORMAT_DESIGN.md)
- [Examples](../examples/wikidata_ingestion_example.py)

