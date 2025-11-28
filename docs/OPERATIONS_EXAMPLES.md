# Operations with Multiple Backends and Formats - Examples

## Overview

This document shows concrete examples of how operations work with different backends (Spark, Pandas, Native) and formats (CSV, JSON, RDF).

## Example 1: Filter Operation with Spark and CSV

```python
from pyodibel.core.operations import FilterOperation, OperationConfig
from pyodibel.core.backend import BackendConfig, BackendType

class SparkCSVFilter(FilterOperation):
    def _apply_backend(self, data, backend, **kwargs):
        """Filter using Spark DataFrame operations."""
        criteria = kwargs.get('criteria', {})
        df = data  # data is already a Spark DataFrame
        
        for key, value in criteria.items():
            df = df.filter(df[key] == value)
        
        return df

# Usage
config = OperationConfig(
    name="filter_persons",
    backend=BackendConfig(
        backend_type=BackendType.SPARK,
        config={"master": "local[*]", "app_name": "FilterOperation"}
    ),
    input_format="csv",
    output_format="csv"
)

filter_op = SparkCSVFilter(config)

# Automatically loads CSV as Spark DataFrame, filters, and saves as CSV
result = filter_op.apply_with_backend(
    data="/path/to/input.csv",
    target="/path/to/output.csv",
    criteria={"type": "Person"}
)
```

## Example 2: Transform Operation with Pandas and JSON

```python
from pyodibel.core.operations import TransformOperation

class PandasJSONTransform(TransformOperation):
    def _apply_backend(self, data, backend, **kwargs):
        """Transform using Pandas DataFrame operations."""
        mapping = kwargs.get('mapping', {})
        df = data  # data is already a Pandas DataFrame
        
        # Apply field mapping
        df = df.rename(columns=mapping)
        
        # Add computed columns
        if 'computed_fields' in kwargs:
            for field, expr in kwargs['computed_fields'].items():
                df[field] = eval(expr, {"df": df})
        
        return df

# Usage
config = OperationConfig(
    name="transform_json",
    backend=BackendConfig(backend_type=BackendType.PANDAS),
    input_format="json",
    output_format="json"
)

transform_op = PandasJSONTransform(config)

result = transform_op.apply_with_backend(
    data="/path/to/input.json",
    target="/path/to/output.json",
    mapping={"old_name": "new_name", "old_age": "new_age"},
    computed_fields={"full_name": "df['first_name'] + ' ' + df['last_name']"}
)
```

## Example 3: RDF Filter with Native Backend

```python
from pyodibel.core.operations import FilterOperation
from rdflib import Graph

class RDFNativeFilter(FilterOperation):
    def _apply_backend(self, data, backend, **kwargs):
        """Filter RDF using RDFLib (native Python)."""
        criteria = kwargs.get('criteria', {})
        graph = data  # data is already an RDFLib Graph
        
        # Filter by predicate
        if 'predicate' in criteria:
            predicate = criteria['predicate']
            filtered_triples = [
                (s, p, o) for s, p, o in graph
                if str(p) == predicate
            ]
            result_graph = Graph()
            for triple in filtered_triples:
                result_graph.add(triple)
            return result_graph
        
        # Filter by object value
        if 'object_value' in criteria:
            obj_value = criteria['object_value']
            filtered_triples = [
                (s, p, o) for s, p, o in graph
                if str(o) == obj_value
            ]
            result_graph = Graph()
            for triple in filtered_triples:
                result_graph.add(triple)
            return result_graph
        
        return graph

# Usage
config = OperationConfig(
    name="filter_rdf",
    backend=BackendConfig(backend_type=BackendType.NATIVE),
    input_format="rdf",
    output_format="rdf"
)

filter_op = RDFNativeFilter(config)

result = filter_op.apply_with_backend(
    data="/path/to/input.nt",
    target="/path/to/output.nt",
    criteria={"predicate": "http://dbpedia.org/ontology/birthDate"}
)
```

## Example 4: Join Operation with Spark (Multiple Formats)

```python
from pyodibel.core.operations import JoinOperation

class SparkJoin(JoinOperation):
    def _apply_backend(self, data, backend, **kwargs):
        """Join multiple datasets using Spark."""
        datasets = kwargs.get('datasets', [])
        strategy = kwargs.get('strategy', 'inner')
        on = kwargs.get('on', None)
        
        spark = backend.get_session()
        
        # Load all datasets
        dfs = []
        for dataset_path, format_type in datasets:
            if format_type == 'csv':
                df = spark.read.csv(dataset_path, header=True, inferSchema=True)
            elif format_type == 'json':
                df = spark.read.json(dataset_path)
            elif format_type == 'parquet':
                df = spark.read.parquet(dataset_path)
            dfs.append(df)
        
        # Join datasets
        result = dfs[0]
        for df in dfs[1:]:
            if on:
                result = result.join(df, on=on, how=strategy)
            else:
                # Natural join on common columns
                common_cols = set(result.columns) & set(df.columns)
                if common_cols:
                    join_col = list(common_cols)[0]
                    result = result.join(df, on=join_col, how=strategy)
        
        return result

# Usage
config = OperationConfig(
    name="join_datasets",
    backend=BackendConfig(backend_type=BackendType.SPARK),
    output_format="parquet"
)

join_op = SparkJoin(config)

result = join_op.apply_with_backend(
    datasets=[
        ("/path/to/csv_data.csv", "csv"),
        ("/path/to/json_data.json", "json"),
        ("/path/to/parquet_data.parquet", "parquet")
    ],
    strategy="inner",
    on="entity_id"
)
```

## Example 5: Format Conversion Operation

```python
from pyodibel.core.operations import ConvertOperation

class FormatConverter(ConvertOperation):
    def _apply_backend(self, data, backend, **kwargs):
        """Convert between formats using the backend."""
        to_format = kwargs.get('to_format', self.config.output_format)
        from_format = kwargs.get('from_format', self.config.input_format)
        
        # The backend and format handlers handle the actual conversion
        # This is just a pass-through that uses the backend's capabilities
        return data
    
    def convert(self, data, from_format, to_format, **kwargs):
        """Convert data between formats."""
        # Use apply_with_backend which handles format conversion
        return self.apply_with_backend(
            data,
            input_format=from_format,
            output_format=to_format,
            **kwargs
        )
    
    def supports_conversion(self, from_format, to_format):
        """Check if conversion is supported."""
        # Check if both formats are supported by the backend
        backend = self._get_backend()
        if backend is None:
            return False
        
        return (
            backend.supports_format(from_format) and
            backend.supports_format(to_format)
        )

# Usage: CSV to JSON with Pandas
config = OperationConfig(
    name="csv_to_json",
    backend=BackendConfig(backend_type=BackendType.PANDAS),
    input_format="csv",
    output_format="json"
)

converter = FormatConverter(config)

result = converter.convert(
    data="/path/to/data.csv",
    from_format="csv",
    to_format="json",
    target="/path/to/output.json"
)
```

## Example 6: Backend-Agnostic Operation

```python
from pyodibel.core.operations import FilterOperation

class UniversalFilter(FilterOperation):
    """Filter operation that works with any backend."""
    
    def _apply_backend(self, data, backend, **kwargs):
        """Apply filter using backend-specific operations."""
        criteria = kwargs.get('criteria', {})
        backend_type = backend.config.backend_type
        
        if backend_type == BackendType.SPARK:
            # Spark DataFrame filtering
            df = data
            for key, value in criteria.items():
                df = df.filter(df[key] == value)
            return df
        
        elif backend_type == BackendType.PANDAS:
            # Pandas DataFrame filtering
            df = data
            for key, value in criteria.items():
                df = df[df[key] == value]
            return df
        
        else:  # Native
            # Native Python filtering
            return [
                row for row in data
                if all(row.get(k) == v for k, v in criteria.items())
            ]
    
    def filter(self, data, criteria, **kwargs):
        """Fallback native implementation."""
        return [
            row for row in data
            if all(row.get(k) == v for k, v in criteria.items())
        ]

# Usage with different backends
# Spark
spark_config = OperationConfig(
    name="filter",
    backend=BackendConfig(backend_type=BackendType.SPARK),
    input_format="csv"
)
spark_filter = UniversalFilter(spark_config)

# Pandas
pandas_config = OperationConfig(
    name="filter",
    backend=BackendConfig(backend_type=BackendType.PANDAS),
    input_format="json"
)
pandas_filter = UniversalFilter(pandas_config)

# Native
native_config = OperationConfig(
    name="filter",
    backend=BackendConfig(backend_type=BackendType.NATIVE),
    input_format="csv"
)
native_filter = UniversalFilter(native_config)
```

## Key Points

1. **Backend Selection**: Operations can specify which backend to use via `BackendConfig`
2. **Format Handling**: Format handlers automatically load/save data in the correct format
3. **Backend-Specific Logic**: `_apply_backend()` method contains backend-specific implementations
4. **Fallback**: Operations can work without backends using native Python
5. **Flexibility**: Same operation interface works with different backends and formats

## Implementation Checklist

When implementing a new operation:

- [ ] Override `_apply_backend()` for backend-specific optimizations
- [ ] Provide native `apply()` fallback for when no backend is used
- [ ] Support multiple formats via format handlers
- [ ] Handle backend initialization and cleanup
- [ ] Document supported backends and formats

