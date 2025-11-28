# Linked Data Source

## Overview

The `LinkedDataSource` provides a way to consume and crawl RDF graphs from the web using HTTP Linked Data requests with content negotiation. It follows the Linked Data principles for accessing RDF data from URIs.

## Features

- **HTTP Content Negotiation**: Automatically requests RDF in preferred formats (N-Triples, Turtle, RDF/XML, JSON-LD)
- **Recursive Crawling**: Follows links in RDF graphs to discover related resources
- **Depth Control**: Limit crawling depth to prevent infinite loops
- **Rate Limiting**: Respectful crawling with configurable delays
- **Domain Filtering**: Restrict crawling to specific domains
- **Predicate Filtering**: Only follow links with specific predicates
- **Format Detection**: Automatically detects and parses various RDF formats

## Basic Usage

### Fetch a Single URI

```python
from pyodibel.core.data_source import DataSourceConfig
from pyodibel.source import LinkedDataSource

config = DataSourceConfig(
    name="linked_data",
    description="Linked Data source"
)

source = LinkedDataSource(config, max_depth=0, follow_links=False)

# Fetch a single DBpedia resource
uri = "http://dbpedia.org/resource/Berlin"
graphs = list(source.fetch(uri))

if graphs:
    graph = graphs[0]
    print(f"Retrieved {len(graph)} triples")
```

### Crawl Linked Data

```python
# Configure for crawling
source = LinkedDataSource(
    config,
    max_depth=2,  # Crawl up to 2 levels deep
    follow_links=True,
    rate_limit=1.0,  # 1 second between requests
    allowed_domains={"dbpedia.org"}  # Only crawl DBpedia
)

seed_uris = ["http://dbpedia.org/resource/Berlin"]

# Crawl starting from seed URIs
for graph in source.crawl(seed_uris, max_depth=2):
    print(f"Retrieved {len(graph)} triples")
```

## Configuration Options

### LinkedDataSource Parameters

- **`max_depth`** (int, default=3): Maximum depth for recursive crawling
  - `0` = no recursion, only fetch the seed URI
  - `1` = fetch seed + direct links
  - `2+` = recursive crawling

- **`follow_links`** (bool, default=True): Whether to follow links found in RDF graphs

- **`rate_limit`** (float, default=1.0): Minimum seconds between HTTP requests

- **`max_retries`** (int, default=3): Maximum number of retries for failed requests

- **`timeout`** (int, default=10): HTTP request timeout in seconds

- **`allowed_domains`** (Set[str], optional): Set of allowed domains for crawling
  - `None` = allow all domains
  - `{"dbpedia.org"}` = only crawl DBpedia

## Advanced Usage

### Filtered Crawling

Only follow links with specific predicates:

```python
source = LinkedDataSource(config, max_depth=2)

# Only follow owl:sameAs links
follow_predicates = [
    "http://www.w3.org/2002/07/owl#sameAs"
]

for graph in source.crawl(
    seed_uris=["http://dbpedia.org/resource/Berlin"],
    follow_predicates=follow_predicates
):
    print(f"Retrieved {len(graph)} triples")
```

### Domain-Restricted Crawling

Restrict crawling to specific domains:

```python
source = LinkedDataSource(
    config,
    allowed_domains={"dbpedia.org", "wikidata.org"},
    max_depth=1
)

# Only URIs from allowed domains will be crawled
for graph in source.crawl(seed_uris):
    ...
```

### Custom Format Preferences

Override default format preferences:

```python
custom_formats = {
    "application/turtle": 1.0,  # Prefer Turtle
    "application/n-triples": 0.8,
    "application/rdf+xml": 0.5,
}

source = LinkedDataSource(config)
graph = next(source.fetch(uri, format_preference=custom_formats))
```

## RDF Format Support

The Linked Data source supports these RDF formats (in order of preference):

1. **application/n-triples** (highest priority)
2. **application/turtle** / **text/turtle**
3. **application/rdf+xml**
4. **application/ld+json**
5. **application/json** (may contain JSON-LD)

The source automatically tries formats in order until one succeeds.

## HTTP Content Negotiation

The source uses HTTP `Accept` headers to request RDF data:

```http
GET /resource/Berlin HTTP/1.1
Host: dbpedia.org
Accept: application/n-triples
User-Agent: PyODIBEL/1.0 (Linked Data Client)
```

The server responds with RDF in the requested format (if supported).

## Link Following

When `follow_links=True`, the source:

1. Fetches the seed URI
2. Extracts all URIs from object positions in the RDF graph
3. Follows those URIs (up to `max_depth`)
4. Repeats recursively

This enables crawling Linked Data clouds like DBpedia, Wikidata, etc.

## Best Practices

1. **Rate Limiting**: Always set appropriate `rate_limit` to be respectful
   ```python
   source = LinkedDataSource(config, rate_limit=2.0)  # 2 seconds between requests
   ```

2. **Domain Restrictions**: Limit crawling to specific domains
   ```python
   source = LinkedDataSource(config, allowed_domains={"dbpedia.org"})
   ```

3. **Depth Limits**: Prevent infinite crawling
   ```python
   source = LinkedDataSource(config, max_depth=2)
   ```

4. **Error Handling**: Wrap in try-except for production use
   ```python
   try:
       for graph in source.fetch(uri):
           process(graph)
   except Exception as e:
       logger.error(f"Failed to fetch {uri}: {e}")
   ```

## Examples

See `examples/linked_data_example.py` for complete examples:

- Single URI fetching
- Recursive crawling
- Filtered crawling by predicates
- Domain-restricted crawling

## Limitations

- **No Authentication**: Currently doesn't support HTTP authentication
- **No Caching**: Each request is made fresh (consider adding caching)
- **Sequential**: Requests are made sequentially (not parallel)
- **Memory**: Large graphs are loaded into memory

## Future Enhancements

- Parallel fetching with thread pool
- Persistent caching of fetched graphs
- HTTP authentication support
- Streaming for very large graphs
- SPARQL endpoint fallback if HTTP fails

