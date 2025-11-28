"""
Linked Data source implementation.

Handles consuming and crawling RDF graphs using HTTP Linked Data requests
with content negotiation. Follows the Linked Data principles for accessing
RDF data from URIs via HTTP.
"""

from typing import Any, Iterator, Optional, Dict, Set, List
from pathlib import Path
import logging
import time
from urllib.parse import urlparse, urljoin
from collections import deque

import requests
from rdflib import Graph, URIRef, BNode, Literal
from rdflib.namespace import RDF

from pyodibel.core.data_source import DataSource, DataSourceConfig

logger = logging.getLogger(__name__)


class LinkedDataResponse:
    """Response from a Linked Data HTTP request."""
    
    def __init__(self, content: bytes, format: str, uri: str):
        self.content = content
        self.format = format
        self.uri = uri


class LinkedDataSource(DataSource):
    """
    Data source for consuming and crawling RDF graphs via HTTP Linked Data.
    
    This source implements the Linked Data principles:
    - Uses HTTP content negotiation to request RDF formats
    - Follows links in RDF graphs to crawl related resources
    - Supports various RDF serialization formats
    
    Features:
    - Single URI fetching
    - Recursive crawling with depth control
    - Link following (extract URIs from object positions)
    - Format detection and parsing
    - Rate limiting and retry logic
    """
    
    # RDF format preferences (weighted by popularity and efficiency)
    RDF_FORMATS = {
        "application/n-triples": 1.0,      # Most efficient for parsing
        "application/turtle": 0.8,         # Human-readable, compact
        "text/turtle": 0.8,
        "application/rdf+xml": 0.6,        # Standard XML format
        "application/ld+json": 0.4,        # JSON-LD
        "application/json": 0.2,           # Generic JSON (may contain JSON-LD)
    }
    
    def __init__(
        self,
        config: DataSourceConfig,
        max_depth: int = 3,
        follow_links: bool = True,
        rate_limit: float = 1.0,  # seconds between requests
        max_retries: int = 3,
        timeout: int = 10,
        allowed_domains: Optional[Set[str]] = None,
    ):
        """
        Initialize Linked Data source.
        
        Args:
            config: Data source configuration
            max_depth: Maximum depth for recursive crawling (0 = no recursion)
            follow_links: Whether to follow links found in RDF graphs
            rate_limit: Minimum seconds between HTTP requests
            max_retries: Maximum number of retries for failed requests
            timeout: HTTP request timeout in seconds
            allowed_domains: Set of allowed domains for crawling (None = all domains)
        """
        super().__init__(config)
        self.max_depth = max_depth
        self.follow_links = follow_links
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.timeout = timeout
        self.allowed_domains = allowed_domains
        
        self._visited: Set[str] = set()
        self._last_request_time: float = 0.0
        self._session = requests.Session()
    
    def fetch(self, uri: str, **kwargs) -> Iterator[Graph]:
        """
        Fetch RDF graph from a URI.
        
        Args:
            uri: URI to fetch
            **kwargs: Additional parameters:
                - max_depth: Override instance max_depth
                - follow_links: Override instance follow_links
                - format_preference: Override format preferences
        
        Yields:
            RDF graphs (one per URI fetched)
        """
        max_depth = kwargs.get("max_depth", self.max_depth)
        follow_links = kwargs.get("follow_links", self.follow_links)
        
        # Reset visited set for this fetch operation
        visited = set()
        
        # Use BFS for crawling
        queue = deque([(uri, 0)])  # (uri, depth)
        
        while queue:
            current_uri, depth = queue.popleft()
            
            # Skip if already visited or exceeds max depth
            if current_uri in visited or depth > max_depth:
                continue
            
            visited.add(current_uri)
            
            try:
                # Fetch the URI
                graph = self._fetch_uri(current_uri, **kwargs)
                
                if graph and len(graph) > 0:
                    yield graph
                    
                    # Follow links if enabled and not at max depth
                    if follow_links and depth < max_depth:
                        linked_uris = self._extract_linked_uris(graph, current_uri)
                        for linked_uri in linked_uris:
                            if linked_uri not in visited:
                                # Check domain restrictions
                                if self._is_allowed_domain(linked_uri):
                                    queue.append((linked_uri, depth + 1))
            
            except Exception as e:
                logger.warning(f"Failed to fetch {current_uri}: {e}")
                continue
    
    def _fetch_uri(self, uri: str, **kwargs) -> Optional[Graph]:
        """
        Fetch a single URI and return as RDF graph.
        
        Args:
            uri: URI to fetch
            **kwargs: Additional parameters
        
        Returns:
            RDF graph or None if fetch failed
        """
        # Rate limiting
        self._enforce_rate_limit()
        
        # Try different RDF formats
        format_preference = kwargs.get("format_preference", self.RDF_FORMATS)
        formats = sorted(format_preference.items(), key=lambda x: x[1], reverse=True)
        
        last_error = None
        for format_type, _ in formats:
            try:
                response = self._http_request(uri, format_type)
                if response:
                    graph = self._parse_rdf(response)
                    if graph and len(graph) > 0:
                        logger.debug(f"Successfully fetched {uri} as {format_type}")
                        return graph
            except Exception as e:
                last_error = e
                logger.debug(f"Failed to fetch {uri} as {format_type}: {e}")
                continue
        
        if last_error:
            raise last_error
        return None
    
    def _http_request(self, uri: str, accept_format: str) -> Optional[LinkedDataResponse]:
        """
        Make HTTP request with content negotiation.
        
        Args:
            uri: URI to request
            accept_format: Accept header value
        
        Returns:
            LinkedDataResponse or None
        """
        headers = {
            "Accept": accept_format,
            "User-Agent": "PyODIBEL/1.0 (Linked Data Client)",
        }
        
        for attempt in range(self.max_retries):
            try:
                response = self._session.get(
                    uri,
                    headers=headers,
                    timeout=self.timeout,
                    allow_redirects=True
                )
                
                if response.status_code == 200:
                    content_type = response.headers.get("Content-Type", "")
                    # Extract base content type (remove charset, etc.)
                    base_type = content_type.split(";")[0].strip()
                    
                    return LinkedDataResponse(
                        content=response.content,
                        format=base_type,
                        uri=uri
                    )
                elif response.status_code == 303 or response.status_code == 307:
                    # Follow redirects
                    location = response.headers.get("Location")
                    if location:
                        absolute_location = urljoin(uri, location)
                        logger.debug(f"Following redirect: {uri} -> {absolute_location}")
                        return self._http_request(absolute_location, accept_format)
                else:
                    logger.warning(f"HTTP {response.status_code} for {uri}")
            
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.debug(f"Retry {attempt + 1}/{self.max_retries} for {uri} after {wait_time}s")
                    time.sleep(wait_time)
                else:
                    raise
        
        return None
    
    def _parse_rdf(self, response: LinkedDataResponse) -> Optional[Graph]:
        """
        Parse RDF from response.
        
        Args:
            response: LinkedDataResponse
        
        Returns:
            RDF graph or None
        """
        try:
            graph = Graph()
            
            # Determine format from content type
            content_type = response.format.lower()
            
            if "n-triples" in content_type or "ntriples" in content_type:
                format = "nt"
            elif "turtle" in content_type or "ttl" in content_type:
                format = "turtle"
            elif "rdf+xml" in content_type or "xml" in content_type:
                format = "xml"
            elif "json" in content_type or "ld+json" in content_type:
                format = "json-ld"
            else:
                # Try to auto-detect
                format = None
            
            if format:
                graph.parse(data=response.content, format=format)
            else:
                # Try common formats
                for fmt in ["nt", "turtle", "xml", "json-ld"]:
                    try:
                        graph.parse(data=response.content, format=fmt)
                        break
                    except:
                        continue
            
            return graph
        
        except Exception as e:
            logger.error(f"Failed to parse RDF from {response.uri}: {e}")
            return None
    
    def _extract_linked_uris(self, graph: Graph, base_uri: str) -> Set[str]:
        """
        Extract URIs from RDF graph that can be followed.
        
        Args:
            graph: RDF graph
            base_uri: Base URI for resolving relative URIs
        
        Returns:
            Set of URIs found in object positions
        """
        uris = set()
        
        for s, p, o in graph:
            # Extract URIs from object position
            if isinstance(o, URIRef):
                uri = str(o)
                # Resolve relative URIs
                if not uri.startswith(("http://", "https://")):
                    uri = urljoin(base_uri, uri)
                uris.add(uri)
            
            # Also check subject (for sameAs links, etc.)
            if isinstance(s, URIRef):
                uri = str(s)
                if not uri.startswith(("http://", "https://")):
                    uri = urljoin(base_uri, uri)
                # Only add if different from base
                if uri != base_uri:
                    uris.add(uri)
        
        return uris
    
    def _is_allowed_domain(self, uri: str) -> bool:
        """Check if URI is in allowed domains."""
        if self.allowed_domains is None:
            return True
        
        parsed = urlparse(uri)
        domain = parsed.netloc
        return domain in self.allowed_domains
    
    def _enforce_rate_limit(self):
        """Enforce rate limiting between requests."""
        current_time = time.time()
        elapsed = current_time - self._last_request_time
        if elapsed < self.rate_limit:
            sleep_time = self.rate_limit - elapsed
            time.sleep(sleep_time)
        self._last_request_time = time.time()
    
    def supports_format(self, format: str) -> bool:
        """
        Check if the source supports a specific format.
        
        Args:
            format: Format identifier
        
        Returns:
            True if RDF format is supported
        """
        rdf_formats = ["rdf", "rdf+xml", "turtle", "ttl", "nt", "ntriples", "json-ld", "ld+json"]
        return format.lower() in rdf_formats
    
    def crawl(
        self,
        seed_uris: List[str],
        max_depth: Optional[int] = None,
        follow_predicates: Optional[List[str]] = None,
        **kwargs
    ) -> Iterator[Graph]:
        """
        Crawl Linked Data starting from seed URIs.
        
        Args:
            seed_uris: List of starting URIs
            max_depth: Maximum crawl depth (overrides instance setting)
            follow_predicates: Only follow links with these predicates (None = all)
            **kwargs: Additional parameters
        
        Yields:
            RDF graphs discovered during crawling
        """
        max_depth = max_depth if max_depth is not None else self.max_depth
        visited = set()
        queue = deque([(uri, 0) for uri in seed_uris])
        
        while queue:
            current_uri, depth = queue.popleft()
            
            if current_uri in visited or depth > max_depth:
                continue
            
            visited.add(current_uri)
            
            try:
                graph = self._fetch_uri(current_uri, **kwargs)
                
                if graph and len(graph) > 0:
                    yield graph
                    
                    if depth < max_depth:
                        linked_uris = self._extract_linked_uris(graph, current_uri)
                        
                        # Filter by predicates if specified
                        if follow_predicates:
                            linked_uris = self._filter_by_predicates(
                                graph, linked_uris, follow_predicates
                            )
                        
                        for linked_uri in linked_uris:
                            if linked_uri not in visited and self._is_allowed_domain(linked_uri):
                                queue.append((linked_uri, depth + 1))
            
            except Exception as e:
                logger.warning(f"Failed to crawl {current_uri}: {e}")
                continue
    
    def _filter_by_predicates(
        self, graph: Graph, uris: Set[str], predicates: List[str]
    ) -> Set[str]:
        """Filter URIs to only those connected by specified predicates."""
        filtered = set()
        predicate_uris = {URIRef(p) for p in predicates}
        
        for uri in uris:
            uri_ref = URIRef(uri)
            # Check if URI appears as object with any of the specified predicates
            for s, p, o in graph:
                if p in predicate_uris and o == uri_ref:
                    filtered.add(uri)
                    break
        
        return filtered
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata about the data source."""
        metadata = super().get_metadata()
        metadata.update({
            "type": "linked_data",
            "max_depth": self.max_depth,
            "follow_links": self.follow_links,
            "rate_limit": self.rate_limit,
            "supported_formats": list(self.RDF_FORMATS.keys()),
        })
        return metadata

