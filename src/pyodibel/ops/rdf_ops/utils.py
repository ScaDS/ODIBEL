from typing import Any, Dict, List, Mapping, Optional, Set, Tuple, Union
from rdflib import Graph, URIRef, BNode, Literal
from rdflib.namespace import RDFS
from pyodibel.rdf_ops.construct import DirectMappingType

# TODO move to construct.rdf_json.py

JsonDict = Dict[str, Any]
Provenance = Dict[str, str]  # json_path -> source_uri_or_bnode

# class DirectMappingType(Enum):
#     LITERAL = "literal"
#     OBJECT  = "object"

def build_recursive_json(
    subject: Union[str, URIRef, BNode],
    graph: Graph,
    visited: Optional[Set[str]] = None,
    mappings: Optional[Mapping[str, "DirectMappingType"]] = None,
    trace: bool = False
) -> Tuple[Optional[JsonDict], Optional[Provenance]]:
    """
    Plain JSON from RDF, limited to predicates present in `mappings`.

    Mapping semantics
    - LITERAL: rdfs:label(s) for URIRef; lexical form for Literal; provenance points to the URI/BNode if applicable.
    - OBJECT : recurse for URIRef/BNode; provenance maps the *object value path* (e.g., `$.genre` or `$.starring[0]`) to that URI/BNode,
               and merges any child provenance under that path.
    """

    if visited is None:
        visited = set()

    def as_literal_values(obj) -> Tuple[List[str], Optional[str]]:
        """Values to emit for LITERAL mappings + source for provenance (if URI/BNode)."""
        if isinstance(obj, URIRef):
            labels = [str(lbl) for _, _, lbl in graph.triples((obj, RDFS.label, None))]
            return (labels or [str(obj)]), str(obj)
        if isinstance(obj, BNode):
            return [str(obj)], str(obj)
        if isinstance(obj, Literal):
            return [str(obj)], None
        return [str(obj)], None

    def process_value(pred_uri: URIRef, obj: Any) -> Tuple[List[Any], List[Optional[str]], List[Optional[Provenance]]]:
        """
        Returns parallel lists:
          - values:    JSON values (strings or nested dicts)
          - sources:   source URI/BNode for each value (or None)
          - childprov: child provenance dicts for nested OBJECT values (or None)
        """
        mt = mappings.get(str(pred_uri), None) if mappings else None

        if mt == DirectMappingType.LITERAL:
            vals, src = as_literal_values(obj)
            return vals, [src] * len(vals), [None] * len(vals)

        if mt == DirectMappingType.OBJECT:
            if isinstance(obj, (URIRef, BNode)):
                nested, child_prov = build_recursive_json(obj, graph, visited, mappings, trace)
                if nested not in (None, {}, ""):
                    # value is the nested object; source is the object's URI/BNode
                    return [nested], [str(obj)], [child_prov if trace else None]
                else:
                    # nested object has no mapped predicates, fall back to literal representation
                    vals, src = as_literal_values(obj)
                    return vals, [str(obj)] * len(vals), [None] * len(vals)
            # literal but asked as OBJECT -> fallback to string (no provenance)
            return [str(obj)], [None], [None]

        # not in mappings -> caller skips anyway, but return a safe default
        return [str(obj)], [str(obj)] if isinstance(obj, (URIRef, BNode)) else [None], [None]

    # -------- cycle guard --------
    sid = str(subject)
    if sid in visited:
        return None, ({} if trace else None)
    visited.add(sid)

    node = subject if isinstance(subject, (URIRef, BNode)) else URIRef(str(subject))

    result: JsonDict = {}
    prov: Provenance = {}

    # record the root subject provenance
    if trace:
        prov["$"] = str(node)

    # buckets to aggregate multi-values while keeping provenance aligned
    buckets: Dict[str, List[Any]] = {}
    bucket_srcs: Dict[str, List[Optional[str]]] = {}
    bucket_childprov: Dict[str, List[Optional[Provenance]]] = {}

    for _, p, o in graph.triples((node, None, None)):
        if mappings and str(p) not in mappings:
            continue

        ps = str(p)
        key = ps.split("#")[-1] if "#" in ps else ps.rstrip("/").split("/")[-1]

        values, srcs, childprovs = process_value(p, o)
        if not values:
            continue

        if key not in buckets:
            buckets[key], bucket_srcs[key], bucket_childprov[key] = [], [], []

        # dedupe by repr to keep stable order
        seen = {repr(v) for v in buckets[key]}
        for v, s, cp in zip(values, srcs, childprovs):
            if v in (None, {}, "", []):
                continue
            rv = repr(v)
            if rv not in seen:
                seen.add(rv)
                buckets[key].append(v)
                bucket_srcs[key].append(s)
                bucket_childprov[key].append(cp)

    # compact + final provenance (paths match the final shape)
    for k, vals in buckets.items():
        srcs = bucket_srcs[k]
        cps  = bucket_childprov[k]

        if len(vals) == 1:
            val = vals[0]
            result[k] = val
            value_path = f"$.{k}"  # singleton path
            if trace and srcs[0]:
                # map the value path itself to the originating URI (covers OBJECT and LITERAL-from-URI)
                prov[value_path] = srcs[0]
            if trace and isinstance(val, dict) and cps[0]:
                # merge nested provenance: rebase "$" to this value path
                for child_path, child_src in cps[0].items():
                    if child_path == "$":
                        # child root becomes the parent value path
                        prov[value_path] = child_src  # redundant with srcs[0], but harmless
                    elif child_path.startswith("$"):
                        prov[child_path.replace("$", value_path, 1)] = child_src
                    else:
                        prov[f"{value_path}{child_path}"] = child_src
        else:
            result[k] = vals
            if trace:
                for idx, (val, src, cp) in enumerate(zip(vals, srcs, cps)):
                    elem_path = f"$.{k}[{idx}]"
                    if src:
                        prov[elem_path] = src
                    if isinstance(val, dict) and cp:
                        for child_path, child_src in cp.items():
                            if child_path == "$":
                                prov[elem_path] = child_src  # value object’s own URI
                            elif child_path.startswith("$"):
                                prov[child_path.replace("$", elem_path, 1)] = child_src
                            else:
                                prov[f"{elem_path}{child_path}"] = child_src

    if not result:
        return None, ({} if trace else None)

    return result, (prov if trace else None)