from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import yaml

from .constants import SUBDOMAINS
from .ontology import load_ontology


def export_hierarchy_tree_by_subdomain(
    class_subdomains_yaml: Path,
    ontology_ttl: Path,
    output_tree_txt: Path,
) -> tuple[int, int]:
    with class_subdomains_yaml.open("r", encoding="utf-8") as handle:
        rows = yaml.safe_load(handle) or []

    class_to_domain: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_class = row.get("class")
        domain = row.get("primary_subdomain")
        if isinstance(raw_class, str) and raw_class.startswith("dbo:") and isinstance(domain, str):
            class_to_domain[raw_class.removeprefix("dbo:")] = domain

    _, parents, _ = load_ontology(ontology_ttl)
    children: dict[str, set[str]] = defaultdict(set)
    for cls, cls_parents in parents.items():
        for parent in cls_parents:
            children[parent].add(cls)

    domain_classes: dict[str, set[str]] = defaultdict(set)
    for cls, domain in class_to_domain.items():
        domain_classes[domain].add(cls)

    ordered_domains = [domain for domain in SUBDOMAINS if domain in domain_classes]
    ordered_domains.extend(sorted(domain for domain in domain_classes if domain not in SUBDOMAINS))

    lines: list[str] = []

    for domain in ordered_domains:
        cls_set = domain_classes[domain]
        if not cls_set:
            continue

        roots = sorted(
            cls for cls in cls_set if not any(parent in cls_set for parent in parents.get(cls, set()))
        )

        lines.append(f"{domain} ({len(cls_set)} classes)")
        seen: set[str] = set()

        def walk(node: str, depth: int, active: set[str]) -> None:
            lines.append(f"{'  ' * depth}- dbo:{node}")
            seen.add(node)
            next_active = set(active)
            next_active.add(node)
            for child in sorted(c for c in children.get(node, set()) if c in cls_set):
                if child in next_active:
                    lines.append(f"{'  ' * (depth + 1)}- dbo:{child} (cycle)")
                    continue
                walk(child, depth + 1, next_active)

        for root in roots:
            walk(root, 0, set())

        leftovers = sorted(cls_set - seen)
        if leftovers:
            lines.append("  [unreached/disconnected]")
            for cls in leftovers:
                lines.append(f"  - dbo:{cls}")
        lines.append("")

    output_tree_txt.parent.mkdir(parents=True, exist_ok=True)
    output_tree_txt.write_text("\n".join(lines), encoding="utf-8")
    return len(ordered_domains), len(class_to_domain)

