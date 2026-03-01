from typing import Literal, List, Tuple, Any, Dict
from dataclasses import dataclass, field
from pathlib import Path
import pandas as pd
import json
import gzip
from abc import ABC, abstractmethod

# ============== Internal Representation ==============

@dataclass
class Entity:
    """Internal representation of an entity."""
    id: str
    source: str  # which dataset it came from
    attributes: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        return hash((self.id, self.source))
    
    def __eq__(self, other):
        return self.id == other.id and self.source == other.source


@dataclass
class MatchPair:
    """A matching pair of entities."""
    left: Entity
    right: Entity
    
    def as_tuple(self) -> Tuple[str, str]:
        return (self.left.id, self.right.id)


@dataclass 
class EntityCollection:
    """Collection of entities from one or more sources."""
    entities: Dict[Tuple[str, str], Entity] = field(default_factory=dict)  # (source, id) -> Entity
    
    def add(self, entity: Entity):
        self.entities[(entity.source, entity.id)] = entity
    
    def get(self, source: str, entity_id: str) -> Entity | None:
        return self.entities.get((source, entity_id))
    
    def by_source(self, source: str) -> List[Entity]:
        return [e for e in self.entities.values() if e.source == source]
    
    def __len__(self):
        return len(self.entities)


@dataclass
class MatchSet:
    """Collection of matching pairs."""
    pairs: List[MatchPair] = field(default_factory=list)
    
    def add(self, pair: MatchPair):
        self.pairs.append(pair)
    
    def __len__(self):
        return len(self.pairs)
    
    def __iter__(self):
        return iter(self.pairs)


# ============== Dataset Definitions ==============

EntityResolutionType = Literal[
    "knowledge_graph_alignment",
    "entity_resolution",
    "record_linkage",
    "deduplication",
    "binary_entity_resolution",
    "multi_source_entity_resolution",
]

EntityResolutionFormat = Literal[
    "separate_datasets",  # Traditional: Dataset A, Dataset B, Ground Truth mapping
    "pairwise_labeled",   # Pre-paired records with match/no-match labels
]

@dataclass
class Dataset:
    uri: str
    id_column: str = "id"
    
    def load(self) -> pd.DataFrame:
        path = self.uri.replace("file://", "")
        if path.endswith(".csv"):
            for enc in ["utf-8-sig", "utf-8", "latin-1"]:
                try:
                    return pd.read_csv(path, encoding=enc)
                except UnicodeDecodeError:
                    continue
            raise ValueError(f"Could not decode: {path}")
        elif path.endswith(".json"):
            return pd.read_json(path)
        raise ValueError(f"Unsupported format: {path}")

@dataclass
class GroundTruth:
    uri: str
    left_id_column: str = "id_left"
    right_id_column: str = "id_right"
    
    def load(self) -> pd.DataFrame:
        path = self.uri.replace("file://", "")
        return pd.read_csv(path)
    
    def get_matches(self) -> List[Tuple[str, str]]:
        df = self.load()
        return list(zip(df[self.left_id_column], df[self.right_id_column]))


class EntityResolutionBenchset(ABC):
    types: List[EntityResolutionType] = field(default_factory=list)
    format: EntityResolutionFormat = "separate_datasets"
    
    @abstractmethod
    def get_datasets(self) -> dict[str, Dataset]:
        """Return dict of named datasets, e.g. {'abt': Dataset(...), 'buy': Dataset(...)}"""
        pass
    
    @abstractmethod
    def get_ground_truth(self) -> GroundTruth:
        pass
    
    # Convenience for binary case
    def get_dataset_a(self) -> Dataset:
        return list(self.get_datasets().values())[0]
    
    def get_dataset_b(self) -> Dataset:
        return list(self.get_datasets().values())[1]
    
    def load_all(self) -> Tuple[dict[str, pd.DataFrame], List[Tuple[str, str]]]:
        """Load all datasets and ground truth matches."""
        datasets = {name: ds.load() for name, ds in self.get_datasets().items()}
        return datasets, self.get_ground_truth().get_matches()
    
    # ---- Internal representation loading ----
    
    def row_to_entity(self, source: str, row: pd.Series, id_column: str) -> Entity:
        """Convert a DataFrame row to an Entity. Override for custom mapping."""
        return Entity(
            id=str(row[id_column]),
            source=source,
            attributes=row.drop(id_column).to_dict()
        )
    
    def load_entities(self) -> EntityCollection:
        """Load all datasets as EntityCollection."""
        collection = EntityCollection()
        for name, dataset in self.get_datasets().items():
            df = dataset.load()
            for _, row in df.iterrows():
                entity = self.row_to_entity(name, row, dataset.id_column)
                collection.add(entity)
        return collection
    
    def load_matches(self, entities: EntityCollection | None = None) -> MatchSet:
        """Load ground truth as MatchSet. Optionally link to loaded entities."""
        gt = self.get_ground_truth()
        df = gt.load()
        sources = list(self.get_datasets().keys())
        
        match_set = MatchSet()
        for _, row in df.iterrows():
            left_id = str(row[gt.left_id_column])
            right_id = str(row[gt.right_id_column])
            
            if entities:
                left = entities.get(sources[0], left_id)
                right = entities.get(sources[1], right_id)
            else:
                left = Entity(id=left_id, source=sources[0])
                right = Entity(id=right_id, source=sources[1])
            
            if left and right:
                match_set.add(MatchPair(left=left, right=right))
        
        return match_set


# ============== Abt-Buy Entities ==============

@dataclass
class AbtEntity(Entity):
    """Entity from Abt dataset."""
    name: str = ""
    description: str = ""
    price: str = ""
    
    @classmethod
    def from_row(cls, row: pd.Series) -> "AbtEntity":
        return cls(
            id=str(row["id"]),
            source="abt",
            name=str(row.get("name", "")),
            description=str(row.get("description", "")),
            price=str(row.get("price", "")),
        )


@dataclass
class BuyEntity(Entity):
    """Entity from Buy dataset."""
    name: str = ""
    description: str = ""
    manufacturer: str = ""
    price: str = ""
    
    @classmethod
    def from_row(cls, row: pd.Series) -> "BuyEntity":
        return cls(
            id=str(row["id"]),
            source="buy",
            name=str(row.get("name", "")),
            description=str(row.get("description", "")),
            manufacturer=str(row.get("manufacturer", "")),
            price=str(row.get("price", "")),
        )


class AbtBuyBenchset(EntityResolutionBenchset):
    types = ["record_linkage", "binary_entity_resolution"]
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
    
    def get_datasets(self) -> dict[str, Dataset]:
        return {
            "abt": Dataset(uri=f"file://{self.base_path}/Abt.csv", id_column="id"),
            "buy": Dataset(uri=f"file://{self.base_path}/Buy.csv", id_column="id"),
        }
    
    def get_ground_truth(self) -> GroundTruth:
        return GroundTruth(
            uri=f"file://{self.base_path}/abt_buy_perfectMapping.csv",
            left_id_column="idAbt",
            right_id_column="idBuy"
        )
    
    def row_to_entity(self, source: str, row: pd.Series, id_column: str) -> Entity:
        if source == "abt":
            return AbtEntity.from_row(row)
        else:
            return BuyEntity.from_row(row)


# ============== DBLP-Scholar Entities ==============

@dataclass
class DblpEntity(Entity):
    """Entity from DBLP dataset."""
    title: str = ""
    authors: str = ""
    venue: str = ""
    year: str = ""
    
    @classmethod
    def from_row(cls, row: pd.Series) -> "DblpEntity":
        return cls(
            id=str(row["id"]),
            source="dblp",
            title=str(row.get("title", "")),
            authors=str(row.get("authors", "")),
            venue=str(row.get("venue", "")),
            year=str(row.get("year", "")),
        )


@dataclass
class ScholarEntity(Entity):
    """Entity from Scholar dataset."""
    title: str = ""
    authors: str = ""
    venue: str = ""
    year: str = ""
    
    @classmethod
    def from_row(cls, row: pd.Series) -> "ScholarEntity":
        return cls(
            id=str(row["id"]),
            source="scholar",
            title=str(row.get("title", "")),
            authors=str(row.get("authors", "")),
            venue=str(row.get("venue", "")),
            year=str(row.get("year", "")),
        )


class DblpScholarBenchset(EntityResolutionBenchset):
    types = ["record_linkage", "binary_entity_resolution"]
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
    
    def get_datasets(self) -> dict[str, Dataset]:
        return {
            "dblp": Dataset(uri=f"file://{self.base_path}/DBLP1.csv", id_column="id"),
            "scholar": Dataset(uri=f"file://{self.base_path}/Scholar.csv", id_column="id"),
        }
    
    def get_ground_truth(self) -> GroundTruth:
        return GroundTruth(
            uri=f"file://{self.base_path}/DBLP-Scholar_perfectMapping.csv",
            left_id_column="idDBLP",
            right_id_column="idScholar"
        )
    
    def row_to_entity(self, source: str, row: pd.Series, id_column: str) -> Entity:
        if source == "dblp":
            return DblpEntity.from_row(row)
        else:
            return ScholarEntity.from_row(row)


# ============== WDC Product Corpus Entities ==============

@dataclass
class WdcProductEntity(Entity):
    """Entity from WDC Large Product Corpus."""
    title: str = ""
    description: str = ""
    brand: str = ""
    category: str = ""
    price: str = ""
    identifiers: List[Dict] = field(default_factory=list)
    
    @classmethod
    def from_dict(cls, data: Dict, source: str) -> "WdcProductEntity":
        return cls(
            id=str(data.get("id", "")),
            source=source,
            title=str(data.get("title", "") or ""),
            description=str(data.get("description", "") or ""),
            brand=str(data.get("brand", "") or ""),
            category=str(data.get("category", "") or ""),
            price=str(data.get("price", "") or ""),
            identifiers=data.get("identifiers", []) or [],
        )


@dataclass
class WdcPairedRecord:
    """A paired record from WDC gold standard (already contains left/right)."""
    left: WdcProductEntity
    right: WdcProductEntity
    label: int  # 1 = match, 0 = no match
    pair_id: str = ""
    cluster_id_left: int = 0
    cluster_id_right: int = 0


class WdcProductBenchset(EntityResolutionBenchset):
    """WDC Large Product Web Corpus v2 benchset (pairwise format)."""
    types = ["binary_entity_resolution", "record_linkage"]
    format = "pairwise_labeled"
    CATEGORIES = ["all", "cameras", "computers", "shoes", "watches"]
    
    def __init__(self, base_path: str, category: str = "all"):
        self.base_path = Path(base_path)
        if category not in self.CATEGORIES:
            raise ValueError(f"Category must be one of {self.CATEGORIES}")
        self.category = category
    
    def get_datasets(self) -> dict[str, Dataset]:
        # WDC doesn't have separate A/B files - entities are embedded in gold standard
        return {}
    
    def get_ground_truth(self) -> GroundTruth:
        return GroundTruth(
            uri=f"file://{self.base_path}/goldstandards/{self.category}_gs.json.gz"
        )
    
    def _load_gold_standard(self) -> List[Dict]:
        """Load the gzipped JSON gold standard."""
        gs_path = self.base_path / "goldstandards" / f"{self.category}_gs.json.gz"
        records = []
        with gzip.open(gs_path, "rt", encoding="utf-8") as f:
            for line in f:
                records.append(json.loads(line))
        return records
    
    def load_paired_records(self) -> List[WdcPairedRecord]:
        """Load gold standard as paired records with labels."""
        records = self._load_gold_standard()
        pairs = []
        for r in records:
            left = WdcProductEntity.from_dict({
                "id": r.get("id_left"),
                "title": r.get("title_left"),
                "description": r.get("description_left"),
                "brand": r.get("brand_left"),
                "category": r.get("category_left"),
                "price": r.get("price_left"),
                "identifiers": r.get("identifiers_left"),
            }, source="left")
            
            right = WdcProductEntity.from_dict({
                "id": r.get("id_right"),
                "title": r.get("title_right"),
                "description": r.get("description_right"),
                "brand": r.get("brand_right"),
                "category": r.get("category_right"),
                "price": r.get("price_right"),
                "identifiers": r.get("identifiers_right"),
            }, source="right")
            
            pairs.append(WdcPairedRecord(
                left=left,
                right=right,
                label=int(r.get("label", 0)),
                pair_id=r.get("pair_id", ""),
                cluster_id_left=r.get("cluster_id_left", 0),
                cluster_id_right=r.get("cluster_id_right", 0),
            ))
        return pairs
    
    def load_entities(self) -> EntityCollection:
        """Extract unique entities from paired records."""
        collection = EntityCollection()
        for pair in self.load_paired_records():
            collection.add(pair.left)
            collection.add(pair.right)
        return collection
    
    def load_matches(self, entities: EntityCollection | None = None) -> MatchSet:
        """Load only positive matches (label=1)."""
        match_set = MatchSet()
        for pair in self.load_paired_records():
            if pair.label == 1:
                match_set.add(MatchPair(left=pair.left, right=pair.right))
        return match_set


class WdcProductCorpusBenchset(EntityResolutionBenchset):
    """WDC Product Corpus with separate entity corpus + gold standard (traditional format)."""
    types = ["binary_entity_resolution", "record_linkage"]
    format = "separate_datasets"
    CATEGORIES = ["all", "cameras", "computers", "shoes", "watches"]
    
    def __init__(self, base_path: str, category: str = "all"):
        self.base_path = Path(base_path)
        if category not in self.CATEGORIES:
            raise ValueError(f"Category must be one of {self.CATEGORIES}")
        self.category = category
        self._entity_index: Dict[int, WdcProductEntity] | None = None
    
    def get_datasets(self) -> dict[str, Dataset]:
        # Single corpus, entities identified by cluster_id
        return {
            "corpus": Dataset(
                uri=f"file://{self.base_path}/offers_corpus_english_v2.json.gz",
                id_column="id"
            )
        }
    
    def get_ground_truth(self) -> GroundTruth:
        return GroundTruth(
            uri=f"file://{self.base_path}/goldstandards/{self.category}_gs.json.gz"
        )
    
    def _load_corpus(self) -> Dict[int, WdcProductEntity]:
        """Load and index the offers corpus by id."""
        if self._entity_index is not None:
            return self._entity_index
        
        corpus_path = self.base_path / "offers_corpus_english_v2.json.gz"
        self._entity_index = {}
        with gzip.open(corpus_path, "rt", encoding="utf-8") as f:
            for line in f:
                data = json.loads(line)
                entity = WdcProductEntity.from_dict(data, source="corpus")
                self._entity_index[int(data["id"])] = entity
        return self._entity_index
    
    def load_entities(self) -> EntityCollection:
        """Load all entities from corpus."""
        collection = EntityCollection()
        for entity in self._load_corpus().values():
            collection.add(entity)
        return collection
    
    def load_matches(self, entities: EntityCollection | None = None) -> MatchSet:
        """Load positive matches from gold standard, linking to corpus entities."""
        corpus = self._load_corpus()
        gs_path = self.base_path / "goldstandards" / f"{self.category}_gs.json.gz"
        
        match_set = MatchSet()
        with gzip.open(gs_path, "rt", encoding="utf-8") as f:
            for line in f:
                r = json.loads(line)
                if int(r.get("label", 0)) == 1:
                    left_id = int(r["id_left"])
                    right_id = int(r["id_right"])
                    left = corpus.get(left_id)
                    right = corpus.get(right_id)
                    if left and right:
                        match_set.add(MatchPair(left=left, right=right))
        return match_set


if __name__ == "__main__":
    # Abt-Buy example
    benchset = AbtBuyBenchset("/d/tmp/er/Abt-Buy")
    
    entities = benchset.load_entities()
    print(f"Total entities: {len(entities)}")
    print(f"Abt entities: {len(entities.by_source('abt'))}")
    print(f"Buy entities: {len(entities.by_source('buy'))}")
    
    matches = benchset.load_matches(entities)
    print(f"Ground truth pairs: {len(matches)}")
    
    # Show sample matches with typed entities
    for pair in list(matches)[:3]:
        abt: AbtEntity = pair.left
        buy: BuyEntity = pair.right
        print(f"  {abt.name[:40]}... <-> {buy.name[:40]}...")
    
    # DBLP-Scholar example
    print("\n--- DBLP-Scholar ---")
    dblp = DblpScholarBenchset("/d/tmp/er/DBLP-Scholar")
    entities2 = dblp.load_entities()
    print(f"Total entities: {len(entities2)}")
    matches2 = dblp.load_matches(entities2)
    print(f"Ground truth pairs: {len(matches2)}")
    
    # Show sample DBLP matches
    for pair in list(matches2)[:3]:
        d: DblpEntity = pair.left
        s: ScholarEntity = pair.right
        print(f"  {d.title[:40]}... <-> {s.title[:40]}...")
    
    # WDC Product Corpus - pairwise format
    print("\n--- WDC Product (pairwise format) ---")
    wdc = WdcProductBenchset("/d/tmp/er/largeproductwebcorpus/v2", category="computers")
    print(f"Format: {wdc.format}")
    paired = wdc.load_paired_records()
    print(f"Total pairs: {len(paired)}")
    print(f"Positive: {sum(1 for p in paired if p.label == 1)}, Negative: {sum(1 for p in paired if p.label == 0)}")
    
    # WDC Product Corpus - separate datasets format (uses full corpus)
    print("\n--- WDC Product (separate datasets format) ---")
    wdc2 = WdcProductCorpusBenchset("/d/tmp/er/largeproductwebcorpus/v2", category="computers")
    print(f"Format: {wdc2.format}")
    print("Loading corpus (this may take a moment)...")
    entities3 = wdc2.load_entities()
    print(f"Total entities in corpus: {len(entities3)}")
    matches3 = wdc2.load_matches()
    print(f"Positive matches: {len(matches3)}")




