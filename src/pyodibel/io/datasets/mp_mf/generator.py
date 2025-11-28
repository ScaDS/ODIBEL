from pyodibel.datasets.mp_mf.multipart_multisource import Dataset, SourceType, load_dataset
from pathlib import Path
from typing import List, Literal
import os

class DatasetGenerator:
    def __init__(self, dataset: Dataset, split_cnt: int, part_cnt: int, source_types: List[SourceType], kg_type: Literal["reference", "seed"], entites_list: List[str]):
        self.dataset = dataset
        self.split_cnt = split_cnt
        self.part_cnt = part_cnt
        self.source_types = source_types
        self.kg_type = kg_type
        self.create_dirs(dataset, split_cnt, part_cnt, source_types, kg_type, entites_list)

    @classmethod
    def create_dirs(cls, dataset: Dataset, split_cnt: int, part_cnt: int, source_types: List[SourceType], kg_type: Literal["reference", "seed"], entites_list: List[str]):
        for split_id in range(split_cnt):
            # touch index/entities.csv
            index_path = dataset.root / f"split_{split_id}" / "index" / "entities.csv"
            os.makedirs(index_path.parent, exist_ok=True)
            Path(index_path).touch()
            with open(index_path, "w") as f:
                f.write("entity_id\n")
                f.write("\n".join(entites_list))

            # touch split_id/rdf.0, split_id/text.0, split_id/json.0, split_id/kg_reference
            # touch split_id/rdf.1, split_id/text.1, split_id/json.1, split_id/kg_reference
            # touch split_id/rdf.2, split_id/text.2, split_id/json.2, split_id/kg_reference
            for part_id in range(part_cnt):
                root = dataset.root / f"split_{split_id}"
                for source_type in source_types:
                    path = root / f"{source_type}" / f"{part_id}"
                    Path(path).mkdir(parents=True, exist_ok=True)
                kg_path = root / f"kg_{kg_type}"
                Path(kg_path).mkdir(parents=True, exist_ok=True)

    def generate(self):
        pass


if __name__ == "__main__":
    ds = Dataset(root=Path("/d/tmp/"))
    dg = DatasetGenerator(ds, 2, 3, [SourceType.rdf, SourceType.text, SourceType.json], "reference", ["a", "b", "c"])

    print(ds.root.as_posix())


    ds2 = load_dataset(ds.root)
    print(ds2.splits["split_0"].index.entities_csv.as_posix())
    print(ds2.splits["split_0"].sources[SourceType.rdf].data.parts[0])

