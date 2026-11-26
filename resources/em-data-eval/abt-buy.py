from typing import List

from pydantic import BaseModel

from pyodibel.api.source import Source, SourceConfig


# Abt CSV
# "id","name","description","price"

class AbtEntity(BaseModel):
    id: str
    name: str
    description: str
    price: float

# Buy CSV
# "id","name","description","manufacturer","price"

class BuyEntity(BaseModel):
    id: str
    name: str
    description: str
    manufacturer: str
    price: float


class AbtDataSource(Source):
    def __init__(self, location: str):
        super().__init__(SourceConfig(name="abt", source_type="csv", location=location))

    def read_entities(self):
        raise NotImplementedError("Abt CSV loading is not implemented")

    def read_raw(self):
        raise NotImplementedError("Abt CSV loading is not implemented")

    def get_schema(self):
        return {"columns": ["id", "name", "description", "price"]}

    def validate(self) -> bool:
        return False


class BuyDataSource(Source):
    def __init__(self, location: str):
        super().__init__(SourceConfig(name="buy", source_type="csv", location=location))

    def read_entities(self):
        raise NotImplementedError("Buy CSV loading is not implemented")

    def read_raw(self):
        raise NotImplementedError("Buy CSV loading is not implemented")

    def get_schema(self):
        return {"columns": ["id", "name", "description", "manufacturer", "price"]}

    def validate(self) -> bool:
        return False


class AbtBuyEntityResolutionBenchData(BaseModel):
    """Abt and Buy records. Pair labels belong in EntityResolutionData."""

    abt_entities: List[AbtEntity]
    buy_entities: List[BuyEntity]
