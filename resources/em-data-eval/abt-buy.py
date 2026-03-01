from pydantic import BaseModel
from typing import List
from pyodibel.benchmark.entity_resolution import EntityResolutionBenchData


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

class AbtDataSource(DataSource):
    pass

class BuyDataSource(DataSource):
    pass

class AbtBuyEntityResolutionBenchData(EntityResolutionBenchData):
    abt_entities: List[AbtEntity]
    buy_entities: List[BuyEntity]
