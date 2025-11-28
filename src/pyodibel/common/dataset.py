
from pydantic import BaseModel

class Dataset(BaseModel):
    name: str
    time: str


class GeneratorConfig():
    pass
