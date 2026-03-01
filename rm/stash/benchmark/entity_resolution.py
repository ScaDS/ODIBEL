from abc import ABC, abstractmethod


from kgcore.system import kg_tracked, KGTracker
from kgcore.system.publishing import set_kg, get_kg
from kgcore.api import KnowledgeGraph
from kgcore.backend.rdf.rdf_rdflib import RDFLibBackend
from kgcore.model.rdf.rdf_base import RDFBaseModel


"""Create a KnowledgeGraph instance for testing."""
backend = RDFLibBackend()
model = RDFBaseModel()
kg_instance = KnowledgeGraph(model=model, backend=backend)
set_kg(kg_instance)


@kg_tracked()
class BenchData(ABC):
    pass

@kg_tracked()
class BenchDataEval(ABC):
    pass

class EntityResolutionBenchData(BenchData):
    pass

class EntityResolutionBenchDataEval(BenchDataEval):


    def __init__(self, data: EntityResolutionBenchData):
        self.data = data


model: RDFBaseModel = get_kg().model

for triple in model.triples(backend=backend, s=None, p=None, o=None):
    print(triple)