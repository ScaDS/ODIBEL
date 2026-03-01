from typing import List

from .entity import Entity


class Cluster:


    def __init__(self, entities: List[Entity]=[], cluster_iri=None):
        self.entities = dict()
        for e in entities:
            self.entities[e.iri] = e
        self.cluster_iri = cluster_iri
        self.id = 0
        self.una_violations = -1
        self.number_of_records = len(self.entities)

    def remove(self, entity: Entity):
        self.entities.pop(entity.iri)

    def remove_by_iri(self, iri: str):
        self.entities.pop(iri)

    def __repr__(self):
        return str(self.id)+": "+str([str(k) for k in self.entities.keys()])