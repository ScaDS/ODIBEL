from abc import ABC, abstractmethod

class SparqlBenchData(ABC):
    """
    Abstract base class for SPARQL benchmark data.

    A SPARQL benchmark data is a dataset of SPARQL queries and their results.
    The queries are used to test the performance of the SPARQL endpoint.
    The results are used to evaluate the accuracy of the SPARQL endpoint.
    """
    pass