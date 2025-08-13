from abc import ABC, abstractmethod

class RDFFlow(ABC):

    @abstractmethod
    def run(self):
        pass


class RDFConstructFlow(RDFFlow):

    def run(self):
        pass