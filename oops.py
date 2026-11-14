from abc import ABC, abstractmethod

class Ingestor(ABC):
    @abstractmethod
    def read(self):
        pass

class OracleIngestor(Ingestor):
    def read(self):
        return "Reading Oracle data"

class APIIngestor(Ingestor):
    def read(self):
        return "Reading API data"

for ingestor in [OracleIngestor(), APIIngestor()]:
    print(ingestor.read())