import hashlib
import os
from rdflib import Graph
import logging


logger = logging.getLogger(__name__)

def hash_uri(uri: str) -> str:
    """Hash a URI to create a filename-safe string."""
    return hashlib.md5(uri.encode('utf-8')).hexdigest()


class FileHashStore:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def store(self, uri: str, graph: Graph) -> None:
        """Store data in the file hash store."""
        hash_value = hash_uri(uri)
        file_path = os.path.join(self.base_dir, hash_value+".nt")
        graph.serialize(destination=file_path, format='nt')

    def retrieve(self, uri: str) -> Graph:
        """Retrieve data from the file hash store."""
        hash_value = hash_uri(uri)
        file_path = os.path.join(self.base_dir, hash_value+"/data.nt")
        try:
            return Graph().parse(file_path, format='nt')
        except Exception as e:
            logger.error(f"File not found: {uri} {file_path} {e}")
            return Graph()

class FileHashStore2:
    """
    Without data.nt file
    """
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

    def store(self, uri: str, graph: Graph) -> None:
        """Store data in the file hash store."""
        hash_value = hash_uri(uri)
        file_path = os.path.join(self.base_dir, hash_value+".nt")
        graph.serialize(destination=file_path, format='nt')

    def retrieve(self, uri: str) -> Graph:
        """Retrieve data from the file hash store."""
        hash_value = hash_uri(uri)
        file_path = os.path.join(self.base_dir, hash_value+".nt")
        try:
            return Graph().parse(file_path, format='nt')
        except Exception as e:
            logger.error(f"File not found: {uri} {file_path} {e}")
            return Graph()

    def exists(self, uri: str) -> bool:
        """Check if the data exists in the file hash store."""
        hash_value = hash_uri(uri)
        file_path = os.path.join(self.base_dir, hash_value+".nt")
        return os.path.exists(file_path)