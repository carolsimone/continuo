import csv
import os
from abc import ABC, abstractmethod
from domain.model import NodeRegistry, NodeRegistryEntry

_FIELDNAMES = ["table_name", "schema_name", "service_name", "owner"]


class RegistryRepository(ABC):
    @abstractmethod
    def save(self, registry: NodeRegistry) -> None: ...

    @abstractmethod
    def load(self) -> NodeRegistry: ...


class FilesystemRegistryRepository(RegistryRepository):
    def __init__(self, path: str) -> None:
        self._path = path

    def save(self, registry: NodeRegistry) -> None:
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        with open(self._path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=_FIELDNAMES)
            writer.writeheader()
            for e in registry.entries:
                writer.writerow({
                    "table_name": e.table_name,
                    "schema_name": e.schema_name,
                    "service_name": e.service_name,
                    "owner": e.owner,
                })

    def load(self) -> NodeRegistry:
        if not os.path.exists(self._path):
            return NodeRegistry(entries=[])
        with open(self._path, newline="") as f:
            reader = csv.DictReader(f)
            entries = [
                NodeRegistryEntry(
                    table_name=row["table_name"],
                    schema_name=row["schema_name"],
                    service_name=row["service_name"],
                    owner=row["owner"],
                )
                for row in reader
            ]
        return NodeRegistry(entries=entries)
