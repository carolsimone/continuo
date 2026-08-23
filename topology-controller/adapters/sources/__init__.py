from abc import ABC, abstractmethod
from domain.model import ManifestFile


class ManifestSource(ABC):
    @abstractmethod
    def list_manifests(self) -> list[ManifestFile]: ...

    def cleanup(self) -> None:
        """No-op by default. S3Source overrides to clean up temp files."""
        pass
