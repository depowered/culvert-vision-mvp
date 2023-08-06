from pathlib import Path

from cv_assets.config import get_settings

settings = get_settings()
STORAGE_DIR = settings.file_storage_dir.resolve() / "vector"


class VectorFileAsset:
    _base_path: Path = STORAGE_DIR

    def __init__(self, filename: str) -> None:
        self.filename = filename

    def _make_parent_directories(self) -> None:
        self._base_path.mkdir(parents=True, exist_ok=True)

    def get_path(self) -> Path:
        self._make_parent_directories()
        return self._base_path / self.filename
