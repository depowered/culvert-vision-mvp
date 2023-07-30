from pathlib import Path

from cv_assets.config import get_settings

settings = get_settings()
FILE_ASSET_STORAGE_DIR = settings.file_asset_storage_dir.resolve()


class FileAsset:
    _base_path: Path = FILE_ASSET_STORAGE_DIR

    def __init__(self, filename: str) -> None:
        self.filename = filename

    def _make_parent_directories(self) -> None:
        self._base_path.mkdir(parents=True, exist_ok=True)

    def get_path(self) -> Path:
        self._make_parent_directories()
        return self._base_path / self.filename


class VectorFileAsset(FileAsset):
    _base_path: Path = FILE_ASSET_STORAGE_DIR / "vector"
