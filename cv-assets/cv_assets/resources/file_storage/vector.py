from dataclasses import dataclass

from dagster import ConfigurableResource
from upath import UPath


@dataclass
class VectorFile:
    filename: str
    path: UPath


class LocalVectorFileStorage(ConfigurableResource):
    base_path: str

    @property
    def base_upath(self) -> UPath:
        return UPath(self.base_path)

    def _make_parent_directories(self) -> None:
        if not self.base_upath.exists():
            self.base_upath.mkdir(parents=True, exist_ok=True)

    def _get_file(self, filename: str) -> VectorFile:
        self._make_parent_directories()
        return VectorFile(filename, self.base_upath / filename)

    def get_file_by_filename(self, filename: str) -> VectorFile:
        return self._get_file(filename)

    def get_file_by_asset_key(self, asset_key: str) -> VectorFile:
        parts = asset_key.split("_")

        # Handle special cases like zipped geodatabases
        if parts[-2:] == ["gdb", "zip"]:
            extension = "gdb.zip"
            filename = f"{parts[:-2]}.{extension}"
            return self._get_file(filename)

        extension = parts[-1:]
        filename = f"{parts[:-1]}.{extension}"
        return self._get_file(filename)
