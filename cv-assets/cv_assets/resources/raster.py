from dataclasses import dataclass

from dagster import ConfigurableResource
from upath import UPath


@dataclass
class RasterFile:
    filename: str
    path: UPath


class LocalRasterFileStorage(ConfigurableResource):
    base_path: str

    @property
    def base_upath(self) -> UPath:
        return UPath(self.base_path)

    def _make_parent_directories(self) -> None:
        if not self.base_upath.exists():
            self.base_upath.mkdir(parents=True, exist_ok=True)

    def _get_file(self, filename: str) -> RasterFile:
        self._make_parent_directories()
        return RasterFile(filename, self.base_upath / filename)

    def get_file_by_filename(self, filename: str) -> RasterFile:
        return self._get_file(filename)
