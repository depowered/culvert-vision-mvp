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

    def get_file_by_asset_name(self, asset_key: str) -> VectorFile:
        filename = get_filename_by_asset_name(asset_key)
        return self._get_file(filename)


def get_filename_by_asset_name(asset_key: str) -> str:
    """
    Returns the filename associated with the given asset key. The last part(s) of the
    asset key are assumed to be the file extension.

    Parameters:
        asset_key (str): The asset key to retrieve the filename for.

    Returns:
        str: The filename associated with the asset key.

    Example:
        >>> get_filename_by_asset_key("usgs_wesm_gpkg")
        "usgs_wesm.gpkg"
        >>> get_filename_by_asset_key("usgs_wesm_gdb_zip")
        "usgs_wesm.gdb.zip"
    """
    parts = asset_key.split("_")

    if parts[-2:] == ["gdb", "zip"]:
        filename = "_".join(parts[:-2]) + ".gdb.zip"
    else:
        filename = "_".join(parts[:-1]) + "." + parts[-1]

    return filename
