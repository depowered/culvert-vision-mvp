from dagster import Definitions, load_assets_from_modules

from cv_assets.config import get_settings
from cv_assets.resources.file_storage.vector import LocalVectorFileStorage
from cv_assets.resources.postgis import PostGISResource
from cv_assets.vectors import (
    mndnr_culvert_inventory,
    mndnr_watershed_suite,
    usgs_opr_tesm,
    usgs_wesm,
)

vector_assets = load_assets_from_modules(
    [mndnr_culvert_inventory, mndnr_watershed_suite, usgs_opr_tesm, usgs_wesm],
    group_name="vectors",
)

settings = get_settings()
VECTOR_FILE_STORAGE = str(settings.file_storage_dir.resolve() / "vector")

defs = Definitions(
    assets=vector_assets,
    resources={
        "postgis": PostGISResource.from_config(settings),
        "vector_storage": LocalVectorFileStorage(base_path=VECTOR_FILE_STORAGE),
    },
)
