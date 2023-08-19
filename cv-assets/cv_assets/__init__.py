from dagster import Definitions, load_assets_from_modules

from cv_assets.assets.goodhue import county_boundary, goodhue_county_culvert_lines
from cv_assets.assets.minnesota import (
    mn_counties,
    mndnr_culvert_inventory,
    mndnr_watershed_suite,
    mngio_500m_tile_index,
    mngio_1000m_tile_index,
    workunits,
)
from cv_assets.assets.national import usgs_opr_tesm, usgs_wesm
from cv_assets.config import get_settings
from cv_assets.resources.postgis import PostGISResource
from cv_assets.resources.vector import LocalVectorFileStorage

national_assets = load_assets_from_modules(
    [usgs_opr_tesm, usgs_wesm],
    group_name="national",
)

minnesota_assets = load_assets_from_modules(
    [
        mn_counties,
        mndnr_culvert_inventory,
        mndnr_watershed_suite,
        mngio_1000m_tile_index,
        mngio_500m_tile_index,
        workunits,
    ],
    group_name="minnesota",
)

goodhue_assets = load_assets_from_modules(
    [county_boundary, goodhue_county_culvert_lines], group_name="goodhue"
)

settings = get_settings()
VECTOR_FILE_STORAGE = str(settings.file_storage_dir.resolve() / "vector")

defs = Definitions(
    assets=[*national_assets, *minnesota_assets, *goodhue_assets],
    resources={
        "postgis": PostGISResource.from_config(settings),
        "vector_storage": LocalVectorFileStorage(base_path=VECTOR_FILE_STORAGE),
    },
)
