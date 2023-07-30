from dagster import Definitions, load_assets_from_modules

from cv_assets.vectors import mndnr_culvert_inventory, usgs_opr_tesm, usgs_wesm

vector_assets = load_assets_from_modules(
    [mndnr_culvert_inventory, usgs_opr_tesm, usgs_wesm], group_name="vectors"
)

defs = Definitions(assets=vector_assets)
