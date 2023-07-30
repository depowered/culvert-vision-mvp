from cv_assets.vectors import usgs_opr_tesm, usgs_wesm
from dagster import Definitions, load_assets_from_modules

vector_assets = load_assets_from_modules(
    [usgs_wesm, usgs_opr_tesm], group_name="vectors"
)

defs = Definitions(assets=vector_assets)
