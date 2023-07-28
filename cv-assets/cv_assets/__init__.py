from dagster import Definitions, load_assets_from_modules

from cv_assets.vectors import usgs_wesm

vector_assets = load_assets_from_modules([usgs_wesm], group_name="vectors")

defs = Definitions(assets=vector_assets)
