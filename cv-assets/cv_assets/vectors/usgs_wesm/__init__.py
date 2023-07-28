import subprocess
from pathlib import Path

from dagster import asset

from cv_assets import config


@asset
def usgs_wesm_gpkg() -> Path:
    url = "https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/metadata/WESM.gpkg"
    output = config.CV_ASSETS_DATA / "raw/WESM.gpkg"
    cmd = f"curl --create-dirs --output {output} {url}"
    subprocess.run(cmd.split())
    return output
