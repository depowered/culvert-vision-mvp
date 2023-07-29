import subprocess
from pathlib import Path

from dagster import asset, file_relative_path

from cv_assets import config


@asset
def usgs_wesm_gpkg() -> Path:
    """Download USGS Workunit Extent Spatial Metadata (WESM) GeoPackage from source"""
    url = "https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/metadata/WESM.gpkg"
    output = config.CV_ASSETS_DATA / "raw/WESM.gpkg"
    cmd = f"curl --create-dirs --output {output} {url}"
    subprocess.run(cmd.split())
    return output


@asset
def usgs_wesm_parquet(usgs_wesm_gpkg: Path) -> Path:
    """Filter and reproject USGS WESM GeoPackage to GeoParquet"""

    input = usgs_wesm_gpkg
    output = config.CV_ASSETS_DATA / "interim/usgs_wesm.parquet"
    to_srs = f"EPSG:{config.TO_EPSG}"
    select_statement = "SELECT * FROM WESM WHERE workunit LIKE 'MN%'"

    output.parent.mkdir(parents=True, exist_ok=True)
    script = Path(file_relative_path(__file__, "./raw_to_parquet.sh")).resolve()
    subprocess.run(
        ["bash", f"{script}", f"{output}", f"{input}", to_srs, select_statement]
    )

    return output
