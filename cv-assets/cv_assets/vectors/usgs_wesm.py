import subprocess
from pathlib import Path
from string import Template

from dagster import asset

from cv_assets import config


@asset
def usgs_wesm_gpkg() -> Path:
    """Download USGS Workunit Extent Spatial Metadata (WESM) GeoPackage from source"""

    cmd = Template("curl --create-dirs --output $output $url")

    subprocess.run(
        args=cmd.substitute(
            output=config.CV_ASSETS_DATA / "raw/WESM.gpkg",
            url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/metadata/WESM.gpkg",
        ),
        shell=True,
        check=True,
    )

    return config.CV_ASSETS_DATA / "raw/WESM.gpkg"


@asset
def usgs_wesm_parquet(usgs_wesm_gpkg: Path) -> Path:
    """Filter and reproject USGS WESM GeoPackage to Parquet"""

    cmd = Template(
        """
        ogr2ogr \
            -f Parquet \
            -t_srs $to_srs \
            -sql "SELECT * FROM WESM WHERE workunit LIKE 'MN%'" \
            $output $input
        """
    )

    subprocess.run(
        args=cmd.substitute(
            input=usgs_wesm_gpkg,
            output=config.CV_ASSETS_DATA / "interim/usgs_wesm.parquet",
            to_srs=f"EPSG:{config.TO_EPSG}",
        ),
        shell=True,
        check=True,
    )

    return config.CV_ASSETS_DATA / "interim/usgs_wesm.parquet"
