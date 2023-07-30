import subprocess
from string import Template

import pandas as pd
from dagster import asset

from cv_assets.file_asset import VectorFileAsset
from cv_assets.settings import Settings

TARGET_EPSG = Settings().target_epsg


@asset
def raw_usgs_wesm() -> VectorFileAsset:
    """Download USGS Workunit Extent Spatial Metadata (WESM) GeoPackage from source"""

    output = VectorFileAsset("raw_usgs_wesm.gpkg")

    # The file is large, avoid redownloading if it already exists
    if output.get_path().exists():
        return output

    cmd = Template("curl --create-dirs --output $output $url")

    subprocess.run(
        args=cmd.substitute(
            output=output.get_path(),
            url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/metadata/WESM.gpkg",
        ),
        shell=True,  # Allows args to be passed as a string
        check=True,  # Prevents cmd from failing silently
    )

    return output


@asset
def stg_usgs_wesm(raw_usgs_wesm: VectorFileAsset) -> VectorFileAsset:
    """Filter and reproject USGS WESM GeoPackage to Parquet"""

    output = VectorFileAsset("stg_usgs_wesm.parquet")

    cmd = Template(
        """
        ogr2ogr \
            -f Parquet \
            -t_srs $to_srs \
            -sql "SELECT * FROM WESM WHERE workunit LIKE 'MN%' AND ql IN ('QL 0', 'QL 1')" \
            $output $input
        """
    )

    subprocess.run(
        args=cmd.substitute(
            output=output.get_path(),
            input=raw_usgs_wesm.get_path(),
            to_srs=f"EPSG:{TARGET_EPSG}",
        ),
        shell=True,  # Allows args to be passed as a string
        check=True,  # Prevents cmd from failing silently
    )

    return output


@asset
def workunit_ids(stg_usgs_wesm: VectorFileAsset) -> list[int]:
    """List of workunit_id in filtered USGS WESM to be used as the filtering
    criteria for USGS OPR TESM"""
    df = pd.read_parquet(path=stg_usgs_wesm.get_path(), columns=["workunit_id"])
    return df["workunit_id"].to_list()
