from string import Template

import pandas as pd
from dagster import asset

from cv_assets.config import get_settings
from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector_file_asset import VectorFileAsset
from cv_assets.utils import run_shell_cmd
from cv_assets.vectors.load_pg_table import load_table_from_parquet

settings = get_settings()
TARGET_EPSG = settings.target_epsg


@asset
def raw_usgs_wesm() -> VectorFileAsset:
    """Download USGS Workunit Extent Spatial Metadata (WESM) GeoPackage from source"""

    output = VectorFileAsset("raw_usgs_wesm.gpkg")

    # The file is large, avoid redownloading if it already exists
    if output.get_path().exists():
        return output

    cmd = Template("curl --create-dirs --output $output $url")

    run_shell_cmd(
        cmd=cmd,
        output=output.get_path(),
        url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/metadata/WESM.gpkg",
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

    run_shell_cmd(
        cmd=cmd,
        output=output.get_path(),
        input=raw_usgs_wesm.get_path(),
        to_srs=f"EPSG:{TARGET_EPSG}",
    )

    return output


@asset
def workunit_ids(stg_usgs_wesm: VectorFileAsset) -> list[int]:
    """List of workunit_id in filtered USGS WESM to be used as the filtering
    criteria for USGS OPR TESM"""
    df = pd.read_parquet(path=stg_usgs_wesm.get_path(), columns=["workunit_id"])
    return df["workunit_id"].to_list()


@asset
def pg_stg_usgs_wesm(
    stg_usgs_wesm: VectorFileAsset, postgis: PostGISResource
) -> PGTable:
    """Load USGS WESM Parquet into PostGIS table"""
    output = PGTable(schema="mn", table="usgs_workunits")

    load_table_from_parquet(
        input=stg_usgs_wesm.get_path(),
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output
