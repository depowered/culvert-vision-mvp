import subprocess
import tempfile
from pathlib import Path
from string import Template

import pandas as pd
from dagster import asset

from cv_assets.config import get_settings
from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector_file_asset import VectorFileAsset

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


@asset
def pg_stg_usgs_wesm(
    stg_usgs_wesm: VectorFileAsset, postgis: PostGISResource
) -> PGTable:
    """Load USGS WESM Parquet into PostGIS table"""
    output = PGTable(schema="public", table="stg_usgs_wesm")

    # Dump parquet content to PGDump
    dump_cmd = Template(
        """
        ogr2ogr --config PG_USE_COPY YES -f PGDump $output $input \
            -nln $table \
            -nlt PROMOTE_TO_MULTI \
            -lco GEOMETRY_NAME=geom \
            -lco DIM=2 \
            -lco SCHEMA=$schema \
            -lco CREATE_SCHEMA=ON \
            -lco DROP_TABLE=IF_EXISTS \
            -lco SPATIAL_INDEX=GIST
        """
    )

    # Load the PGDump into the database
    load_cmd = Template("psql --file $input $dsn")

    with tempfile.TemporaryDirectory() as tempdir:
        pg_dump = Path(tempdir) / "stg_usgs_wesm.sql"
        subprocess.run(
            args=dump_cmd.substitute(
                output=pg_dump,
                input=stg_usgs_wesm.get_path(),
                table=output.table,
                schema=output.schema,
            ),
            shell=True,  # Allows args to be passed as a string
            check=True,  # Prevents cmd from failing silently
        )

        subprocess.run(
            args=load_cmd.substitute(
                input=pg_dump,
                dsn=postgis.dsn,
            ),
            shell=True,  # Allows args to be passed as a string
            check=True,  # Prevents cmd from failing silently
        )

    return output
