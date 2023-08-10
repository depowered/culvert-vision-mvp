from string import Template

import pandas as pd
from dagster import asset

from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector import LocalVectorFileStorage, VectorFile
from cv_assets.utils import load_table_from_parquet, run_shell_cmd


@asset
def source_usgs_wesm(vector_storage: LocalVectorFileStorage) -> VectorFile:
    """Download USGS Workunit Extent Spatial Metadata (WESM) GeoPackage from source"""

    output = vector_storage.get_file_by_filename("source_usgs_wesm.gpkg")

    # The file is large, avoid redownloading if it already exists
    if output.path.exists():
        return output

    cmd = Template("curl --create-dirs --output $output $url")

    run_shell_cmd(
        cmd=cmd,
        output=output.path,
        url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/metadata/WESM.gpkg",
    )

    return output


@asset
def raw_usgs_wesm(
    vector_storage: LocalVectorFileStorage, source_usgs_wesm: VectorFile
) -> VectorFile:
    """Convert USGS WESM source to Parquet. The project requires high density surveys,
    so this step filters for workunits with Quality Level 0 or 1."""

    output = vector_storage.get_file_by_filename("raw_usgs_wesm.parquet")

    cmd = Template(
        """
        ogr2ogr \
            -f Parquet \
            -sql "SELECT * FROM WESM WHERE ql IN ('QL 0', 'QL 1')" \
            $output $input
        """
    )

    run_shell_cmd(
        cmd=cmd,
        output=output.path,
        input=source_usgs_wesm.path,
    )

    return output


@asset
def workunit_ids(raw_usgs_wesm: VectorFile) -> list[int]:
    """List of workunit_id in filtered USGS WESM to be used as the filtering
    criteria for USGS OPR TESM"""
    df = pd.read_parquet(path=raw_usgs_wesm.path, columns=["workunit_id"])
    return df["workunit_id"].to_list()


# There is far more data in the USGS WESM parquet than is necessary for any given study
# area. Instead of loading the whole dataset into PostGIS, state and workunit specific
# assets can read the relevant parts from the parquet.
# @asset
def pg_raw_usgs_wesm(raw_usgs_wesm: VectorFile, postgis: PostGISResource) -> PGTable:
    """Load USGS WESM Parquet into PostGIS"""
    output = PGTable(schema="national", table="raw_usgs_wesm")

    load_table_from_parquet(
        input=raw_usgs_wesm.path,
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output
