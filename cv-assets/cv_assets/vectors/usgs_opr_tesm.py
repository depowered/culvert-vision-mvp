from string import Template

from dagster import asset

from cv_assets.config import get_settings
from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector import LocalVectorFileStorage, VectorFile
from cv_assets.utils import run_shell_cmd
from cv_assets.vectors.load_pg_table import load_table_from_parquet
from cv_assets.vectors.usgs_wesm import workunit_ids  # noqa F411

settings = get_settings()
TARGET_EPSG = settings.target_epsg


@asset
def raw_usgs_opr_tesm(vector_storage: LocalVectorFileStorage) -> VectorFile:
    """Download USGS Original Product Resolution (OPR)
    Tile Extent Spatial Metadata (TESM) GeoPackage from source"""

    output = vector_storage.get_file_by_filename("raw_usgs_opr_tesm.gpkg")

    # The file is large, avoid redownloading if it already exists
    if output.path.exists():
        return output

    cmd = Template("curl --create-dirs --output $output $url")

    run_shell_cmd(
        cmd=cmd,
        output=output.path,
        url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/OPR/FullExtentSpatialMetadata/OPR_TESM.gpkg",
    )

    return output


@asset
def stg_usgs_opr_tesm(
    vector_storage: LocalVectorFileStorage,
    raw_usgs_opr_tesm: VectorFile,
    workunit_ids: list[int],  # noqa F811
) -> VectorFile:
    """Filter and reproject USGS OPR TESM GeoPackage to Parquet"""

    output = vector_storage.get_file_by_filename("stg_usgs_opr_tesm.parquet")

    cmd = Template(
        """
        ogr2ogr \
            -f Parquet \
            -t_srs $to_srs \
            -sql "SELECT * FROM OPR_TILE_SMD WHERE workunit_id IN $workunit_ids" \
            $output $input
        """
    )

    run_shell_cmd(
        cmd=cmd,
        to_srs=f"EPSG:{TARGET_EPSG}",
        workunit_ids=tuple(workunit_ids),
        output=output.path,
        input=raw_usgs_opr_tesm.path,
    )

    return output


@asset
def pg_stg_usgs_opr_tesm(
    stg_usgs_opr_tesm: VectorFile, postgis: PostGISResource
) -> PGTable:
    """Load USGS OPR TESM Parquet into PostGIS table"""
    output = PGTable(schema="mn", table="usgs_opr_tiles")

    load_table_from_parquet(
        input=stg_usgs_opr_tesm.path,
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output
