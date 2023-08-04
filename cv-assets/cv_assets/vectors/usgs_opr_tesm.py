from string import Template

from dagster import asset

from cv_assets.config import get_settings
from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector_file_asset import VectorFileAsset
from cv_assets.utils import run_shell_cmd
from cv_assets.vectors.load_pg_table import load_table_from_parquet
from cv_assets.vectors.usgs_wesm import workunit_ids  # noqa F411

settings = get_settings()
TARGET_EPSG = settings.target_epsg


@asset
def raw_usgs_opr_tesm() -> VectorFileAsset:
    """Download USGS Original Product Resolution (OPR)
    Tile Extent Spatial Metadata (TESM) GeoPackage from source"""

    output = VectorFileAsset("raw_usgs_opr_tesm.gpkg")

    # The file is large, avoid redownloading if it already exists
    if output.get_path().exists():
        return output

    cmd = Template("curl --create-dirs --output $output $url")

    run_shell_cmd(
        cmd=cmd,
        output=output.get_path(),
        url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/OPR/FullExtentSpatialMetadata/OPR_TESM.gpkg",
    )

    return output


@asset
def stg_usgs_opr_tesm(
    raw_usgs_opr_tesm: VectorFileAsset, workunit_ids: list[int]  # noqa F811
) -> VectorFileAsset:
    """Filter and reproject USGS OPR TESM GeoPackage to Parquet"""

    output = VectorFileAsset("stg_usgs_opr_tesm.parquet")

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
        output=output.get_path(),
        input=raw_usgs_opr_tesm.get_path(),
    )

    return output


@asset
def pg_stg_usgs_opr_tesm(
    stg_usgs_opr_tesm: VectorFileAsset, postgis: PostGISResource
) -> PGTable:
    """Load USGS OPR TESM Parquet into PostGIS table"""
    output = PGTable(schema="mn", table="usgs_opr_tiles")

    load_table_from_parquet(
        input=stg_usgs_opr_tesm.get_path(),
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output
