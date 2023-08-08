from string import Template

from dagster import asset

from cv_assets.config import get_settings
from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector import LocalVectorFileStorage, VectorFile
from cv_assets.utils import load_table_from_parquet, run_shell_cmd

settings = get_settings()
TARGET_EPSG = settings.target_epsg


@asset
def raw_mndnr_watershed_suite(vector_storage: LocalVectorFileStorage) -> VectorFile:
    """Download MnDNR Watershed Suite as a GeoPackage."""

    output = vector_storage.get_file_by_filename("raw_mndnr_watershed_suite.gpkg.zip")

    # The file is large, avoid redownloading if it already exists
    if output.path.exists():
        return output

    cmd = Template("curl --create-dirs --output $output $url")

    run_shell_cmd(
        cmd=cmd,
        output=output.path,
        url="https://resources.gisdata.mn.gov/pub/gdrs/data/pub/us_mn_state_dnr/geos_dnr_watersheds/gpkg_geos_dnr_watersheds.zip",
    )

    return output


@asset
def stg_mndnr_level_8_catchments(
    vector_storage: LocalVectorFileStorage,
    raw_mndnr_watershed_suite: VectorFile,
) -> VectorFile:
    """Extract and reproject the Level 8 All Catchments layer from the MnDOT Watershed
    Suite and write to Parquet."""

    output = vector_storage.get_file_by_filename("stg_mndnr_level_8_catchments.parquet")

    cmd = Template(
        """
        ogr2ogr \
            -f Parquet \
            -t_srs $to_srs \
            -sql "SELECT * FROM dnr_watersheds_dnr_level_08_all_catchments" \
            $output $input
        """
    )

    run_shell_cmd(
        cmd=cmd,
        to_srs=f"EPSG:{TARGET_EPSG}",
        output=output.path,
        input=raw_mndnr_watershed_suite.path,
    )

    return output


@asset
def pg_stg_mndnr_level_8_catchments(
    stg_mndnr_level_8_catchments: VectorFile, postgis: PostGISResource
) -> PGTable:
    """Load MnDNR Level 8 Catchments Parquet into PostGIS table"""
    output = PGTable(schema="mn", table="mndnr_level_8_catchments")

    load_table_from_parquet(
        input=stg_mndnr_level_8_catchments.path,
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output
