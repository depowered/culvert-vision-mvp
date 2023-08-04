from string import Template

from dagster import asset

from cv_assets.config import get_settings
from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector_file_asset import VectorFileAsset
from cv_assets.utils import run_shell_cmd
from cv_assets.vectors.load_pg_table import load_table_from_parquet

settings = get_settings()
TARGET_EPSG = settings.target_epsg


@asset
def raw_mndnr_watershed_suite() -> VectorFileAsset:
    """Download MnDNR Watershed Suite as a GeoPackage."""

    output = VectorFileAsset("raw_mndnr_watershed_suite.gpkg.zip")

    cmd = Template("curl --create-dirs --output $output $url")

    run_shell_cmd(
        cmd=cmd,
        output=output.get_path(),
        url="https://resources.gisdata.mn.gov/pub/gdrs/data/pub/us_mn_state_dnr/geos_dnr_watersheds/gpkg_geos_dnr_watersheds.zip",
    )

    return output


@asset
def stg_mndnr_level_8_catchments(
    raw_mndnr_watershed_suite: VectorFileAsset,
) -> VectorFileAsset:
    """Extract and reproject the Level 8 All Catchments layer from the MnDOT Watershed
    Suite and write to Parquet."""

    output = VectorFileAsset("stg_mndnr_level_8_catchments.parquet")

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
        output=output.get_path(),
        input=raw_mndnr_watershed_suite.get_path(),
    )

    return output


@asset
def pg_stg_mndnr_level_8_catchments(
    stg_mndnr_level_8_catchments: VectorFileAsset, postgis: PostGISResource
) -> PGTable:
    """Load MnDNR Level 8 Catchments Parquet into PostGIS table"""
    output = PGTable(schema="mn", table="mndnr_level_8_catchments")

    load_table_from_parquet(
        input=stg_mndnr_level_8_catchments.get_path(),
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output
