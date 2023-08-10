from string import Template

from dagster import asset

from cv_assets.assets.minnesota.proj import crs
from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector import LocalVectorFileStorage, VectorFile
from cv_assets.utils import load_table_from_parquet, run_shell_cmd


@asset
def source_mndnr_watershed_suite(vector_storage: LocalVectorFileStorage) -> VectorFile:
    """Download MnDNR Watershed Suite as a GeoPackage."""

    output = vector_storage.get_file_by_filename(
        "source_mndnr_watershed_suite.gpkg.zip"
    )

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
def raw_mndnr_level_8_catchments(
    vector_storage: LocalVectorFileStorage,
    source_mndnr_watershed_suite: VectorFile,
) -> VectorFile:
    """Extract and reproject the Level 8 All Catchments layer from the MnDOT Watershed
    Suite and write to Parquet."""

    output = vector_storage.get_file_by_filename("raw_mndnr_level_8_catchments.parquet")

    cmd = Template(
        """
        ogr2ogr \
            -f Parquet \
            -t_srs $to_srs \
            -sql "SELECT * FROM $layer" \
            $output $input
        """
    )

    run_shell_cmd(
        cmd=cmd,
        to_srs=f"EPSG:{crs.to_epsg()}",
        layer="dnr_watersheds_dnr_level_08_all_catchments",
        output=output.path,
        input=source_mndnr_watershed_suite.path,
    )

    return output


@asset
def pg_raw_mndnr_level_8_catchments(
    raw_mndnr_level_8_catchments: VectorFile, postgis: PostGISResource
) -> PGTable:
    """Load MnDNR Level 8 Catchments Parquet into PostGIS"""
    output = PGTable(schema="minnesota", table="raw_mndnr_level_8_catchments")

    load_table_from_parquet(
        input=raw_mndnr_level_8_catchments.path,
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output


# dnr_watersheds_catchment_flow_lines
@asset
def raw_mndnr_catchment_flow_lines(
    vector_storage: LocalVectorFileStorage,
    source_mndnr_watershed_suite: VectorFile,
) -> VectorFile:
    """Extract and reproject the catchment flow lines layer from the MnDOT Watershed
    Suite and write to Parquet."""

    output = vector_storage.get_file_by_filename(
        "raw_mndnr_catchment_flow_lines.parquet"
    )

    cmd = Template(
        """
        ogr2ogr \
            -f Parquet \
            -t_srs $to_srs \
            -sql "SELECT * FROM $layer" \
            $output $input
        """
    )

    run_shell_cmd(
        cmd=cmd,
        to_srs=f"EPSG:{crs.to_epsg()}",
        layer="dnr_watersheds_catchment_flow_lines",
        output=output.path,
        input=source_mndnr_watershed_suite.path,
    )

    return output


@asset
def pg_raw_mndnr_catchment_flow_lines(
    raw_mndnr_catchment_flow_lines: VectorFile, postgis: PostGISResource
) -> PGTable:
    """Load MnDNR Level 8 Catchments Parquet into PostGIS"""
    output = PGTable(schema="minnesota", table="raw_mndnr_catchment_flow_lines")

    load_table_from_parquet(
        input=raw_mndnr_catchment_flow_lines.path,
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output
