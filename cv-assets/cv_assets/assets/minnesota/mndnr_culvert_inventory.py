from string import Template

from dagster import asset

from cv_assets.assets.minnesota.proj import crs
from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector import LocalVectorFileStorage, VectorFile
from cv_assets.utils import load_table_from_parquet, run_shell_cmd


@asset
def source_mndnr_culvert_inventory(
    vector_storage: LocalVectorFileStorage,
) -> VectorFile:
    """Download MnDNR Culvert Inventory Suite as an ESRI File Geodatabase.

    The File Geodatabase source is chosen because the Shapefile and GeoPackage
    sources have truncated column names."""

    output = vector_storage.get_file_by_filename(
        "source_mndnr_culvert_inventory.gdb.zip"
    )

    cmd = Template("curl --create-dirs --output $output $url")

    run_shell_cmd(
        cmd=cmd,
        output=output.path,
        url="https://resources.gisdata.mn.gov/pub/gdrs/data/pub/us_mn_state_dnr/struc_culvert_inventory_pub/fgdb_struc_culvert_inventory_pub.zip",
    )

    return output


@asset
def raw_mndnr_stream_crossing_summary(
    vector_storage: LocalVectorFileStorage,
    source_mndnr_culvert_inventory: VectorFile,
) -> VectorFile:
    """Extract and reproject the Stream_Crossing_Summary layer from the MnDOT Culvert
    Inventory to parquet"""

    output = vector_storage.get_file_by_filename(
        "raw_mndnr_stream_crossing_summary.parquet"
    )

    cmd = Template(
        """
        ogr2ogr \
            -f Parquet \
            -t_srs $to_srs \
            -sql "SELECT * FROM Stream_Crossing_Summary" \
            $output $input
        """
    )

    run_shell_cmd(
        cmd=cmd,
        to_srs=f"EPSG:{crs.to_epsg()}",
        output=output.path,
        input=source_mndnr_culvert_inventory.path,
    )

    return output


@asset
def pg_raw_mndnr_stream_crossing_summary(
    raw_mndnr_stream_crossing_summary: VectorFile, postgis: PostGISResource
) -> PGTable:
    """Load Stream Crossing Summary Parquet into PostGIS"""
    output = PGTable(schema="minnesota", table="raw_mndnr_stream_crossing")

    load_table_from_parquet(
        input=raw_mndnr_stream_crossing_summary.path,
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output


@asset
def raw_mndnr_culvert_opening(
    vector_storage: LocalVectorFileStorage,
    source_mndnr_culvert_inventory: VectorFile,
) -> VectorFile:
    """Extract and reproject the Culvert_Opening layer from the MnDOT Culvert
    Inventory to parquet.

    The source layer is incompatible with the Parquet driver, so an intermediate
    GeoPackage is used as a compatible step."""

    temp_gpkg = vector_storage.get_file_by_filename("temp_mndnr_culvert_opening.gpkg")

    cmd = Template(
        """
        ogr2ogr \
            -f GPKG \
            -t_srs $to_srs \
            -sql "SELECT * FROM Culvert_Opening" \
            $output $input
        """
    )

    run_shell_cmd(
        cmd=cmd,
        to_srs=f"EPSG:{crs.to_epsg()}",
        output=temp_gpkg.path,
        input=source_mndnr_culvert_inventory.path,
    )

    output = vector_storage.get_file_by_filename("raw_mndnr_culvert_opening.parquet")

    cmd = Template("ogr2ogr -f Parquet $output $input")

    run_shell_cmd(
        cmd=cmd,
        output=output.path,
        input=temp_gpkg.path,
    )

    # Remove temporary GeoPackage
    temp_gpkg.path.unlink()

    return output


@asset
def pg_stg_mndnr_culvert_opening(
    raw_mndnr_culvert_opening: VectorFile, postgis: PostGISResource
) -> PGTable:
    """Load Stream Crossing Summary Parquet into PostGIS"""
    output = PGTable(schema="minnesota", table="raw_mndnr_culvert_opening")

    load_table_from_parquet(
        input=raw_mndnr_culvert_opening.path,
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output
