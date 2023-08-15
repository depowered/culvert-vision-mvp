from string import Template

from dagster import asset

from cv_assets.assets.minnesota.proj import crs
from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector import LocalVectorFileStorage, VectorFile
from cv_assets.utils import load_table_from_parquet, run_shell_cmd


@asset
def source_mngio_1000m_tile_index(vector_storage: LocalVectorFileStorage) -> VectorFile:
    """Download MnGIO 1000m Tile Index as a GeoPackage."""

    output = vector_storage.get_file_by_filename(
        "source_mngio_1000m_tile_index.gpkg.zip"
    )

    cmd = Template("curl --create-dirs --output $output $url")

    run_shell_cmd(
        cmd=cmd,
        output=output.path,
        url="https://resources.gisdata.mn.gov/pub/gdrs/data/pub/us_mn_state_mngeo/loc_index_3dgeo_1000m_tilescheme/gpkg_loc_index_3dgeo_1000m_tilescheme.zip",
    )

    return output


@asset
def raw_mngio_1000m_tile_index(
    vector_storage: LocalVectorFileStorage,
    source_mngio_1000m_tile_index: VectorFile,
) -> VectorFile:
    """Extract and reproject the MnGIO 1000m Tile Index and write to Parquet."""

    output = vector_storage.get_file_by_filename("raw_mngio_1000m_tile_index.parquet")

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
        layer="MN3DGEO_1K_TILE_INDEX",
        output=output.path,
        input=source_mngio_1000m_tile_index.path,
    )

    return output


@asset
def pg_mngio_1000m_tile_index(
    raw_mngio_1000m_tile_index: VectorFile, postgis: PostGISResource
) -> PGTable:
    """Load MnGIO 1000m Tile Index Parquet into PostGIS"""
    output = PGTable(schema="minnesota", table="raw_mngio_1000m_tile_index")

    load_table_from_parquet(
        input=raw_mngio_1000m_tile_index.path,
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output
