from string import Template

from dagster import asset

from cv_assets.assets.minnesota.proj import crs
from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector import LocalVectorFileStorage, VectorFile
from cv_assets.utils import load_table_from_parquet, run_shell_cmd


@asset
def source_mn_county_boundaries(vector_storage: LocalVectorFileStorage) -> VectorFile:
    """Download Minnesota County Boundaries as a GeoPackage."""

    output = vector_storage.get_file_by_filename("source_mn_county_boundaries.gpkg.zip")

    cmd = Template("curl --create-dirs --output $output $url")

    run_shell_cmd(
        cmd=cmd,
        output=output.path,
        url="https://resources.gisdata.mn.gov/pub/gdrs/data/pub/us_mn_state_dnr/bdry_counties_in_minnesota/gpkg_bdry_counties_in_minnesota.zip",
    )

    return output


@asset
def raw_mn_county_boundaries(
    vector_storage: LocalVectorFileStorage,
    source_mn_county_boundaries: VectorFile,
) -> VectorFile:
    """Extract and reproject the Minnesota County Boundaries and write to Parquet."""

    output = vector_storage.get_file_by_filename("raw_mn_county_boundaries.parquet")

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
        layer="mn_county_boundaries",  # Extract the full resolution features
        output=output.path,
        input=source_mn_county_boundaries.path,
    )

    return output


@asset
def pg_mn_county_boundaries(
    raw_mn_county_boundaries: VectorFile, postgis: PostGISResource
) -> PGTable:
    """Load Minnesota County Boundaries Parquet into PostGIS"""
    output = PGTable(schema="minnesota", table="raw_mn_county_boundaries")

    load_table_from_parquet(
        input=raw_mn_county_boundaries.path,
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output
