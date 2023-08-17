from string import Template

from dagster import asset

from cv_assets.assets.minnesota.proj import crs
from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector import LocalVectorFileStorage, VectorFile
from cv_assets.utils import load_table_from_parquet, run_shell_cmd


@asset
def source_goodhue_culverts(vector_storage: LocalVectorFileStorage) -> VectorFile:
    """Culvert lines for all of Goodhue County. Obtained by public data request from
    Goodhue County GIS Department."""

    return vector_storage.get_file_by_filename("source_goodhue_culvert_lines.shp.zip")


@asset
def raw_goodhue_culverts(
    vector_storage: LocalVectorFileStorage, source_goodhue_culverts: VectorFile
) -> VectorFile:
    """Reproject culvert lines to common CRS and write to parquet."""

    output = vector_storage.get_file_by_filename("raw_goodhue_culverts.parquet")

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
        layer="GoodhueCountyCulvertLines",
        output=output.path,
        input=source_goodhue_culverts.path,
    )

    return output


@asset
def pg_raw_goodhue_culverts(
    raw_goodhue_culverts: VectorFile, postgis: PostGISResource
) -> PGTable:
    """Load Goodhue Culverts parquet into PostGIS"""
    output = PGTable(schema="goodhue", table="raw_goodhue_culverts")

    load_table_from_parquet(
        input=raw_goodhue_culverts.path,
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output
