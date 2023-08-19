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


@asset
def pg_culvert_bounding_boxes(
    pg_raw_goodhue_culverts: PGTable, postgis: PostGISResource
) -> PGTable:
    """Culvert bounding boxes"""

    output = PGTable(schema="goodhue", table="culvert_bounding_boxes")

    # Create a bounding box around each culvert line. The bounding box of a line that
    # aligns with the x- or y-axis is a line. A similar problem exists for lines that
    # nearly align with an axis or very short lines which result in very narrow bounding
    # boxes.
    #
    # The query below ensures that the width (dx) and height (dy) of all of bounding
    # boxes created are at least a specified minimum value (MIN_SIDE_LENGTH).

    MIN_SIDE_LENGTH = 4.0

    query = """
        CREATE OR REPLACE VIEW {view} AS (

            WITH envelopes AS (
                SELECT
                    ogc_fid AS id,
                    ST_ENVELOPE(geom) AS geom
                FROM {source_table}
            ),

            envelopes_with_dx_dy AS (
                SELECT
                    id,
                    geom,
                    ST_XMAX(geom) - ST_XMIN(geom) AS dx,
                    ST_YMAX(geom) - ST_YMIN(geom) AS dy
                FROM envelopes
            ),

            meets_min_dx_dy AS (
                SELECT 
                    id, 
                    geom
                FROM envelopes_with_dx_dy
                WHERE 
                    dx >= {min_side_length} AND
                    dy >= {min_side_length}
            ),

            expand_dx AS (
                SELECT 
                    id,
                    ST_EXPAND(geom, ({min_side_length} - dx) / 2, 0.0) AS geom
                FROM envelopes_with_dx_dy
                WHERE
                    dx < {min_side_length} AND
                    dy >= {min_side_length}
            ),

            expand_dy AS (
                SELECT 
                    id,
                    ST_EXPAND(geom, 0.0, ({min_side_length} - dy) / 2) AS geom
                FROM envelopes_with_dx_dy
                WHERE
                    dx >= {min_side_length} AND
                    dy < {min_side_length}
            ),

            expand_dx_dy AS (
                SELECT 
                    id,
                    ST_EXPAND(
                        geom, 
                        ({min_side_length} - dx) / 2, 
                        ({min_side_length} - dy) / 2
                    ) AS geom
                FROM envelopes_with_dx_dy
                WHERE
                    dx < {min_side_length} AND
                    dy < {min_side_length}
            ),

            union_envelopes AS (
                SELECT * FROM meets_min_dx_dy
                UNION
                SELECT * FROM expand_dx
                UNION
                SELECT * FROM expand_dy
                UNION
                SELECT * FROM expand_dx_dy
            )

            SELECT * FROM union_envelopes
            ORDER BY id ASC
        )
    """.format(
        view=output.qualified_name,
        source_table=pg_raw_goodhue_culverts.qualified_name,
        min_side_length=MIN_SIDE_LENGTH,
    )

    postgis.execute_and_commit(query)

    return output
