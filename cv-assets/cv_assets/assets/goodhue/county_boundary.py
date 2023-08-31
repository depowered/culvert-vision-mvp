from string import Template

from dagster import asset

from cv_assets.resources.postgis import PGTable, PostGISResource


@asset
def pg_county_boundary(
    pg_mn_county_boundaries: PGTable, postgis: PostGISResource
) -> PGTable:
    """Goodhue County Boundary"""

    output = PGTable(schema="goodhue", table="county_boundary")

    query = Template(
        """
    CREATE OR REPLACE VIEW $view AS (
        SELECT 
            coun as county_number,
            geom,
            cty_name as county_name
        FROM $from_table
        WHERE cty_name = 'Goodhue'
    )"""
    ).substitute(
        view=output.qualified_name,
        from_table=pg_mn_county_boundaries.qualified_name,
    )

    postgis.execute_and_commit(query)

    return output
