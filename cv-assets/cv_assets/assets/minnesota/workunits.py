from string import Template

from dagster import asset

from cv_assets.assets.minnesota.proj import crs
from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector import LocalVectorFileStorage, VectorFile
from cv_assets.utils import load_table_from_parquet, run_shell_cmd


@asset
def pg_mn_workunits(
    postgis: PostGISResource,
    vector_storage: LocalVectorFileStorage,
    raw_usgs_wesm: VectorFile,
) -> PGTable:
    """Load Minnesota Workunits into PostGIS"""

    temp_workunits = vector_storage.get_file_by_filename("temp_workunits.parquet")

    cmd = Template(
        """
        ogr2ogr \
            -f Parquet \
            -t_srs $to_srs \
            -sql "SELECT * FROM $layer WHERE $where" \
            $output $input
        """
    )

    run_shell_cmd(
        cmd=cmd,
        to_srs=f"EPSG:{crs.to_epsg()}",
        layer="raw_usgs_wesm",
        where="workunit LIKE 'MN%'",
        output=temp_workunits.path,
        input=raw_usgs_wesm.path,
    )

    output = PGTable(schema="minnesota", table="raw_workunits")

    load_table_from_parquet(
        input=temp_workunits.path,
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    # Remove temp file
    temp_workunits.path.unlink()

    return output
