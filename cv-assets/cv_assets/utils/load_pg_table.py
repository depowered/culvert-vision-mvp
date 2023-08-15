import tempfile
from pathlib import Path
from string import Template

from cv_assets.utils.shell_cmd import run_shell_cmd


def create_pg_dump(output: Path, input: Path, schema: str, table: str) -> None:
    """Create a PGDump SQL file with ogr2ogr"""

    cmd = Template(
        """
        ogr2ogr --config PG_USE_COPY YES -f PGDump $output $input \
            -nln $table \
            -nlt PROMOTE_TO_MULTI \
            -lco GEOMETRY_NAME=geom \
            -lco DIM=2 \
            -lco SCHEMA=$schema \
            -lco CREATE_SCHEMA=ON \
            -lco DROP_TABLE=IF_EXISTS \
            -lco SPATIAL_INDEX=GIST
        """
    )

    run_shell_cmd(
        cmd=cmd,
        output=output,
        input=input,
        schema=schema,
        table=table,
    )


def load_table_from_pg_dump(input: Path, dsn: str) -> None:
    """Load data from PGDump into PostGIS"""

    cmd = Template("psql --file $input $dsn")

    run_shell_cmd(
        cmd=cmd,
        input=input,
        dsn=dsn,
    )


def load_table_from_parquet(input: Path, dsn: str, schema: str, table: str) -> None:
    """Create or replace PostGIS table with Parquet content"""
    with tempfile.TemporaryDirectory() as tempdir:
        pg_dump = Path(tempdir) / "pg_dump.sql"

        create_pg_dump(
            output=pg_dump,
            input=input,
            table=table,
            schema=schema,
        )

        load_table_from_pg_dump(
            input=pg_dump,
            dsn=dsn,
        )
