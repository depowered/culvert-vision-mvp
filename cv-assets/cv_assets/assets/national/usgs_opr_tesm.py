from string import Template

from dagster import asset

from cv_assets.assets.national.usgs_wesm import workunit_ids  # noqa F411
from cv_assets.resources.postgis import PGTable, PostGISResource
from cv_assets.resources.vector import LocalVectorFileStorage, VectorFile
from cv_assets.utils import load_table_from_parquet, run_shell_cmd


@asset
def source_usgs_opr_tesm(vector_storage: LocalVectorFileStorage) -> VectorFile:
    """Download USGS Original Product Resolution (OPR)
    Tile Extent Spatial Metadata (TESM) GeoPackage from source"""

    output = vector_storage.get_file_by_filename("source_usgs_opr_tesm.gpkg")

    # The file is large, avoid redownloading if it already exists
    if output.path.exists():
        return output

    cmd = Template("curl --create-dirs --output $output $url")

    run_shell_cmd(
        cmd=cmd,
        output=output.path,
        url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/OPR/FullExtentSpatialMetadata/OPR_TESM.gpkg",
    )

    return output


@asset
def raw_usgs_opr_tesm(
    vector_storage: LocalVectorFileStorage,
    source_usgs_opr_tesm: VectorFile,
    workunit_ids: list[int],  # noqa F811
) -> VectorFile:
    """Convert USGS OPR TESM source to Parquet. Keep only the tiles for workunits found
    in the workunit_ids list."""

    output = vector_storage.get_file_by_filename("raw_usgs_opr_tesm.parquet")

    cmd = Template(
        """
        ogr2ogr \
            -f Parquet \
            -sql "SELECT * FROM OPR_TILE_SMD WHERE workunit_id IN $workunit_ids" \
            $output $input
        """
    )

    run_shell_cmd(
        cmd=cmd,
        workunit_ids=tuple(workunit_ids),
        output=output.path,
        input=source_usgs_opr_tesm.path,
    )

    return output


# There is a problem with the source GeoPackage getting truncated at 1 million features.
# This means that we can't rely on the workunits of interest being present in the source.
# Do not load into PostGIS until this issue is resolved.
# When it is resolved, uncomment the @asset line to enable loading into PostGIS.
# @asset
def pg_raw_usgs_opr_tesm(
    raw_usgs_opr_tesm: VectorFile, postgis: PostGISResource
) -> PGTable:
    """Load USGS OPR TESM Parquet into PostGIS"""
    output = PGTable(schema="mn", table="usgs_opr_tiles")

    load_table_from_parquet(
        input=raw_usgs_opr_tesm.path,
        dsn=postgis.dsn,
        schema=output.schema,
        table=output.table,
    )

    return output
