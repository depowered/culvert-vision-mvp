import subprocess
from string import Template

from dagster import asset

from cv_assets.config import get_settings
from cv_assets.resources.file_asset import VectorFileAsset
from cv_assets.vectors.usgs_wesm import workunit_ids  # noqa F411

settings = get_settings()
TARGET_EPSG = settings.target_epsg


@asset
def raw_usgs_opr_tesm() -> VectorFileAsset:
    """Download USGS Original Product Resolution (OPR)
    Tile Extent Spatial Metadata (TESM) GeoPackage from source"""

    output = VectorFileAsset("raw_usgs_opr_tesm.gpkg")

    # The file is large, avoid redownloading if it already exists
    if output.get_path().exists():
        return output

    cmd = Template("curl --create-dirs --output $output $url")

    subprocess.run(
        args=cmd.substitute(
            output=output.get_path(),
            url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/OPR/FullExtentSpatialMetadata/OPR_TESM.gpkg",
        ),
        shell=True,  # Allows args to be passed as a string
        check=True,  # Prevents cmd from failing silently
    )

    return output


@asset
def stg_usgs_opr_tesm(
    raw_usgs_opr_tesm: VectorFileAsset, workunit_ids: list[int]  # noqa F811
) -> VectorFileAsset:
    """Filter and reproject USGS OPR TESM GeoPackage to Parquet"""

    output = VectorFileAsset("stg_usgs_opr_tesm.parquet")

    cmd = Template(
        """
        ogr2ogr \
            -f Parquet \
            -t_srs $to_srs \
            -sql "SELECT * FROM OPR_TILE_SMD WHERE workunit_id IN $workunit_ids" \
            $output $input
        """
    )

    subprocess.run(
        args=cmd.substitute(
            to_srs=f"EPSG:{TARGET_EPSG}",
            workunit_ids=tuple(workunit_ids),
            output=output.get_path(),
            input=raw_usgs_opr_tesm.get_path(),
        ),
        shell=True,  # Allows args to be passed as a string
        check=True,  # Prevents cmd from failing silently
    )

    return output
