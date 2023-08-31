from dagster import asset

from cv_assets.resources.raster import LocalRasterFileStorage, RasterFile


@asset
def source_goodhue_dem_1m(raster_storage: LocalRasterFileStorage) -> RasterFile:
    """Digital Elevation Model (DEM) for Goodhue County, Minnesota with a 1-meter resolution.
    Derivative of the MN_Phase4_Metro_H_2011 workunit; created by MnGEO."""

    return raster_storage.get_file_by_filename("1-meter/goodhue_dem_1m.tif")
