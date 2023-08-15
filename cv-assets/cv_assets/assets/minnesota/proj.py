"""Defines a common Coordinate Representation System (CRS) to project all assets."""
from pyproj import CRS

EPSG_CODE = 26915
crs = CRS.from_epsg(EPSG_CODE)
