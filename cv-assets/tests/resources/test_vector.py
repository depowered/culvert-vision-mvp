from cv_assets.resources.file_storage.vector import get_filename_by_asset_key


def test_get_filename_by_asset_key_gpkg():
    asset_key = "usgs_wesm_gpkg"
    expected_filename = "usgs_wesm.gpkg"
    assert get_filename_by_asset_key(asset_key) == expected_filename


def test_get_filename_by_asset_key_gdb_zip():
    asset_key = "usgs_wesm_gdb_zip"
    expected_filename = "usgs_wesm.gdb.zip"
    assert get_filename_by_asset_key(asset_key) == expected_filename


def test_get_filename_by_asset_key_other():
    asset_key = "example_key"
    expected_filename = "example.key"
    assert get_filename_by_asset_key(asset_key) == expected_filename
