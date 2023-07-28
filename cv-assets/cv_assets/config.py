from pathlib import Path

from dagster import file_relative_path

CV_ASSETS_DATA = Path(file_relative_path(__file__, "../../data/cv_assets"))
