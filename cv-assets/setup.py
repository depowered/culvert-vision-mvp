from setuptools import find_packages, setup

setup(
    name="cv_assets",
    packages=find_packages(exclude=["cv_assets_tests"]),
    install_requires=["dagster", "python-dotenv", "duckdb==0.8.1", "pandas[parquet]"],
    extras_require={
        "dev": ["dagster-webserver", "pytest", "ruff", "black", "isort", "pyright"]
    },
)
