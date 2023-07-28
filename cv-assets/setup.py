from setuptools import find_packages, setup

setup(
    name="cv_assets",
    packages=find_packages(exclude=["cv_assets_tests"]),
    install_requires=[
        "dagster",
    ],
    extras_require={
        "dev": ["dagster-webserver", "pytest", "ruff", "black", "isort", "pyright"]
    },
)
