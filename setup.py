from setuptools import find_packages, setup

setup(
    name="asset_check_demo",
    packages=find_packages(exclude=["asset_check_demo_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "dagster-dbt",
        "dbt-duckdb",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
