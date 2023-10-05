from dagster import Definitions, load_assets_from_modules

from dagster_dbt import DbtCliResource
import os
from pathlib import Path

from . import assets

all_assets = load_assets_from_modules([assets])

dbt_project_dir = Path(__file__).joinpath("..", "..", "dbt_project").resolve()

dbt_resource = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

defs = Definitions(
    assets=all_assets,
    asset_checks=[
        assets.no_anamoly_days,
        assets.no_null_orders
    ],
    resources={
        "dbt": dbt_resource
    }
)
