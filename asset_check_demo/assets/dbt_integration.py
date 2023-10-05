from dagster import AssetExecutionContext

from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator, DagsterDbtTranslatorSettings
from pathlib import Path

dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "dbt_project").resolve()

dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")

translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(
        enable_asset_checks=True,
    )
)

@dbt_assets(manifest=dbt_manifest_path, dagster_dbt_translator=translator)
def jaffle_shop_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()