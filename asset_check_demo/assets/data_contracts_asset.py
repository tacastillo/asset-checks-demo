from dagster import asset, AssetCheckSpec, AssetCheckResult, AssetExecutionContext, AssetCheckSeverity, MaterializeResult

from .. import utils

import pandas as pd

@asset(
    check_specs=[
        AssetCheckSpec(
            name="data_contract",
            asset="shipments",
            description="Checks that the schemas are consistent across materializations"
        )
    ]
)
def shipments(context: AssetExecutionContext):# -> MaterializeResult:
    df = pd.DataFrame(
        {
            "date": ["2023-09-01", "2023-09-02", "2023-09-03", "2023-09-04", "2023-09-05", "2023-09-06", "2023-09-07"],
            "total": [100, 200, 300, 400, 500, 600, 700],
        }
    )

    utils.mock_schema_change_for_shipments(context, df)

    schema = utils.write_table(df)

    old_schema = utils.get_recent_metadata_for_asset(context, "shipments", "column_types")

    comparison = {}
    any_changes = True
    destructive_changes = False

    if old_schema is not None:
        comparison = utils.diff_schemas(old_schema, schema)

        destructive_changes = len(comparison["changed"]) > 0 or len(comparison["removed"]) > 0

        any_changes = len(comparison["new"]) > 0 or destructive_changes

    return MaterializeResult(
        metadata={
            "column_types": schema
        },
        check_results=[
            AssetCheckResult(
                check_name="data_contract",
                passed=not any_changes,
                metadata={
                    **comparison
                },
                severity=AssetCheckSeverity.ERROR if destructive_changes else AssetCheckSeverity.WARN
            )
        ]
    )