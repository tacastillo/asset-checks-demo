from dagster import (
    asset, 
    asset_check, 
    AssetCheckResult, 
    DailyPartitionsDefinition, 
    AssetExecutionContext,
)

import statistics


from .. import utils

from .nullness_asset import orders

@asset(
    deps=[orders],
    partitions_def=DailyPartitionsDefinition(
        start_date="2023-09-01",
        end_date="2023-09-07"
    )
)
def daily_sales_report(context: AssetExecutionContext):
    df = utils.get_orders_for_date(context.partition_key)

    df = df.groupby("status").sum()

    utils.write_to_daily_report(df, context.partition_key)

    context.add_output_metadata({
        "date": context.partition_key,
        "total_sales": df["total"].sum()
    })

@asset_check(
    asset=daily_sales_report
)
def no_anamoly_days(context: AssetExecutionContext):
    metadata_records = utils.get_metadata_for_asset(context, "daily_sales_report")

    mean = statistics.mean(metadata_records.values())
    stdev = statistics.stdev(metadata_records.values())

    anomalies = []

    for date in metadata_records.keys():
        if abs(metadata_records[date] - mean) > 2 * stdev:
            anomalies.append({
                "date": date,
                "total_sales": metadata_records[date],
                "mean": mean,
            })

    return AssetCheckResult(
        success=len(anomalies) == 0,
        metadata={
            "anomalies": anomalies,
        }
    )