import os
import pandas as pd

from dagster import file_relative_path, EventRecordsFilter, DagsterEventType, AssetKey, AssetExecutionContext

from typing import Mapping

import random

RAW_ORDERS_CSV_PATH = file_relative_path(__file__, "../data/sample_orders.csv")
TRANSFORMED_ORDERS_CSV_PATH = file_relative_path(__file__, "../data/orders_result.csv")
ORDER_CSV_REPORT_PATH = file_relative_path(__file__, "../data/orders_report.csv")
SHIPMENTS_CSV_PATH = file_relative_path(__file__, "../data/shipments.csv")

def get_orders() -> pd.DataFrame:
    return pd.read_csv(RAW_ORDERS_CSV_PATH)

def get_orders_for_date(date: str) -> pd.DataFrame:
    df = pd.read_csv(TRANSFORMED_ORDERS_CSV_PATH)

    return df[df["date"] == date]

def write_to_daily_report(df: pd.DataFrame, date: str):
    if not os.path.exists(ORDER_CSV_REPORT_PATH):
        # If it doesn't exist, create a new CSV file with the provided dataframe
        df.to_csv(ORDER_CSV_REPORT_PATH, index=False)
    else:
        existing_data = pd.read_csv(ORDER_CSV_REPORT_PATH)
        existing_data = existing_data[existing_data['date'] != date]

        updated_data = pd.concat([existing_data, df], ignore_index=True)

        updated_data.to_csv(ORDER_CSV_REPORT_PATH, index=False)

def get_sales_metadata_for_asset(context, asset_name: str):
    """
    Non-generic function to get metadata from assets that have
    a "total_sales" and "date" metadata key
    """
    records = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION, asset_key=AssetKey(asset_name)
        )
    )

    metadata_records = [
        record.asset_materialization.metadata for record in records
    ]

    metadata = {}

    for record in metadata_records:
        if "total_sales" in record and record["date"].value not in metadata:
            metadata[record["date"].value] = record["total_sales"].value

    return metadata

def get_recent_metadata_for_asset(context: AssetExecutionContext, asset_name: str, metadata_key: str):
    metadata_records = [
        record.asset_materialization.metadata
        for record in context.instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=AssetKey(asset_name)
            ),
            limit=1
        )
    ]

    return metadata_records[0][metadata_key].value if len(metadata_records) > 0 else None

def diff_schemas(schema_a: Mapping[str, str], schema_b: Mapping[str, str]):
    keys_a = set(schema_a.keys())
    keys_b = set(schema_b.keys())
    
    common_keys = keys_a.intersection(keys_b)
    
    unique_to_schema_a = keys_a - keys_b
    unique_to_schema_b = keys_b - keys_a
    
    comparison_result = {
        'changed': {},
        'removed': list(unique_to_schema_a),
        'new': list(unique_to_schema_b),
    }
    
    # Compare values for common keys
    for key in common_keys:
        if schema_a[key] != schema_b[key]:
            difference = {
                'key': key,
                'old': schema_a[key],
                'new': schema_b[key]
            }
            comparison_result['changed'][key] = difference
    
    return comparison_result

def mock_schema_change_for_shipments(context: AssetExecutionContext, df: pd.DataFrame):
    """
    A utility method for the data contracts asset.
    Changes the schema of the table based on the materialization.
    """

    records = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=AssetKey("shipments")
        )
    )

    if len(records) % 3 == 0:
        df["tracking_number"] = [random.randint(100000, 999999) for _ in range(len(df))]
    else:
        df["total"] = [float(total) for total in df["total"]]
        df["status"] = ["shipped" for _ in range(len(df))]


def write_table(df: pd.DataFrame):
    """
    A utility method for the data contracts asset.
    Writes the dataframe to a CSV file.
    """
    df.to_csv(SHIPMENTS_CSV_PATH, index=False)
    return convert_pandas_dtypes_to_serializable_dict(df)


def convert_pandas_dtypes_to_serializable_dict(df: pd.DataFrame):
    """Converts the pandas dtypes to a serializable dict."""
    return {
        column: "date" if column == "date" else str(df.dtypes[column])
        for column in df.dtypes.index
    }