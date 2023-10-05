import os
import pandas as pd

from dagster import file_relative_path, AssetExecutionContext, EventRecordsFilter, DagsterEventType, AssetKey

RAW_ORDERS_CSV_PATH = file_relative_path(__file__, "../data/sample_orders.csv")
TRANSFORMED_ORDERS_CSV_PATH = file_relative_path(__file__, "../data/orders_result.csv")
ORDER_CSV_REPORT_PATH = file_relative_path(__file__, "../data/orders_report.csv")

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

def get_metadata_for_asset(context, asset_name: str):
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