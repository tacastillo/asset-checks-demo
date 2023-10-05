from dagster import asset, asset_check, AssetCheckResult
from ..utils import get_orders, TRANSFORMED_ORDERS_CSV_PATH

import pandas as pd

@asset
def orders():
    df = get_orders()

    df = df[df["status"] != "canceled"]
    df['total'] = df['quantity'] * df['unit_price']

    df.to_csv(TRANSFORMED_ORDERS_CSV_PATH, index=False)

@asset_check(
    asset=orders,
)
def no_null_orders():
    df = pd.read_csv(TRANSFORMED_ORDERS_CSV_PATH)

    all_not_null = bool(df["customer_id"].notnull().all())

    return AssetCheckResult(
        passed=all_not_null,
        metadata={
            "num_rows": len(df),
            "num_empty": len(df[df["customer_id"].isnull()]),
        },
    )