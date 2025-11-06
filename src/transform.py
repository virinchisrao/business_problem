import pandas as pd
from typing import Tuple

def clean_and_split(
        df: pd.DataFrame,
        execution_date: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (clean_df, quarantine_df)
    """
    df = df.copy()
    df['sale_date'] = pd.to_datetime(execution_date).date()

    # Standard column names
    rename = {
        'product_id': 'product_id',
        'product_name': 'product_name',
        'quantity': 'quantity',
        'unit_price': 'unit_price',
        'channel': 'channel'
    }
    df.rename(columns={k: v for k, v in rename.items() if k in df.columns},
              inplace=True)

    # Validation rules
    invalid = (df['product_id'].isna()) | \
              (df['quantity'].isna()) | \
              (df['unit_price'].isna()) | \
              (df['quantity'] < 0) | \
              (df['unit_price'] < 0)

    quarantine = df[invalid].copy()
    quarantine['error_reason'] = 'missing/negative values'
    quarantine['raw_data'] = quarantine.apply(lambda r: str(r.to_dict()), axis=1)

    clean = df[~invalid].copy()
    clean['total_amount'] = clean['quantity'] * clean['unit_price']
    return clean, quarantine


def aggregate(clean: pd.DataFrame) -> pd.DataFrame:
    """
    Produces daily aggregates per channel + top-5 products JSON.
    """
    by_channel = clean.groupby('channel').agg(
        total_sales=('quantity', 'sum'),
        total_revenue=('total_amount', 'sum')
    ).reset_index()

    top_products = (clean
                    .groupby(['product_id', 'product_name'])
                    .agg(qty=('quantity', 'sum'))
                    .sort_values('qty', ascending=False)
                    .head(5)
                    .reset_index()
                    .to_dict(orient='records'))

    # attach same top-products to every channel row
    import json
    by_channel['top_products'] = json.dumps(top_products)
    return by_channel