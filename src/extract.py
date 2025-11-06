import pandas as pd
import os
from datetime import datetime

def load_csv_files(raw_dir: str, execution_date: str) -> pd.DataFrame:
    """
    Loads every CSV found in raw_dir whose filename contains execution_date.
    Returns single concatenated DataFrame.
    """
    files = [os.path.join(raw_dir, f)
             for f in os.listdir(raw_dir)
             if execution_date in f and f.endswith('.csv')]
    if not files:
        raise FileNotFoundError(f'No CSV files found for date {execution_date}')
    dfs = [pd.read_csv(f) for f in files]
    return pd.concat(dfs, ignore_index=True)