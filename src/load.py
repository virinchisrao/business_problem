import pandas as pd
import psycopg2
import psycopg2.extras     
import os
import json
from datetime import datetime

def get_conn(cfg: dict):
    return psycopg2.connect(
        host=cfg['host'],
        port=cfg['port'],
        dbname=cfg['name'],
        user=cfg['user'],
        password=cfg['password']
    )

def save_clean_to_db(clean: pd.DataFrame, db_cfg: dict):
    clean['loaded_at'] = datetime.now()
    with get_conn(db_cfg) as conn:
        with conn.cursor() as cur:
            for _, r in clean.iterrows():
                cur.execute("""
                    INSERT INTO sales.sales_cleaned
                    (sale_date, channel, product_id, product_name,
                     quantity, unit_price, total_amount, loaded_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (r.sale_date, r.channel, r.product_id, r.product_name,
                      int(r.quantity), float(r.unit_price),
                      float(r.total_amount), r.loaded_at))
        conn.commit()

def save_quarantine_to_db_and_file(qdf: pd.DataFrame,
                                   db_cfg: dict,
                                   filename: str,
                                   quarantine_dir: str):
    os.makedirs(quarantine_dir, exist_ok=True)
    csv_path = os.path.join(quarantine_dir, f'quarantined_{filename}.csv')
    qdf.to_csv(csv_path, index=False)

    with get_conn(db_cfg) as conn:
        with conn.cursor() as cur:
            for _, r in qdf.iterrows():
                cur.execute("""
                    INSERT INTO sales.sales_quarantine
                    (filename, row_number, raw_data, error_reason)
                    VALUES (%s,%s,%s,%s)
                """, (filename, int(r.name), r.raw_data, r.error_reason))
        conn.commit()

def save_report(agg: pd.DataFrame, report_date: str, output_dir: str, db_cfg: dict):
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, f'daily_sales_report_{report_date}.csv')
    agg.to_csv(csv_path, index=False)

    with get_conn(db_cfg) as conn:
        with conn.cursor() as cur:
            for _, r in agg.iterrows():
                cur.execute("""
                    INSERT INTO sales.sales_daily_report
                    (report_date, channel, total_sales, total_revenue, top_products)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (report_date, channel)
                    DO UPDATE SET
                        total_sales   = EXCLUDED.total_sales,
                        total_revenue = EXCLUDED.total_revenue,
                        top_products  = EXCLUDED.top_products
                """, (report_date,
                      r.channel,
                      int(r.total_sales),
                      float(r.total_revenue),
                      json.dumps(r.top_products))) 
    conn.commit()