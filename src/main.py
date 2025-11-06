import os, yaml, logging
from datetime import datetime
from extract import load_csv_files
from transform import clean_and_split, aggregate
from load import (
    save_clean_to_db,
    save_quarantine_to_db_and_file,
    save_report
)

def setup_logging(log_dir: str, date: str):
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'pipeline_{date}.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
    )

def main():
    with open('config.yaml') as f:
        cfg = yaml.safe_load(f)

    execution_date = datetime.now().strftime(cfg['report']['date_format'])
    setup_logging(cfg['files']['log_dir'], execution_date)

    logging.info('Starting ETL for %s', execution_date)

    try:
        raw_df = load_csv_files(cfg['files']['raw_dir'], execution_date)
        logging.info('Loaded %s rows from raw files', len(raw_df))

        clean_df, quarantine_df = clean_and_split(raw_df, execution_date)
        logging.info('Clean rows: %s, Quarantined: %s', len(clean_df), len(quarantine_df))

        agg_df = aggregate(clean_df)
        logging.info('Aggregation complete')

        save_clean_to_db(clean_df, cfg['db'])
        logging.info('Clean data saved to DB')

        if not quarantine_df.empty:
            save_quarantine_to_db_and_file(
                quarantine_df,
                cfg['db'],
                execution_date,
                cfg['files']['quarantine_dir']
            )
            logging.info('Quarantine data saved')

        save_report(agg_df, execution_date, cfg['files']['output_dir'], cfg['db'])
        logging.info('Daily report saved')

    except Exception as e:
        logging.exception('Pipeline failed: %s', e)
        raise

if __name__ == '__main__':
    main()