import os
import datetime
import logging
from google.cloud import bigquery, storage
from concurrent.futures import ThreadPoolExecutor

# Logging configuration
logging.basicConfig(
    filename='/backup/logs/BQ_daily_log.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Constants
KEY_FILE = "/root/jsonfiles/ti-dba-prod-01.json"
PROJECT_ID = "ti-dba-prod-01"
BUCKET = "ti-dba-prod-sql-01"
DATASET_ID = "ti_db_inventory"
TABLE_ID = "daily_log"
DATABASE_TYPES = {
    "MSSQL": "Backups/Current/MSSQL",
    "MYSQL": "Backups/Current/MYSQL",
    "PGSQL": "Backups/Current/POSTGRESQL"
}

# Initialize BigQuery and Storage clients
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_FILE
bigquery_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)

# Get today's date
today = datetime.datetime.today()
date_formats = [
    today.strftime("%Y-%m-%d"),
    today.strftime("%Y%m%d"),
    today.strftime("%d-%m-%Y")
]

def get_backup_details(bucket_name, folder_path):
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    
    backup_details = []
    for blob in blobs:
        for date_format in date_formats:
            if date_format in blob.name:
                backup_details.append({
                    "ID": None,  # Assuming BigQuery handles auto-generating IDs
                    "BackupDate": blob.updated.isoformat(),
                    "Server": folder_path.split('/')[-1],
                    "Database": blob.name.split('/')[-2],
                    "Size": blob.size,
                    "FileName": blob.name,
                    "State": "Successful" if blob.exists() else "ERROR",
                    "LastUpdate": datetime.datetime.utcnow().isoformat()
                })
    return backup_details

def insert_to_bigquery(data):
    table_ref = bigquery_client.dataset(DATASET_ID).table(TABLE_ID)
    table = bigquery_client.get_table(table_ref)
    errors = bigquery_client.insert_rows_json(table, data)
    if errors:
        logging.error(f"BigQuery insertion errors: {errors}")
        return False
    return True

def process_database_type(db_type, folder_path):
    logging.info(f"Processing backups for database type: {db_type}")
    backup_details = get_backup_details(BUCKET, folder_path)
    
    if backup_details:
        if insert_to_bigquery(backup_details):
            logging.info(f"Successfully inserted backup details for {db_type}")
        else:
            logging.error(f"Failed to insert backup details for {db_type}")
    else:
        logging.warning(f"No backup details found for {db_type}")

def main():
    logging.info("Starting backup processing script")
    with ThreadPoolExecutor(max_workers=len(DATABASE_TYPES)) as executor:
        futures = [
            executor.submit(process_database_type, db_type, folder_path)
            for db_type, folder_path in DATABASE_TYPES.items()
        ]
        
        for future in futures:
            future.result()  # Ensures we wait for all tasks to complete
    logging.info("Backup processing script completed")

if __name__ == "__main__":
    main()
