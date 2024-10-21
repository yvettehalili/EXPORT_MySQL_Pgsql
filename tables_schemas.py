from google.cloud import bigquery


class DailyBackupLogSchema:
    def __init__(self):
        self.Daily_Backup_Log = [
            bigquery.SchemaField("backup_date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("backup_server", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("backup_filename", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("backup_filesize", "NUMERIC", mode="NULLABLE"),
            bigquery.SchemaField("backup_path", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("backup_last_modified", "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("backup_bucket", "STRING", mode="NULLABLE"),
        ]