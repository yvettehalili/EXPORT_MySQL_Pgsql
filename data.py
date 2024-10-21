import src.settings as settings

from google.cloud import bigquery
from google.cloud.exceptions import BadRequest
from tables_schemas import DailyBackupLogSchema

class Servers:
    def list_all():
        client = bigquery.Client(credentials=settings._credentials, project=settings._my_cfg.project)

        job_config = bigquery.QueryJobConfig(use_query_cache=True)
        sql = ""

        sql = """
            select  name, ip, os, frecuency, type, location, encrypted, ssl, user, pwd,
            backup, load, size, bucket, authfile, save_path, project
            FROM `""" + settings._my_cfg.project + "." + settings._my_cfg.dataset + "." + settings._my_cfg.table + """`
            WHERE active = true and os='CLOUDSQL'
            ORDER BY name
        """
        query_job = client.query(sql, job_config=job_config)
        results = query_job.result()

        return results
    
    def get_server_info(server_name):
        client = bigquery.Client(credentials=settings._credentials, project=settings._my_cfg.project)

        job_config = bigquery.QueryJobConfig(use_query_cache=True)
        sql = ""

        sql = """
            select  name, ip, os, frecuency, type, location, encrypted, ssl, user, pwd,
            backup, load, size, bucket, authfile, save_path, project
            FROM `""" + settings._my_cfg.project + "." + settings._my_cfg.dataset + "." + settings._my_cfg.table + """`
            WHERE name = '""" + server_name + """'
            ORDER BY name
        """
        query_job = client.query(sql, job_config=job_config)
        results = query_job.result()

        return results
    
    def insert_to_log(rows_to_insert):
        client = bigquery.Client(credentials=settings._credentials, project=settings._my_cfg.project)
        dataset = client.dataset(settings._dataset)
        table = dataset.table(settings._table_log)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        template_schema = DailyBackupLogSchema()
        bigquery_schema = template_schema.Daily_Backup_Log

        job_config.schema = bigquery_schema
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        load_job = client.load_table_from_json(rows_to_insert, table, job_config=job_config, num_retries=10)

        try:
            load_job.result(timeout=3600)  # Waits for table load to complete.
            print("Loaded {} rows.".format(load_job.output_rows))
        except BadRequest as e:
            for e in load_job.errors:
                print('ERROR: {}'.format(e['message']))
    
    def delete_log(backup_date, server):
        client = bigquery.Client(credentials=settings._credentials, project=settings._my_cfg.project)
        dataset = client.dataset(settings._dataset)
        table = dataset.table(settings._table_log)
        query = (
                'DELETE FROM `' + settings._project + "." + settings._dataset + "." + settings._table_log  + "` "
                'WHERE backup_server ="' + server + '" and  backup_date = "' + backup_date + '" '
        )
        
        query_job = client.query(query)  # API request
        try:
            rows = query_job.result(timeout=3600)  # Waits for query to finish
        except BadRequest as e:
            print('ERROR: {}')
            


