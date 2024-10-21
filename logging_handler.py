import settings
from google.cloud.logging import Resource
settings.init()
logger = settings._logger

class Backup_Logger:
    @staticmethod
    def process_start():

        res = Resource(
            type="global",
            labels={},
        )

        logger.log_struct(
            {
                "message": "Daily Backup Process",
                "type": "Backup Process Initiated",
                "component": "database-backups",
                "timestamp" : settings.timestamp,
            },
            severity="INFO",
            resource=res,
        )

    @staticmethod
    def write_entry(project_id, server, region, 
                    export_date, database_name, 
                    size, duration):

        res = Resource(
            type="cloudsql_database",
            labels={
                "project_id": project_id,
                "database_id": server,
                "region": region,
            },
        )

        logger.log_struct(
            {
                "message": "Daily Backup Process",
                "database": database_name,
                "date": export_date,
                "size": size,
                "duration": duration,
                "component": "database-backups",
            },
            severity="INFO",
            resource=res,
        )
