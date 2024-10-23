import settings as settings
import json
import time
import threading
from googleapiclient.errors import HttpError
from googleapiclient import discovery
from google.cloud import storage


SECONDS_BETWEEN_OPERATION_STATUS_CHECKS = 10
_client = None

settings.init()
logger = settings._logger


class CloudSQL:
    @staticmethod
    def getfilesize(file_path, bucket_name):
        # Initialize the storage client with the service account key
        credentials = settings.credentials
        client = storage.Client(credentials=credentials)
        file_size = 0

        try:
            # Get the bucket
            bucket = client.get_bucket(bucket_name)

            # Get the blob (file) from the bucket
            blob = bucket.blob(file_path)

            # Fetch the metadata for the blob
            blob.reload()

            # Get the file size in bytes
            file_size = blob.size
        except:
            file_size = 0

        return file_size

    @staticmethod
    def getListOfCloudSQLS(project_id):
        sqladmin = discovery.build(
            "sqladmin",
            "v1beta4",
            cache_discovery=False,
            credentials=settings.credentials,
        )
        request = sqladmin.instances().list(project=project_id)
        x = []
        while request is not None:
            response = request.execute()

            for database_instance in response["items"]:
                ipAddress = json.loads(json.dumps(database_instance["ipAddresses"]))

                if len(ipAddress) == 2:
                    publicip = ipAddress[0]["ipAddress"]
                    privateip = ipAddress[1]["ipAddress"]
                elif len(ipAddress) == 1:
                    publicip = ipAddress[0]["ipAddress"]
                    privateip = ""
                x.append(
                    {
                        "name": database_instance["name"],
                        "project": database_instance["project"],
                        "databaseInstalledVersion": database_instance[
                            "databaseInstalledVersion"
                        ],
                        "connectionName": database_instance["connectionName"],
                        "publicip": publicip,
                        "privateip": privateip,
                        "serviceAccountEmailAddress": database_instance[
                            "serviceAccountEmailAddress"
                        ],
                        "state": database_instance["state"],
                    }
                )
            request = sqladmin.instances().list_next(
                previous_request=request, previous_response=response
            )
        pprint(x, indent=2)

    @staticmethod
    def getServerType(gcp_instance_name, project_id):
        sqladmin = discovery.build(
            "sqladmin", "v1beta4", cache_discovery=False, credentials=settings.credentials
        )

        req = sqladmin.instances().get(project=project_id, instance=gcp_instance_name)
        resp = req.execute()

        resource = {}
        if "databaseInstalledVersion" in resp:
            version = json.loads(json.dumps(resp["databaseVersion"]))
            ipAddress = json.loads(json.dumps(resp["ipAddresses"]))
            connectioname = json.loads(json.dumps(resp["connectionName"]))

            if len(ipAddress) == 2:
                publicip = ipAddress[0]["ipAddress"]
                privateip = ipAddress[1]["ipAddress"]
            elif len(ipAddress) == 1:
                publicip = ipAddress[0]["ipAddress"]
                privateip = ""

            resource = dict.fromkeys(
                ["sqldata"],
                [
                    gcp_instance_name,
                    project_id,
                    connectioname,
                    version,
                    publicip,
                    privateip,
                ],
            )

        sqladmin.close()

        return resource

    @staticmethod
    def export(project_id, instance_id, save_path, date_prefix, bucket_name):
        global _client
        _client = discovery.build(
            "sqladmin",
            "v1beta4",
            credentials=settings.credentials,
            cache_discovery=False,
        )
        try:
            request = _client.databases().list(project=project_id, instance=instance_id)
            response = request.execute()
            if "items" in response:
                dbs = json.loads(json.dumps(response["items"]))
                threads = []
                for i in range(len(dbs)):
                    data = dbs[i]
                    if data["name"] not in [
                        "mysql",
                        "information_schema",
                        "sys",
                        "performance_schema",
                    ]:
                        thread = threading.Thread(
                            target=CloudSQL.export_single_database,
                            args=(data, project_id, instance_id, save_path, date_prefix, bucket_name),
                        )
                        threads.append(thread)
                        thread.start()

                for thread in threads:
                    thread.join()

        except HttpError as e:
            print(e.resp.status)
            if e.resp.status in [403, 500, 503, 409]:
                time.sleep(20)
        except FileNotFoundError as e:
            print(e.errno)

    @staticmethod
    def export_single_database(data, project_id, instance_id, save_path, date_prefix, bucket_name):
        export_uri = "gs://" + bucket_name + "/" + save_path + instance_id + "/"
        uri = export_uri + date_prefix + "_" + data["name"] + ".sql.gz"
        database = data["name"]
        print(f"Begin to export: {instance_id} database: {database}")
        content = CloudSQL._create_export_context(uri, db_name=data["name"])
        logger.log_text(f"Begin to Export: {instance_id}, database: {data['name']}")
        CloudSQL.execute(export_context=content, project_id=project_id, instance_id=instance_id)

        filepath = save_path + instance_id + "/" + date_prefix + "_" + data["name"] + ".sql.gz"
        filesize = CloudSQL.getfilesize(filepath, bucket_name)

        print(f"End of export: {instance_id} database: {database}")
        logger.log_text(f"End of Export: {instance_id}, database: {data['name']}. size: {filesize}")

    @staticmethod
    def _create_export_context(export_uri, db_name):
        export_context = {
            "exportContext": {
                "kind": "sql#exportContext",
                "fileType": "SQL",
                "offload": True,
                "uri": export_uri,
                "databases": [db_name],
            }
        }
        return export_context

    @staticmethod
    def execute(export_context, project_id, instance_id):
        export_request = _client.instances().export(
            project=project_id, instance=instance_id, body=export_context
        )

        try:
            response = export_request.execute()
            operation_id = response["name"]
            operation_success = CloudSQL.wait_until_operation_finished(project_id, operation_id)
        except HttpError as err:
            print(f"Failed to export database: {format(err)}")
            logger.log_text(f"Failed to export database: {format(err)}")
            return False

    @staticmethod
    def wait_until_operation_finished(project_id, operation_id):
        operation_in_progress = True
        operation_success = False

        while operation_in_progress:
            get_operation = _client.operations().get(project=project_id, operation=operation_id)
            operation = get_operation.execute()
            operation_status = operation["status"]

            if operation_status in {"PENDING", "RUNNING", "UNKNOWN"}:
                time.sleep(SECONDS_BETWEEN_OPERATION_STATUS_CHECKS)
            elif operation_status == "DONE":
                operation_in_progress = False

        if "error" in operation:
            errors = operation["error"].get("errors", [])
            for error in errors:
                logger.log_text(
                    f"Operation {operation_id} finished with error: {error.get('message')}"
                )
        else:
            operation_success = True

        return operation_success
