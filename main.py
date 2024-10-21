from logging_handler import Backup_Logger
import datetime
from export import Database

if __name__ == "__main__":
    log = Backup_Logger()
    log.process_start()

    print(Database.getServerType("ti-mysql-us-we-14", "ti-ca-infrastructure"))
