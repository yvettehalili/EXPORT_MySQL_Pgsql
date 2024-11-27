import mysql.connector
import configparser
import csv
import os

# Constants
DB_USR = "GenBackupUser"
DB_PWD = "DBB@ckuPU53r*"
CONFIG_FILE = "/backup/configs/MYSQL_servers_list.conf"
OUTPUT_DIR = "/backup/"

# Load configuration
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

# Ensure output directory exists
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Function to connect to MySQL and fetch database list
def get_databases(host, ssl_enabled):
    try:
        connection_params = {
            'user': DB_USR,
            'password': DB_PWD,
            'host': host,
            'ssl_disabled': not ssl_enabled
        }
        connection = mysql.connector.connect(**connection_params)
        cursor = connection.cursor()
        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]
        cursor.close()
        connection.close()
        return databases
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return []

# Main script
for section in config.sections():
    instance_name = section
    host = config[section].get('host')
    ssl_enabled = config[section].get('ssl', 'n').lower() == 'y'
    
    # Fetch databases
    databases = get_databases(host, ssl_enabled)
    
    if databases:
        # Save databases to CSV
        csv_file = os.path.join(OUTPUT_DIR, f"{instance_name}_databaselist.csv")
        with open(csv_file, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['Instance Name', 'Database Name'])
            for db in databases:
                csvwriter.writerow([instance_name, db])
        print(f"Database list for {instance_name} saved to {csv_file}")
    else:
        print(f"Failed to fetch databases for {instance_name}")
