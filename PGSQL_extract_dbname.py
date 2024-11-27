import psycopg2
import configparser
import csv
import os
import subprocess

# Constants
DB_USR = "GenBackupUser"
DB_PWD = "DBB@ckuPU53r*"
CONFIG_FILE = "/backup/configs/PGSQL_servers_list.conf"
OUTPUT_DIR = "/backup/"
SSL_PATH = "/ssl-certs/"

# Activate the virtual environment
def activate_virtualenv():
    try:
        os.chdir('/backup/environments')
        subprocess.call(['source', '/backup/environments/backupv1/bin/activate'], shell=True)
        print("Virtual environment activated.")
    except Exception as e:
        print(f"Failed to activate virtual environment: {e}")

# Load configuration
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

# Ensure output directory exists
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Function to connect to PostgreSQL and fetch database list
def get_databases(instance_name, host, port, ssl_enabled):
    try:
        connection_params = {
            'user': DB_USR,
            'password': DB_PWD,
            'host': host,
            'port': port,
            'dbname': 'postgres'
        }

        if ssl_enabled:
            connection_params['sslmode'] = 'require'
            connection_params['sslrootcert'] = os.path.join(SSL_PATH, instance_name, "server-ca.pem")
            connection_params['sslcert'] = os.path.join(SSL_PATH, instance_name, "client-cert.pem")
            connection_params['sslkey'] = os.path.join(SSL_PATH, instance_name, "client-key.pem")
        
        connection = psycopg2.connect(**connection_params)
        cursor = connection.cursor()
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = [db[0] for db in cursor.fetchall()]
        cursor.close()
        connection.close()
        return databases
    except psycopg2.Error as err:
        print(f"Error: {err}")
        return []

# Main script
def main():
    activate_virtualenv()

    for section in config.sections():
        instance_name = section
        host = config[section].get('host')
        port = config[section].get('port', '5432')
        ssl_enabled = config[section].get('ssl', 'n').lower() == 'y'

        # Fetch databases
        databases = get_databases(instance_name, host, port, ssl_enabled)

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

if __name__ == "__main__":
    main()
