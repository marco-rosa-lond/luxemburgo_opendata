from datetime import datetime
import re
import pyodbc
import os
from dotenv import load_dotenv
from tqdm import tqdm
from ftp import FtpHandler

load_dotenv("config.env")

server = os.getenv('DB_SERVER')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASS')
database = os.getenv('DB_NAME')
driver = "ODBC+Driver+17+for+SQL+Server"


conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"
)


backup_dir = "DB_BACKUP"
ftp_path = '/FILES/__DATASET__/BACKUPS'


def get_backup_progress(cursor):
    progress = tqdm(total=100, desc="Backup Progress")
    last_percent = 0

    while cursor.nextset():
        for message in cursor.messages:
            msg_text = message[1]
            match = re.search(r"(\d+)\s+percent\s+processed", msg_text)
            if match:
                percent = int(match.group(1))
                if percent > last_percent:
                    progress.update(percent - last_percent)
                    last_percent = percent

        cursor.messages.clear()

    progress.close()


def ftp_save_backup_file(file_abs_path):
    bak_file = os.path.basename(file_abs_path)
    print(f'Sending backup file to {ftp_path}/{bak_file}')
    input('Continue?')


    ftp_handler = FtpHandler()
    try:

        with open(file_abs_path, "rb") as bak_file_bin:
            ftp_handler.send_to_ftp(
                zip_data=bak_file_bin,
                dest_dir=ftp_path,
                filename=bak_file
            )

        print("BACKUP SENT TO FTP")
    except Exception as e:
        print(e)

    finally:
        ftp_handler.close()


def backup_and_send_with_ftp():

    conn = pyodbc.connect(conn_str, autocommit=True)
    os.makedirs(backup_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak_file = f"{database}_{timestamp}.bak"
    file_abs_path = os.path.abspath(os.path.join(backup_dir, bak_file))

    stats = '5'

    try:
        with conn.cursor() as cursor:

            sql = f"""
                    BACKUP DATABASE [{database}] TO DISK = N'{file_abs_path}'
                    WITH COPY_ONLY, NOFORMAT, SKIP, NOREWIND, NOUNLOAD, COMPRESSION, STATS = {stats}
                """
            cursor.execute(sql)
            get_backup_progress(cursor)

        conn.close()

        ftp_save_backup_file(file_abs_path)

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        os.remove(file_abs_path)
        conn.close()



