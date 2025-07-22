
from SQLServer import SQLServerManager
from ftp import FtpHandler
from bulk_insert import process_and_load_to_sql
from utils import unzip_file, zip_file, create_directory

from download import download_files

# [API]
API = 'https://data.public.lu/api/1'

# Datasets {Name : resource_id}
datasets = {
    'Operations_Delta' : 'operations-delta-des-vehicules-au-luxembourg',
    'Parc_Automobile' : 'parc-automobile-du-luxembourg'
}

downloads_dir = 'downloads'
backup_dir = "backups"

ftp_path = '/FILES/__DATASET__/BACKUPS'

def main():

    # unzip_file(Path( downloads_dir + '.zip'))

    sql_manager = SQLServerManager()

    try:

        sql_manager.recreate_database()
        input('press')
        for name, resource in datasets.items():
            print(f'\n{name.upper()}...')
            create_directory(downloads_dir)
            create_directory(f'{downloads_dir}/{name}/')

            # Step 1 - download files
            download_files( API, name, resource, downloads_dir )

            # Step 2 - update database
            process_and_load_to_sql( downloads_dir, name)

        # Step 3 - Backup the database
        create_directory(backup_dir)
        bak_file_path = sql_manager.backup_database(backup_dir)

        # Upload Backup to ftp path
        ftp_handler = FtpHandler()
        ftp_handler.send_to_ftp(bak_file_path, ftp_path)

    finally:
        sql_manager.drop_database()



if __name__ == '__main__':
    try:
        main()
        print('Done')
    except Exception as e:
        print("An error occurred:",
              e)
        import traceback
        traceback.print_exc()
        input("Press Enter to exit...")