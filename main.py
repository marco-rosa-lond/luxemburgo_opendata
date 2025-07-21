import zipfile

from database import drop_database, create_database
from download import get_monthly_files
from bulk_insert import process_and_load_datasets_to_sql
from backup import backup_and_send_with_ftp
from utils import unzip_file, zip_file, Path


# [API]
API = 'https://data.public.lu/api/1'

# Datasets {Name : resource_id}
datasets = {
    'Operations_Delta' : 'operations-delta-des-vehicules-au-luxembourg',
    'Parc_Automobile' : 'parc-automobile-du-luxembourg'
}

ftp_path = '/FILES/__DATASET__/BACKUPS'
download_dir = 'downloads'

def main():
    create_database()
    unzip_file(Path( download_dir + '.zip'))

    try:
        # Step 1 - download files
        for name, resource in datasets.items():
            get_monthly_files( API, name, resource)
        print('\nAll Files Downloaded')


        # Step 2 - update database
        process_and_load_datasets_to_sql(datasets.keys())

        # Step 3 - Backup the database to ftp destination
        backup_and_send_with_ftp(ftp_path)

    finally:
        drop_database()
        zip_file(Path(download_dir))




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