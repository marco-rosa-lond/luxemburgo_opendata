
from download import get_monthly_files
from bulk_insert import process_and_load_datasets_to_sql
from backup import backup_and_send_with_ftp


# [API]
API = 'https://data.public.lu/api/1'

# Datasets {Name : resource_id}
datasets = {
    'Operations_Delta' : 'operations-delta-des-vehicules-au-luxembourg',
    'Parc_Automobile' : 'parc-automobile-du-luxembourg'
}

def main():

    # Step 1 - download files
    # get_monthly_files(API, datasets)

    # Step 2 - update database
    # process_and_load_datasets_to_sql(datasets.keys())

    # Step 3 - Backup the database to ftp destination
    backup_and_send_with_ftp()



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