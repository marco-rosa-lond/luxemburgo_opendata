
from download import get_monthly_files
from bulk_insert import process_and_load_datasets_to_sql


def main():

    # Step 1 - download files
    get_monthly_files()

    # Step 2 - update database
    process_and_load_datasets_to_sql()



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