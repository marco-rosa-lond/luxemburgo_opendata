
from download import get_monthly_files
from bulk_insert import update_database


def main():

    # Step 1 - download files
    get_monthly_files()

    # Step 2 - update database
    update_database()



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