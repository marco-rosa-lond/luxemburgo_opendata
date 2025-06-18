
from db import insert_files_to_sql
from download import get_monthly_xlsx_files


def main():
    get_monthly_xlsx_files()
    insert_files_to_sql(r'Data/Operations_Delta/', 'Operations_Delta_Staging_Table')
    insert_files_to_sql(r'Data/Parc_Automobile/', 'Parc_Automobile_Staging_Table')


if __name__ == '__main__':
    main()