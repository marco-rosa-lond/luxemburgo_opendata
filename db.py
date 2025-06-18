
import os
import re
import pandas as pd
from sqlalchemy import create_engine

#
# # DB Connection
# server = 'YOUR_SERVER_NAME'
# database = 'YOUR_DATABASE'
# username = 'YOUR_USERNAME'
# password = 'YOUR_PASSWORD'
#
# connection_string = (
#     f"mssql+pyodbc://{username}:{password}@{server}/{database}"
#     "?driver=ODBC+Driver+17+for+SQL+Server"
# )
# engine = create_engine(connection_string)

server = 'localhost'           
database = 'testDB'

connection_string = (
    f"mssql+pyodbc://@{server}/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)


engine = create_engine(connection_string,
    fast_executemany=True)

def get_file_date_string(filename):
    pattern = re.compile(r'\d{6}')
    text_date_matches = re.findall(pattern, filename)
    print(f'Text date: {text_date_matches[0]}')

    year = int(text_date_matches[0][:4])
    month = int(text_date_matches[0][4:6])
    print(f'Year: {year}, Month: {month}')

    return year,month



def get_all_files_in_directory(directory_path):

    files = [f for f in os.listdir(directory_path) if
             os.path.isfile(os.path.join(directory_path, f))]

    return sorted(files, reverse=True)





def insert_files_to_sql(folder, table):
    files = get_all_files_in_directory(folder)

    for filename in files:
        print(f'Processing file: {filename}')
        file_year, file_month = get_file_date_string(filename)

        filepath = os.path.join(folder, filename)
        df = pd.read_excel(filepath, engine='openpyxl', dtype=str)

        if df.empty:
            print(f"Warning: {filename} is empty. Skipping.")
            raise Exception('No resources found')

        df['file_name'] = filename
        df['file_year'] = file_year
        df['file_month'] = file_month


        print(df.columns)

        # df.to_sql(table, con=engine, if_exists='append', index=False)


        print(f"{len(df)} rows inserted into {table} table.")





