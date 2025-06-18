
import os
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



def get_all_files_in_directory(directory_path):

    files = [f for f in os.listdir(directory_path) if
             os.path.isfile(os.path.join(directory_path, f))]
    return files



def insert_operations_delta( folder = r'Data/Operations_Delta'):

    files = get_all_files_in_directory(folder)
    print(files)

    # # Read Excel
    # df = pd.read_excel(file_path, engine='openpyxl')
    # # Insert into SQL Server
    # df.to_sql('your_table_name', con=engine, if_exists='append', index=False)
    # print("Data inserted into SQL Server.")



insert_operations_delta()