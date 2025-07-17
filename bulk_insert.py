import csv
import os
import re
import shutil
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv("config.env")

server = os.getenv('DB_SERVER')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASS')
database = os.getenv('DB_NAME')
driver = "ODBC+Driver+17+for+SQL+Server"


if server == 'localhost':
    # Windows Auth on localhost
    connection_string = (
        f"mssql+pyodbc://@{server}/{database}"
        f"?driver={driver}&trusted_connection=yes"
    )
else:
    # SQL Auth on remote
    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        f"?driver={driver}"
    )



engine = create_engine(connection_string,
    fast_executemany=True)

connection = engine.connect()

LINHAS_POR_CHUNK = 800000
DIRETORIO_DBO = 'DBO'


pandas_to_sql = {
    'int8': 'SMALLINT',
    'int16': 'SMALLINT',
    'int32': 'INT',
    'int64': 'INT',
    'float32': 'FLOAT',
    'float64': 'FLOAT',
    'bool': 'BIT',
    'datetime64[ns]': 'DATE',
    'timedelta[ns]': 'TIME'
}


def convert_to_proper_types(df):

    for col, dtype in df.dtypes.items():
        if 'date' in col:
            df[col] = pd.to_datetime(df[col].dropna(), format= "%Y%m%d",  errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        if 'code' in col:
            df[col] = df[col].astype('string')
    return df



def generate_create_table(df: pd.DataFrame, nome_tabela: str) -> str:
    #sql_cols.append(f"[Id] numeric identity constraint [{nome_tabela}_pk] primary key")

    nome_tabela = nome_tabela.replace(".csv","")
    sql_column_defs = []


    for col in df.columns:
        dtype = str(df[col].dtype).lower()
        max_len = df[col].astype(str).str.len().max()
        sql_type = pandas_to_sql.get(dtype, f'VARCHAR({max_len})')

        if sql_type == f'VARCHAR({max_len})':
            if df[col].apply(lambda x: any(ord(c) > 127 for c in str(x))).any():
                sql_type = f'NVARCHAR({max_len})'

        sql_column_defs.append(f"[{col}] {sql_type}")


    colunas_sql_str = ",\n    ".join(sql_column_defs)
    return f"CREATE TABLE [{nome_tabela}] (\n    {colunas_sql_str}\n);"


def drop_sql_table(table_name: str):

    connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
    connection.commit()

def create_sql_table(df: pd.DataFrame, table_name: str):
    table_name = table_name.replace(".csv", "")

    connection.execute(text(f"IF OBJECT_ID('[{table_name}]', 'U') IS NOT NULL DROP TABLE [{table_name}]"))
    create_sql = generate_create_table(df, table_name)
    print("Criando tabela com SQL:\n", create_sql)

    connection.execute(text(create_sql))
    connection.commit()


def get_all_csv_files_in_directory(directory_path):

    files = [f for f in os.listdir(directory_path) if
             os.path.isfile(os.path.join(directory_path, f))  and f.lower().endswith('.csv')]

    return sorted(files, reverse=True)

def bulk_insert_csv_to_sql(file, table_name):

    dbo_dir = os.path.join(os.getcwd(), DIRETORIO_DBO)
    errors_dir = os.path.join(os.getcwd(), "Errors")

    new_file_abs_path = os.path.abspath(os.path.join(dbo_dir, file))
    try:
        sql = f"""
            BULK INSERT {database}.dbo.[{table_name}]
            FROM '{new_file_abs_path}' 
            WITH (
                FIELDTERMINATOR = '|',
                ROWTERMINATOR = '0x0D0A',
                FIRSTROW = 2,
                CODEPAGE = '65001',
                MAXERRORS = 10000
        )
        """
        print(sql)
        print(f"A importar: {file}")

        # input("PRESS TO IMPORT")
        connection.execute(text(sql))
        connection.commit()

    except Exception as e:
        print(f"Erro ao importar {file}: {e}")
        linhas_com_erro = []

        for match in re.finditer(r"row (\d+), column", str(e)):
            linhas_com_erro.append(int(match.group(1)))
        if linhas_com_erro:
            print(f"Linhas com erro detectadas: {linhas_com_erro}")
            erro_csv = os.path.join(errors_dir, f"errors_{file}")

            with (open(new_file_abs_path, encoding="utf-8") as infile,
                  open(erro_csv, "w", newline="", encoding="utf-8") as outfile):
                reader = csv.reader(infile, delimiter="|")
                writer = csv.writer(outfile, delimiter="|")
                header = next(reader)
                writer.writerow(header)
                for i, row in enumerate(reader, start=2):
                    if i in linhas_com_erro:
                        writer.writerow(row)
            print(f"Linhas com erro guardadas em: {erro_csv}")




def prepare_and_bulk_insert_to_sql(df, table_name, max_rows = LINHAS_POR_CHUNK):

    # Convert data types & translate / clean column names
    df = df.convert_dtypes()
    df.columns = df.columns.str.strip().dropna()
    df = convert_to_proper_types(df)

    # Drop and recreate the SQL table
    drop_sql_table(table_name)
    create_sql_table(df, table_name)
    print("Created table {}".format(table_name))

    # Chunking logic for large datasets
    if len(df) > max_rows:
        total_rows = len(df)
        num_files = (total_rows // max_rows) + int(total_rows % max_rows > 0)

        for i in range(num_files):
            start_row = i * max_rows
            end_row = start_row + max_rows
            df_chunk = df.iloc[start_row:end_row]

            filename = f'{table_name}_part_{i + 1}.csv'
            filepath = os.path.join(DIRETORIO_DBO, filename)

            df_chunk.to_csv(filepath, index=False, sep='|', encoding='utf-8')
            bulk_insert_csv_to_sql(filename, table_name)

    # If small enough just import files directly
    else:
        filename = f'{table_name}.csv'
        filepath = os.path.join(DIRETORIO_DBO, filename)
        df.to_csv(filepath, index=False, sep='|', encoding='utf-8')
        bulk_insert_csv_to_sql(filename, table_name)




def process_and_load_datasets_to_sql(datasets):

    # Create the required directories
    os.makedirs(DIRETORIO_DBO, exist_ok=True)
    os.makedirs("Errors", exist_ok=True)

    # Loop all datasets
    for dataset in datasets:

        print('\n[UPDATE] database... {}'.format(dataset))

        # Get CSV files from the download folder
        folder_path = 'Downloads/{}/'.format(dataset)
        csv_files = get_all_csv_files_in_directory(folder_path)
        csv_files.sort(reverse=False)

        if not csv_files:
            raise Exception('No csv files found in {}'.format(folder_path))

        # For OPERATION_DELTA Dataset combine all files into a Dataframe and import into the table
        if str.upper(dataset) == 'OPERATIONS_DELTA':
            table_name = dataset

            combined = pd.DataFrame()

            for i, file in enumerate(sorted(csv_files)):
                file_path = folder_path + file
                file_df = pd.read_csv(file_path, sep='|',  encoding='utf-8')
                file_df['file_name'] = file
                combined = pd.concat([combined, file_df], ignore_index=True)

            prepare_and_bulk_insert_to_sql(combined, table_name)

        # For PARC_AUTOMOBILE import each file into a table named after the file
        if str.upper(dataset) == 'PARC_AUTOMOBILE':

            for file in csv_files:
                table_name = file.replace('.csv', '')
                file_path = folder_path + file
                df = pd.read_csv(file_path, sep='|', encoding='utf-8')

                prepare_and_bulk_insert_to_sql(df, table_name)

    # clean up temporary directory
    shutil.rmtree(DIRETORIO_DBO)




engine.dispose()


