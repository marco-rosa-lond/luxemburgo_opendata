import csv
import re
from sqlalchemy import create_engine, text
from utils import *
from database import get_connection_string

LINHAS_POR_CHUNK = 800000
DIR_CHUNKS = 'CHUNKS'
DIR_ERRORS = 'Errors'

connection_string = get_connection_string('sqlalchemy')
engine = create_engine(connection_string)
database_name = engine.url.database


def convert_to_proper_types(df):

    for col, dtype in df.dtypes.items():
        if 'date' in col:
            df[col] = pd.to_datetime(df[col].dropna(), format= "%Y%m%d",  errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        if 'code' in col:
            df[col] = df[col].astype('string')
    return df


def recreate_sql_table(df: pd.DataFrame, table_name: str):
    connection = engine.connect()
    try:
        # APAGAR TABELA SE EXISTE
        drop_table_stmt = text(f"IF OBJECT_ID('[{table_name}]', 'U') IS NOT NULL DROP TABLE [{table_name}]")
        connection.execute(drop_table_stmt)
        connection.commit()
        print('Apagou tabela: ', table_name)

        # CRIAR TABELA
        create_sql_stmt = text(generate_create_table(df, table_name))
        connection.execute(create_sql_stmt)
        connection.commit()
        print('Criou tabela SQL: ', table_name)

    except Exception as error:
        print("Erro ao criar tabela:", error)

    finally:
        connection.close()


def bulk_insert_csv_to_sql(file, table_name):
    create_directory(DIR_ERRORS)
    dbo_dir = os.path.join(os.getcwd(), DIR_CHUNKS)
    errors_dir = os.path.join(os.getcwd(), DIR_ERRORS)


    new_file_abs_path = os.path.abspath(os.path.join(dbo_dir, file))
    connection = engine.connect()
    try:
        sql = f"""
            BULK INSERT {database_name}.dbo.[{table_name}]
            FROM '{new_file_abs_path}' 
            WITH (
                FIELDTERMINATOR = '|',
                ROWTERMINATOR = '0x0D0A',
                FIRSTROW = 2,
                CODEPAGE = '65001',
                MAXERRORS = 10000
        )
        """

        # print(sql)
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
    finally:
        connection.close()


def prepare_and_bulk_insert_to_sql(df, table_name, max_rows = LINHAS_POR_CHUNK):
    create_directory(DIR_CHUNKS)

    # Convert data types
    df = df.convert_dtypes()
    df.columns = df.columns.str.strip().dropna()
    df = convert_to_proper_types(df)

    # Drop and recreate the SQL table
    recreate_sql_table(df, table_name)
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
            filepath = os.path.join(DIR_CHUNKS, filename)

            df_chunk.to_csv(filepath, index=False, sep='|', encoding='utf-8')
            bulk_insert_csv_to_sql(filename, table_name)

    # If small enough just import files directly
    else:
        filename = f'{table_name}.csv'
        filepath = os.path.join(DIR_CHUNKS, filename)
        df.to_csv(filepath, index=False, sep='|', encoding='utf-8')
        bulk_insert_csv_to_sql(filename, table_name)



def process_and_load_datasets_to_sql(datasets):

    try:
        # Loop all datasets
        for dataset in datasets:

            # Get CSV files from the download folder
            folder_path = 'Downloads/{}/'.format(dataset)

            csv_files = get_all_csv_files_in_directory(folder_path)
            csv_files.sort(reverse=False)

            if not csv_files:
                raise Exception(f'No csv files found in {folder_path}')

            print(f'\n[UPDATE] database... {dataset}')

            # For OPERATION_DELTA Dataset combine all files into a Dataframe and import into the table
            if str.upper(dataset) == 'OPERATIONS_DELTA':
                table_name = dataset

                combined = pd.DataFrame()

                for i, file in enumerate(sorted(csv_files)):

                    file_path = folder_path + file
                    file_df = pd.read_csv(file_path, sep='|',  encoding='utf-8', low_memory=False)
                    file_df['file_name'] = file

                    # if i == 0:
                    #     sample = file_df.sample(frac=1)
                    # else:
                    #     sample = file_df.sample(frac=0.25)

                    combined = pd.concat([combined, file_df], ignore_index=True)

                prepare_and_bulk_insert_to_sql(combined, table_name)

            # For PARC_AUTOMOBILE import each file into a table named after the file
            if str.upper(dataset) == 'PARC_AUTOMOBILE':

                for file in csv_files:
                    table_name = file.replace('.csv', '')

                    file_path = folder_path + file
                    df = pd.read_csv(file_path, sep='|', encoding='utf-8', low_memory=False)
                    prepare_and_bulk_insert_to_sql(df, table_name)

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        # Cleanup in all cases
        drop_directory(DIR_CHUNKS)
        engine.dispose()
