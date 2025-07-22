import os.path

from utils import *
from SQLServer import *

LINHAS_POR_CHUNK = 800000
DIR_CHUNKS = 'chunk_files'
DIR_ERRORS = 'Errors'

sql_manager = SQLServerManager()
database = sql_manager.database

def convert_to_proper_types(df):
    for col, dtype in df.dtypes.items():
        if 'date' in col:
            df[col] = pd.to_datetime(df[col].dropna(), format= "%Y%m%d",  errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        if 'code' in col:
            df[col] = df[col].astype('string')
    return df


def prepare_and_bulk_insert_to_sql(df, table_name, max_rows = LINHAS_POR_CHUNK):

    errors_dir = os.path.join(os.getcwd(), DIR_ERRORS)
    dbo_dir = os.path.join(os.getcwd(), DIR_CHUNKS)

    df.columns = df.columns.str.strip().dropna()
    df = df.convert_dtypes()
    df = convert_to_proper_types(df)

    # Re-create the sql table
    sql_manager.recreate_sql_table(df, table_name)

    # Create chunk files from large dataframe then bulk insert
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
            new_file_abs_path = os.path.abspath(os.path.join(dbo_dir, filename))

            sql_manager.bulk_insert_csv(new_file_abs_path, table_name, errors_dir, field_terminator='|' )

    # If dataframe is small enough just creates a file and bulk insert
    else:
        filename = f'{table_name}.csv'
        filepath = os.path.join(DIR_CHUNKS, filename)
        df.to_csv(filepath, index=False, sep='|', encoding='utf-8')
        new_file_abs_path = os.path.abspath(os.path.join(dbo_dir, filename))
        sql_manager.bulk_insert_csv(new_file_abs_path, table_name, errors_dir, field_terminator='|')



def process_and_load_to_sql(downloads_dir, dataset):

    try:
        create_directory(DIR_CHUNKS)
        create_directory(DIR_ERRORS)

        # Get CSV files from the download folder
        folder_path = f'{downloads_dir}/{dataset}/'

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
                file_df = pd.read_csv(file_path, sep='|',  encoding='utf-8',low_memory=False)
                file_df = file_df.dropna(axis=1, how='all')
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
        drop_directory(DIR_CHUNKS)



