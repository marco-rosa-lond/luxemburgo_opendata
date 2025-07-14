

import requests, os.path, re
import time
from config import API, datasets
import pandas as pd
from io import BytesIO
import python_calamine
# from python_calamine.pandas import pandas_monkeypatch
from pandas import read_excel

COLUMN_MAPING_PATH = 'column_mapping.csv'


def time_it_took(start_time:float, action:str):
    end = time.time()
    print(f"{action} took {end - start_time:.2f} seconds")


def get_column_mapping(mapping_file = COLUMN_MAPING_PATH):
    return pd.read_csv(mapping_file)



def map_columns(df ):
    csv_mapping = get_column_mapping()

    column_mapping = dict(zip(csv_mapping['original_name'], csv_mapping['new_name']))
    not_mapped_cols =  set(df.columns) - set(column_mapping.keys())
    if not_mapped_cols:
        raise ValueError(f"Mapping of columns: {not_mapped_cols}")


    type_mapping = dict(zip(csv_mapping['original_name'], csv_mapping['type']))

    for col in df.columns.tolist():
        tipo = type_mapping.get(col)

        if tipo == 'int':
            df[col] = pd.to_numeric(df[col], errors='coerce').dropna() \
                .astype(int) \
                .astype(str)

        if tipo == 'float':
            df[col] = pd.to_numeric(df[col], errors='coerce').dropna() \
                .astype(float) \

        if 'date' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce').dropna() \


    return df.rename(columns=column_mapping)





def download_file(url: str)-> bytes:
    print('\nDownloading: ' + url)
    content = None
    try:
        start = time.time()

        s = requests.get(url)
        s.raise_for_status()
        content = s.content

        print(f"[SUCCESS] File was Downloaded")
        time_it_took(start, 'DOWNLOAD')

    except requests.exceptions.HTTPError as eh:
        print("HTTP Error:", eh)
    except requests.exceptions.ConnectionError as ec:
        print("Connection Error:", ec)
    except requests.exceptions.Timeout as et:
        print("Timeout Error:", et)
    except requests.exceptions.RequestException as err:
        print("Unexpected Error:", err)

    return content




def download_all_files(resources, destination_folder):

    for res in resources:

        file_mame  = res['title'].replace('.xlsx', '.csv')
        file_path = os.path.join(destination_folder, file_mame)

        #SKIP IF FILE ALREADY EXISTS
        if os.path.exists(file_path):
            if os.path.getsize(file_path) == 0:
                os.remove(file_path)
            else:
                print('File already exists: ' + res['title'])
                continue

        # DOWNLOAD
        content = download_file(res['url'])

        # READ EXCEL
        start = time.time()
        df = read_excel(BytesIO(content), engine="calamine", dtype='string')
        time_it_took(start, 'READ EXCEL')

        # DROP COLUMNS only if all values are NaN.
        df = df.dropna(axis=1, how='all')

        # MAPEAR NOMES DAS COLUNAS & COLUMN TYPES
        df = map_columns(df)

        # FILTRAR VEICULOS LIGEIROS
        df = df[df['european_category_code'].isin(['M1', 'M1G', 'N1', 'N1G'])]

        # CREATE CSV FILE
        df.to_csv(str(file_path), index=False, encoding='utf-8', sep='|')

        if os.path.getsize(file_path) == 0:
            os.remove(file_path)
            raise Exception('File was not downloaded')






def get_monthly_files():
    """
    Downloads monthly Excel files for each dataset defined in `datasets`.
    Filters files based on filename pattern and, for some datasets, specific suffixes.
    """
    for resource, filename_prefix in datasets.items():
        # Define the filename pattern: e.g., "Parc_Automobile_202507.xlsx"
        filename_pattern = fr'^{filename_prefix}_\d{{6}}\.xlsx$'

        # Create destination folder if it doesn't exist
        destination_folder = f'Data/{filename_prefix}/'
        os.makedirs(destination_folder, exist_ok=True)

        # Fetch dataset metadata from the API
        response = requests.get(f'{API}/datasets/{resource}/')
        response.raise_for_status()
        resource_list = response.json().get('resources', [])

        # Filter resources that match the filename pattern
        matching_files = [
            r for r in resource_list if re.search(filename_pattern, r['title'])
        ]

        # If it's the Parc_Automobile dataset, only include files ending with '12.xlsx'
        if filename_prefix == 'Parc_Automobile':
            matching_files = [
                r for r in matching_files if r['title'].endswith('12.xlsx')
            ]

        # Sort resources by publication date, descending
        matching_files.sort(key=lambda x: x['published'], reverse=True)

        if not matching_files:
            raise Exception(f'No resources found for {filename_prefix}')

        # Download files
        download_all_files(matching_files, destination_folder)

    print('\nAll Files Downloaded')


