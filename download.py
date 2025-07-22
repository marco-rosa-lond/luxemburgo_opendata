from io import BytesIO
from utils import *
import requests
import re

column_mapping_path = 'column_mapping.csv'

def download(url: str)-> bytes:
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


def download_files(api_endpoint, name, resource, downloads_dir):

    destination_folder = f'{downloads_dir}/{name}/'
    create_directory(destination_folder)

    # get datasets files
    response = requests.get(f'{api_endpoint}/datasets/{resource}/')
    response.raise_for_status()
    resource_list = response.json().get('resources', [])

    # Filter files that match the filename pattern
    filename_pattern = fr'^{name}_\d{{6}}\.xlsx$'
    matching_files = [ r for r in resource_list
                       if re.search(filename_pattern, r['title'])]


    if name == 'Parc_Automobile':
        matching_files = [r for r in matching_files
                          if r['title'].endswith('12.xlsx')]

    if not matching_files:
        raise Exception(f'No resources found for {name}')


    for resource in matching_files:
        url = resource.get('url')
        title = resource.get('title')

        file_name = title.replace('.xlsx', '.csv')
        file_path = os.path.join(destination_folder, file_name)

        # SKIP IF FILE ALREADY EXISTS

        delete_empty_file(file_path)

        if os.path.exists(file_path):
            print('File already exists: ' + title)
            continue

        content = download(url)

        df = pd.read_excel(BytesIO(content), engine="calamine")
        df = df.dropna(axis=1, how='all')

        # rename columns to english
        df = map_column_names(df, column_mapping_path)

        # filter only M1 & N1 vehicles
        df = df[df['european_category_code'].isin(['M1', 'M1G', 'N1', 'N1G'])]

        # convert data types and clean data
        df = df.convert_dtypes()
        df.columns = df.columns.str.strip().dropna()

        df.to_csv(str(file_path), index=False, encoding='utf-8', sep='|')
        delete_empty_file(file_path)

