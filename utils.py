import os
import time
import shutil
import pandas as pd
import zipfile
from pathlib import Path


def get_all_csv_files_in_directory(directory_path):
    files = [f for f in os.listdir(directory_path) if
             os.path.isfile(os.path.join(directory_path, f))  and f.lower().endswith('.csv')]
    return sorted(files, reverse=True)

def create_directory(folder):
    if not os.path.exists(folder):
        os.makedirs(folder)

def drop_directory(folder):
    if os.path.exists(folder):
        shutil.rmtree(folder)

def delete_empty_file(file_path):
    if (os.path.exists(file_path)
            and os.path.getsize(file_path) == 0):
        os.remove(file_path)

def count_file_rows(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f)

def time_it_took(start_time: float, action: str):
    end = time.time()
    print(f"{action} durou {end - start_time:.2f} segundos")

def get_column_mapping(mapping_file):
    return pd.read_csv(mapping_file)

def map_column_names(df, mapping_file):
    csv_mapping = get_column_mapping(mapping_file)

    column_mapping = dict(zip(csv_mapping['original_name'], csv_mapping['new_name']))
    not_mapped_cols =  set(df.columns) - set(column_mapping.keys())
    if not_mapped_cols:
        print('Update column_mapping.csv')
        raise ValueError(f"Mapping of columns: {not_mapped_cols}")

    return df.rename(columns=column_mapping)


def unzip_file(zip_path: Path):
    if zip_path.exists():

        if not zip_path.is_file():
            raise FileNotFoundError(f"Zip file does not exist: {zip_path}")

        extract_to = zip_path.parent / zip_path.stem  # Default: ./zipname/

        if extract_to.is_dir():
            print(f"{extract_to} folder already exists. skipping extraction.")
            zip_path.unlink()
            return

        extract_to.mkdir(parents=True, exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, 'r') as zipf:
                zipf.extractall(extract_to)
            print(f'Extracted {zip_path} to {extract_to}')

            os.remove(zip_path)
        except Exception as e:
            print('Failed to extract zip file:', e)


def zip_file(source_path: Path):
    if not source_path.exists():
        raise FileNotFoundError(f"Path does not exist: {source_path}")

    # Set output zip path to be next to source with .zip extension
    zip_file = source_path.with_suffix('.zip')

    # Create the zip file
    print(f'Zipping {source_path}')
    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        if source_path.is_dir():
            for file in source_path.rglob('*'):
                if file.is_file():
                    # Preserve folder structure
                    arcname = file.relative_to(source_path.parent)
                    zipf.write(file, arcname)
                    source_path.rmdir()
        else:
            # Zipping a single file
            zipf.write(source_path, arcname=source_path.name)
            source_path.unlink()

    print(f"Zipped {source_path} -> {zip_file}")


