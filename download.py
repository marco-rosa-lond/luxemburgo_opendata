

import requests, urllib.parse, os.path, re

API = 'https://data.public.lu/api/1'

# the id of the Dataset you want to get
datasets = {
    'operations-delta-des-vehicules-au-luxembourg' : 'Operations_Delta',
    'parc-automobile-du-luxembourg' : 'Parc_Automobile'
}



def download_all_files(resources, destination_folder):

    for res in resources:
        file_path = os.path.join(destination_folder, res['title'])

        if not os.path.exists(file_path):
            print('Downloading: ' + res['title'])

            s = requests.get(res['url'])
            s.raise_for_status()

            with open(os.path.join(destination_folder, res['title']), 'wb') as f:
                f.write(s.content)
        else:
            print('File already exists: ' + res['title'])




def get_monthly_xlsx_files():

    for resource, filename_prefix in datasets.items():

        fPattern = fr'^{filename_prefix}_\d{{6}}\.xlsx$'

        destination_folder = 'Data/{}/'.format(filename_prefix)
        os.makedirs(destination_folder, exist_ok=True)


        r = requests.get('{}/datasets/{}/'.format(API, resource))
        r.raise_for_status()

        resources = r.json()['resources']
        resources = list(filter(lambda x: re.search(fPattern, x['title']), resources))
        resources.sort(key=lambda x: x['published'], reverse=True)



        # print('files available in the dataset with id "{}" and matching the pattern /{}/:'.format(resource, fPattern))
        # print(list(map(lambda x: x['title'], resources)))

        if not resources:
            raise Exception('No resources found')


        download_all_files(resources, destination_folder)



