import json
import os
import requests
import pandas as pd

# Global Constants used throughout script
CMS_API_URL_FORMAT = "https://data.cms.gov/data-api/v1/dataset/{}/data"
CONFIG_FILE = "data_generator_config.json"
ROOT = "data"

with open(CONFIG_FILE, 'r') as file:
    config = json.load(file)

main_frames = []

for name, info in config.items():
    
    print(f"Beginning loading of {name}")
    dataframes = []

    # Each UUID represents an individual year of this dataset
    # We shall merge them all together
    for year, uuid in info['UUID'].items():
        request_url = CMS_API_URL_FORMAT.format(uuid)

        print(f"Requesting {uuid}")
        r = requests.get(request_url)
        tmp = pd.read_json(r.text)
        tmp['year'] = year
        dataframes.append(tmp)

    data = pd.concat(dataframes)
    main_frames.append(data)

    file_name = os.path.join(ROOT, name + '.csv')
    
    print(f"Saving aggregated data of {name} into {file_name}")
    data.to_csv(file_name)
