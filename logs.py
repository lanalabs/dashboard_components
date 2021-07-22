import requests
import pandas as pd


def get_log_attributes(log_id, api_key) -> pd.DataFrame:
    """Returns a Dataframe containing on log attributes (case and activity) and
       their datatypes.
       TODO: his functionality is coming to Pylana"""

    headers = {'accept': 'application/json',
               'API-Key': api_key,
               }
    url = f'https://cloud-backend.lanalabs.com/api/v2/logs/{log_id}/attribute-types'
    response = requests.get(url, headers=headers)

    return pd.DataFrame(response.json())
