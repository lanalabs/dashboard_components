import requests
import pandas as pd
import json

from app import config


def get_log_attributes(log_id, api_key) -> pd.DataFrame:
    """Returns a Dataframe containing on log attributes (case and activity) and
       their datatypes.
       TODO: This functionality is coming to Pylana."""

    headers = {'accept': 'application/json',
               'API-Key': api_key,
               }
    url = f'{config["url"]}/api/v2/logs/{log_id}/attribute-types'
    response = requests.get(url, headers=headers)

    return pd.DataFrame(response.json())


def get_log_activities(log_id, api_key, tfs=[]) -> pd.DataFrame:
    """
    Returns dataframe with frequency and median duration of all activities.
    ["frequency", "activityName", "medianDuration"]

    TODO: This functionality is coming to Pylana.
    median_duration = aggregate(api_key=api_key,
                            log_id=log_id,
                            trace_filter_sequence=tfs,
                            metric="duration",
                            aggregation_function="median",
                            grouping="byActivity",
                            activities=activities,
                            values_from="allEvents")
    """

    trace_filter_sequence = json.loads(tfs) if tfs else []

    aggregation_post = {
        'miningRequest': {'logId': log_id,
                          'traceFilterSequence': trace_filter_sequence},
        'valuesFrom': {
            'type': 'allEvents'
        },
        'metric': {
            'type': 'duration',
            'aggregationFunction': "median"
        },
        "grouping": {
            "type": "byActivity"
        },
        "options": {"maxAmountAttributes": 10000}
    }

    r = requests.post(
        f'{config["url"]}/api/v2/aggregate-data',
        headers={'API-Key': api_key},
        json=aggregation_post
    )
    df = pd.DataFrame(r.json()["chartValues"])
    df = df[["caseCount", "xAxis", "yAxis"]]
    df = ["frequency", "activityName", "medianDuration"]

    return df
