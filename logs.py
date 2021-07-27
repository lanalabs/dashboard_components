import requests
import pandas as pd
import json
from collections import defaultdict
from functools import lru_cache

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


lru_cache(maxsize=5)
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
    df.columns = ["frequency", "activityName", "medianDuration"]

    return df

@lru_cache(maxsize=5)
def get_variants_per_activity(log_id, api_key, tfs=[]) -> pd.DataFrame:
    """Returns pd Dataframe with columns: [activity, numTraces, traces].
       This creates a linkage between activities and which traces they occur in."""

    if api_key[:3] == "API":
        api_key = api_key[8:]
    headers = {
        'accept': 'application/json',
        'API-Key': api_key,
        'Content-Type': 'application/json',
    }
    body = {"includeHeader": True,
            "includeLogId": True,
            "logId": log_id,
            "runConformance": True,
            "sort": "",
            "limit": 10000,
            "traceFilterSequence": json.loads(tfs) if tfs else []
            }
    print("before response")
    response = requests.post(f'{config["url"]}/api/v2/mining/discover-variants',
                             headers=headers, json=body)

    # parsing and processing the result into a pandas dataframe
    print(response)
    variants = response.json()["variants"]
    occurences = defaultdict(list)
    num_traces = defaultdict(int)
    for v in variants:
        variantId = v['variantId']
        sum_traces = v["statistics"]["numTraces"]
        for event in v["trace"]:
            event_name = event["actName"]
            occurences[event_name].append(variantId)
            num_traces[event_name] += sum_traces

    for key, val in occurences.items():
        occurences[key] = set(val)

    a = pd.DataFrame(num_traces.items(), columns=["activity", "numTraces"])
    b = pd.DataFrame(occurences.items(), columns=["activity", "traces"])
    res = pd.merge(left=a, right=b, on="activity")

    return res
