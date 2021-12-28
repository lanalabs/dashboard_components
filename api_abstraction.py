import pylana
import json
from app import config

def create_connection(auth_token, port=None):
    scheme = config["scheme"]
    host = config["host"]

    if scheme != "https":
        port = port if port else 4000
        api = pylana.create_api(**{
            "scheme": scheme,
            "host": host,
            "port": port,
            "token": auth_token
        })
        return api
    else:
        api = pylana.create_api(**{
            "scheme": scheme,
            "host": host,
            "token": auth_token
        })
        return api

def aggregate(auth_token, trace_filter_sequence, **kwargs):
    trace_filter_sequence = json.loads(trace_filter_sequence) if trace_filter_sequence else []

    api = create_connection(auth_token)
    df = api.aggregate(trace_filter_sequence=trace_filter_sequence, **kwargs)
    return df

def variant_id_list_to_tfs(variant_ids: list) -> list:
    return [{"type": "variantFilter", "variantIds": variant_ids}]
