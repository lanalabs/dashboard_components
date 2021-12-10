import pylana
import json
from app import config


def create_connection(api_key, port=None):
    scheme = config["scheme"]
    host = config["host"]
    auth_token = parse_to_auth_token(api_key)

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

def parse_to_auth_token(api_key):
    if api_key[:3] == "API":
        return(api_key[8:])
    else:
        return(api_key)

def aggregate(api_key, trace_filter_sequence, **kwargs):
    trace_filter_sequence = json.loads(trace_filter_sequence) if trace_filter_sequence else []

    api = create_connection(api_key)
    df = api.aggregate(trace_filter_sequence=trace_filter_sequence, **kwargs)
    return df

def variant_id_list_to_tfs(variant_ids: list) -> list:
    return [{"type": "variantFilter", "variantIds": variant_ids}]
