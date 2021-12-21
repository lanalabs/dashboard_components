import pylana
import json
from app import config

def aggregate(api_key, trace_filter_sequence, **kwargs):
    trace_filter_sequence = json.loads(trace_filter_sequence) if trace_filter_sequence else []

    api = create_connection(api_key)
    df = api.aggregate(trace_filter_sequence=trace_filter_sequence, **kwargs)
    return df

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
