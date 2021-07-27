import pylana
import json
import yaml


with open("config.yml") as f:
    config = yaml.safe_load(f)

def create_connection(api_key, port=None):
    scheme = config["scheme"]
    host = config["host"]

    if api_key[:3] == "API":
        api_key = api_key[8:]
    if scheme != "https":
        port = port if port else 4000
        api = pylana.create_api(**{
            "scheme": scheme,
            "host": host,
            "port": port,
            "token": api_key
        })
        return api
    else:
        api = pylana.create_api(**{
            "scheme": scheme,
            "host": host,
            "token": api_key
        })
        return api


def aggregate(api_key, trace_filter_sequence, **kwargs):
    trace_filter_sequence = json.loads(trace_filter_sequence) if trace_filter_sequence else []

    api = create_connection(api_key)
    df = api.aggregate(trace_filter_sequence=trace_filter_sequence, **kwargs)
    return df

def variant_id_list_to_tfs(variant_ids: list) -> list:
    return [{"type": "variantFilter", "variantIds": variant_ids}]
