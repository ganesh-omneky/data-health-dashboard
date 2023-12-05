import base64

import requests

from src.model import AdvertisementChannel
from src.secrets_manager import get_secret


def get_airbyte_sync_status(brand_id: int, channel: AdvertisementChannel):
    endpoint = _get_endpoint_for_channel(channel)
    url = f"{endpoint}/api/v1/connections/list"
    headers = {"Authorization": _get_headers()}
    request_body = {
        "workspaceId": _get_workspace_id_for_channel(channel),
    }
    response = requests.post(url, headers=headers, json=request_body)
    response = response.json()
    for conn in response["connections"]:
        conn_name = conn["name"]
        brand_id_from_conn_name = int(conn_name.split("_")[1])
        if brand_id_from_conn_name == brand_id:
            pass


def _get_endpoint_for_channel(channel: AdvertisementChannel):
    if channel == AdvertisementChannel.GOOGLE_ADS:
        return get_secret("GOOGLE_ADS_AIRBYTE_ENDPOINT")
    elif channel == AdvertisementChannel.FACEBOOK:
        return get_secret("FACEBOOK_AIRBYTE_ENDPOINT")
    else:
        raise Exception(f"Unknown channel: {channel}")


def _get_workspace_id_for_channel(channel: AdvertisementChannel):
    if channel == AdvertisementChannel.GOOGLE_ADS:
        return get_secret("GOOGLE_ADS_WORKSPACE_ID")
    elif channel == AdvertisementChannel.FACEBOOK:
        return get_secret("FACEBOOK_WORKSPACE_ID")
    else:
        raise Exception(f"Unknown channel: {channel}")


def _get_headers():
    username = get_secret("AIRBYTE_USERNAME")
    password = get_secret("AIRBYTE_PASSWORD")
    auth_str = f"{username}:{password}"
    auth_bytes = auth_str.encode("ascii")
    base64_bytes = base64.b64encode(auth_bytes)
    base64_str = base64_bytes.decode("ascii")
    return f"Basic {base64_str}"
