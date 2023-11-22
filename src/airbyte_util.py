import requests

from src.model import AdvertisementChannel
from src.secrets_manager import get_secret


def get_airbyte_sync_status(brand_id: int, channel: AdvertisementChannel):
    endpoint = _get_endpoint_for_channel(channel)
    url = f"{endpoint}/"


def _get_endpoint_for_channel(channel: AdvertisementChannel):
    if channel == AdvertisementChannel.GOOGLE_ADS:
        return get_secret("GOOGLE_ADS_AIRBYTE_ENDPOINT")
    elif channel == AdvertisementChannel.FACEBOOK:
        return get_secret("FACEBOOK_AIRBYTE_ENDPOINT")
    else:
        raise Exception(f"Unknown channel: {channel}")
