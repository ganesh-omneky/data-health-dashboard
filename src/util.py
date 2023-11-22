import datetime
from typing import List

from src.model import AdvertisementChannel, Status
from src.sql import sql_manager


def get_brand_status(brand_id: int, channel: AdvertisementChannel) -> dict:
    return {
        "airbyte_status": _get_airbyte_status(brand_id, channel),
        "otl_status": _get_otl_status(brand_id, channel),
    }


def get_active_brands() -> List[int]:
    return sql_manager.get_active_brands()


def get_brand_details(brand_id: int):
    return sql_manager.get_brand_details(brand_id)


def _get_airbyte_status(brand_id: int, channel: AdvertisementChannel) -> dict:
    return {
        "status": "OK",
        "message": "Airbyte is up and running",
        "channel": channel.value,
        "brand_id": brand_id,
    }


def _get_otl_status(brand_id: int, channel: AdvertisementChannel) -> dict:
    today = datetime.datetime.now()
    last_insight_in_db = sql_manager.get_min_date_for_brand(brand_id, channel)
    if last_insight_in_db is None:
        return {
            "status": Status.UNKNOWN,
            "message": "No insights in DB",
            "channel": channel.name,
            "brand_id": brand_id,
        }

    elif last_insight_in_db < today - datetime.timedelta(
        days=1
    ) and last_insight_in_db > today - datetime.timedelta(days=2):
        return {
            "status": Status.WARNING,
            "message": f"last insight in DB: {last_insight_in_db}",
            "channel": channel.name,
            "brand_id": brand_id,
        }
    elif last_insight_in_db < today - datetime.timedelta(days=2):
        return {
            "status": Status.FAILED,
            "message": f"last insight in DB: {last_insight_in_db}",
            "channel": channel.name,
            "brand_id": brand_id,
        }
    else:
        return {
            "status": "OK",
            "message": f"last insight in DB: {last_insight_in_db}",
            "channel": channel.name,
            "brand_id": brand_id,
        }
