import hashlib
import json
import os
from typing import Dict, Iterable, List, Optional

import sqlalchemy
from cachetools import LRUCache, TLRUCache, TTLCache, cached
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.logging import get_logger
from src.model import AdvertisementChannel
from src.sql.engine import get_engine
from src.sql.tables import *

logger = get_logger(__name__)


@cached(cache=TTLCache(maxsize=100, ttl=60 * 60))
def get_insights_stats():
    insight_tables = [
        DailyInsights,
        TextAssetInsights,
        VideoAssetInsights,
        ImageAssetInsights,
    ]

    engine = get_engine()
    statistics = {}
    with Session(engine) as session:
        for table in insight_tables:
            stmt = (
                sqlalchemy.select(
                    Brands.id,
                    Brands.name,
                    PlatformInfo.platform_id,
                    func.max(table.date).label(f"latest_{table.__tablename__}_date"),
                )
                .join(PlatformInfo, Brands.id == PlatformInfo.brand_id)
                .outerjoin(table, PlatformInfo.id == table.platform_info_id)
                .where(Brands.is_active.is_(True))
                .where(PlatformInfo.deleted_at.is_(None))
                .group_by(Brands.id, PlatformInfo.platform_id)
            )
            logger.info("SQL: %s", str(stmt))
            start_time = datetime.datetime.now()
            result = session.execute(stmt).fetchall()
            end_time = datetime.datetime.now()
            logger.info(
                f"Table {table.__tablename__}, Query took {end_time - start_time}"
            )

            processed_result = []
            if result:
                for row in result:
                    row = row._asdict()
                    row["platform"] = AdvertisementChannel(row["platform_id"]).name
                    key = f'{row["id"]}|{row["platform_id"]}'
                    processed_result.append(row)
                    if key not in statistics:
                        statistics[key] = []
                    statistics[key].append(row)

        unified_list = []
        for key, val_list in statistics.items():
            unified_entry = {
                "brand_id": val_list[0]["id"],
                "brand_name": val_list[0]["name"],
                "platform": val_list[0]["platform"],
            }
            for val in val_list:
                for k in val:
                    if "date" in k:
                        unified_entry[k] = (
                            val[k].strftime("%Y-%m-%d") if val[k] else "NULL"
                        )
            unified_list.append(unified_entry)
        return unified_list


def get_all_brand_ids():
    engine = get_engine()
    with Session(engine) as session:
        stmt = sqlalchemy.select(Brands.id).where(Brands.is_active == True)
        result = session.scalars(stmt).all()
        return result


def get_platform_infos_for_brand(brand_id: int):
    engine = get_engine()
    with Session(engine) as session:
        stmt = sqlalchemy.select(PlatformInfo.id, PlatformInfo.platform_id).where(
            PlatformInfo.brand_id == brand_id
        )
        result = session.execute(stmt).fetchall()
        return result


def get_last_import_stats(platform_info_id: int):
    engine = get_engine()
    with Session(engine) as session:
        insight_tables = [
            DailyInsights,
            ImageAssetInsights,
            VideoAssetInsights,
            TextAssetInsights,
        ]

        entity_tables = [
            Ads,
            AdGroups,
            Campaigns,
        ]

        asset_tables = [
            ImageAsset,
            VideoAsset,
            TextAsset,
        ]
        subq = []
        for table in insight_tables:
            stmt = (
                sqlalchemy.select(func.max(table.date))
                .where(table.platform_info_id == platform_info_id)
                .scalar_subquery()
                .label(table.__name__)
            )
            subq.append(stmt)

        for table in entity_tables:
            stmt = (
                sqlalchemy.select(func.max(table.updated_at))
                .where(table.platform_info_id == platform_info_id)
                .scalar_subquery()
                .label(table.__name__)
            )
            subq.append(stmt)

        for table in asset_tables:
            ads_stmt = sqlalchemy.select(Ads.id).where(
                Ads.platform_info_id == platform_info_id
            )
            stmt = (
                sqlalchemy.select(func.max(table.updated_at))
                .where(table.ad_id.in_(ads_stmt))
                .scalar_subquery()
                .label(table.__name__)
            )
            subq.append(stmt)

        stmt = sqlalchemy.select(*subq)
        result = session.execute(stmt).first()

        return dict(result._mapping)


def get_import_stats(
    brand_id: int, channel: AdvertisementChannel, import_date: datetime.date
):
    engine = get_engine()

    with Session(engine) as session:
        platform_info_id = get_platform_info_id(
            brand_id=brand_id, platform_id=channel.value
        )

        stats = {}
        insight_tables = [
            DailyInsights,
            ImageAssetInsights,
            VideoAssetInsights,
            TextAssetInsights,
        ]

        entity_tables = [
            Ads,
            AdGroups,
            Campaigns,
            ImageAsset,
            VideoAsset,
            TextAsset,
        ]

        for table in insight_tables:
            stmt = (
                sqlalchemy.select(func.count())
                .where(table.platform_info_id == platform_info_id)
                .where(table.date == import_date)
            )
            result = session.scalar(stmt)
            logger.info(f"{table.__name__} count: {result}")
            stats[table.__name__] = result

        for table in entity_tables:
            stmt = (
                sqlalchemy.select(func.count())
                .where(table.platform_info_id == platform_info_id)
                .where(table.created_at == import_date)
            )
            created = session.scalar(stmt)
            stmt = (
                sqlalchemy.select(func.count())
                .where(table.platform_info_id == platform_info_id)
                .where(table.updated_at == import_date)
            )
            updated = session.scalar(stmt) - created
            logger.info(f"{table.__name__} count: {result}")
            stats[table.__name__] = {
                "created": created,
                "updated": updated,
            }

        return stats
