import hashlib
import json
from typing import Dict, Iterable, List, Optional

import sqlalchemy
from cachetools import LRUCache, TTLCache, cached
from sqlalchemy import func
from sqlalchemy.orm import Session

from src import configuration
from src.logging import get_logger
from src.model import AdvertisementChannel
from src.sql.engine import get_engine
from src.sql.tables import *

logger = get_logger(__name__)


def get_active_brands() -> List[int]:
    engine = get_engine()
    with Session(engine) as session:
        brand_info = {}
        stmt = sqlalchemy.select(Brands.id, Brands.name).where(Brands.deleted_at.is_(None))
        brand_result = session.execute(stmt).fetchone()
        if not brand_result:
            return None
        brand_info["brand_id"] = brand_result[0]
        brand_info["brand_name"] = brand_result[1]
        
        table_rows = {}
        stmt = sqlalchemy.select(PlatformInfo.platform_id, PlatformInfo.id, PlatformInfo.account_id).where(
            PlatformInfo.brand_id == brand_info["id"]
        ).where(PlatformInfo.deleted_at.is_(None))
        platform_result = session.execute(stmt).fetchall()
        for row in platform_result:
            table_rows.append({
                'brand_id': brand_info["id"],
                'brand_name': brand_info["name"],
                'platform_id': row[0],
                'platform_info_id': row[1],
                'account_id': row[2],
                'airbyte_status': 0,
                'otl_status': 0,
            })
        return table_rows


    
def get_brand_details(brand_id: int):
    engine = get_engine()
    with Session(engine) as session:
        stmt = (
            sqlalchemy.select(Brands).where(Brands.id == brand_id)
        )
        logger.debug("SQL: %s", str(stmt))
        return session.scalars(stmt).one_or_none()
    
def get_all_brand_ids(ad_channel: AdvertisementChannel):
    engine = get_engine()
    with Session(engine) as session:
        stmt = (
            sqlalchemy.select(PlatformInfo.brand_id)
            .where(PlatformInfo.platform_id == ad_channel.value)
            .where(PlatformInfo.deleted_at.is_(None))
        )
        logger.debug("SQL: %s", str(stmt))
        return session.scalars(stmt).all()


def get_account_id_for_brand(
    channel_name: str, brand_id: str
) -> Dict[AdvertisementChannel, str]:
    logger.info(f"Getting account id for brand {brand_id}, channel {channel_name}")
    engine = get_engine()
    platform_account_ids = {}

    channel = AdvertisementChannel.get_channel_for_name(channel_name)

    try:
        with Session(engine) as session:
            stmt = (
                sqlalchemy.select(PlatformInfo.platform_id, PlatformInfo.account_id)
                .where(PlatformInfo.brand_id == brand_id)
                .where(PlatformInfo.deleted_at.is_(None))
            )
            if channel != AdvertisementChannel.OMNICHANNEL:
                stmt = stmt.where(PlatformInfo.platform_id == channel.value)
            logger.debug("SQL: %s", str(stmt))
            result = session.execute(stmt).fetchall()
            for platform_id, account_id in result:
                channel = AdvertisementChannel(platform_id)
                logger.info(f"found channel {channel} with account id {account_id}")
                platform_account_ids[channel] = account_id

        logger.info("account ids: %s", platform_account_ids)
        return platform_account_ids
    except Exception as e:
        logger.error(
            f"Error getting account id for brand {brand_id}, channel {channel.name}: {e}"
        )
        raise e


def get_platform_info_id(
    brand_id: int = -1, platform_id: int = -1, account_id: str = None
):
    engine = get_engine()
    with Session(engine) as session:
        stmt = sqlalchemy.select(PlatformInfo.id).where(
            PlatformInfo.deleted_at.is_(None)
        )

        if brand_id != -1:
            stmt = stmt.where(PlatformInfo.brand_id == brand_id)

        if account_id:
            stmt = stmt.where(PlatformInfo.account_id == account_id)

        if platform_id != -1:
            stmt = stmt.where(PlatformInfo.platform_id == platform_id)

        logger.debug("SQL: %s", str(stmt))

        platform_info_id = session.scalar(stmt)
        if platform_info_id is None:
            logger.error(
                f"No platform info found for brand id {brand_id}, platform id {platform_id}"
            )
            return None

        return platform_info_id


def get_campaign_id(platform_info_id: int, platform_campaign_id: str) -> Optional[Dict]:
    engine = get_engine()
    with Session(engine) as session:
        stmt = (
            sqlalchemy.select(Campaigns.id)
            .where(Campaigns.platform_info_id == platform_info_id)
            .where(Campaigns.platform_campaign_id == platform_campaign_id)
        )
        return session.scalar(stmt)


def get_adset_id(platform_info_id: int, platform_ad_set_id: str) -> Optional[Dict]:
    engine = get_engine()
    with Session(engine) as session:
        stmt = (
            sqlalchemy.select(AdGroups.id)
            .where(AdGroups.platform_info_id == platform_info_id)
            .where(AdGroups.platform_ad_group_id == platform_ad_set_id)
        )
        return session.scalar(stmt)


def get_creative(platform_info_id, platform_creative_id):
    engine = get_engine()
    with Session(engine) as session:
        stmt = (
            sqlalchemy.select(AdCreatives)
            .where(AdCreatives.platform_info_id == platform_info_id)
            .where(AdCreatives.platform_ad_creative_id == platform_creative_id)
        )

        result = session.scalar(stmt)
        if result:
            return result.as_dict()
        else:
            return None


def get_ad_ids(platform_info_id, creative_ids):
    if not creative_ids or len(creative_ids) == 0:
        return []
    engine = get_engine()
    with Session(engine) as session:
        stmt = (
            sqlalchemy.select(Ads.id)
            .where(Ads.platform_info_id == platform_info_id)
            .where(Ads.ad_creative_id.in_(creative_ids))
        )

        return session.scalars(stmt).all()


def get_ad_ids_for_video_id(platform_info_id, video_id):
    if not video_id:
        return []
    video_ids_by_ad_creative_id = _extract_video_ids_from_creatives(platform_info_id)
    video_id_to_ad_creative_id = _invert_video_id_dict(video_ids_by_ad_creative_id)
    ad_creatives = video_id_to_ad_creative_id.get(video_id, [])
    return get_ad_ids(platform_info_id, ad_creatives)


def get_video_ids_for_account_id(
    account_id, channel: AdvertisementChannel = AdvertisementChannel.FACEBOOK
):
    platform_info_id = get_platform_info_id(
        account_id=account_id, platform_id=channel.value
    )
    creative_to_video_id = _extract_video_ids_from_creatives(platform_info_id)
    video_ids = []
    for video_id_list in creative_to_video_id.values():
        video_ids.extend(video_id_list)

    return set(video_ids)


def get_image_ids_for_account_id(account_id, channel: AdvertisementChannel.FACEBOOK):
    platform_info_id = get_platform_info_id(
        account_id=account_id, platform_id=channel.value
    )
    creative_to_image_id = _extract_image_ids_from_creatives(platform_info_id)
    image_ids = []
    for image_id_list in creative_to_image_id.values():
        image_ids.extend(image_id_list)

    return set(image_ids), _invert_image_id_dict(creative_to_image_id)


def get_access_token(account_id: str, platform: AdvertisementChannel):
    engine = get_engine()
    with Session(engine) as session:
        stmt = (
            sqlalchemy.select(PlatformInfo.token1)
            .where(PlatformInfo.account_id == account_id)
            .where(PlatformInfo.platform_id == platform.value)
            .where(PlatformInfo.deleted_at.is_(None))
        )
        logger.debug("SQL: %s", str(stmt))
        return session.scalar(stmt)


def get_unprocessed_image_sources(brand_id, ad_platform):
    engine = get_engine()
    with Session(engine) as session:
        platform_info_id = get_platform_info_id(
            brand_id=brand_id, platform_id=ad_platform.value
        )
        ad_ids_stmt = sqlalchemy.select(Ads.id).where(
            Ads.platform_info_id == platform_info_id
        )
        stmt = (
            sqlalchemy.select(ImageAsset.platform_asset_id, ImageAsset.source)
            .where(sqlalchemy.not_(ImageAsset.source.startswith(configuration.S3_URL)))
            .where(ImageAsset.ad_id.in_(ad_ids_stmt))
        )
        logger.debug("SQL: %s", str(stmt))
        result = session.execute(stmt).fetchall()
        asset_id_to_source = {}
        for platform_asset_id, source in result:
            asset_id_to_source[platform_asset_id] = source
        return asset_id_to_source


def get_unprocessed_video_sources(brand_id, ad_platform):
    engine = get_engine()
    with Session(engine) as session:
        platform_info_id = get_platform_info_id(
            brand_id=brand_id, platform_id=ad_platform.value
        )
        ad_ids_stmt = sqlalchemy.select(Ads.id).where(
            Ads.platform_info_id == platform_info_id
        )
        stmt = (
            sqlalchemy.select(VideoAsset.platform_asset_id, VideoAsset.source)
            .where(sqlalchemy.not_(VideoAsset.source.startswith(configuration.S3_URL)))
            .where(VideoAsset.ad_id.in_(ad_ids_stmt))
        )
        logger.debug("SQL: %s", str(stmt))
        result = session.execute(stmt).fetchall()
        asset_id_to_source = {}
        for platform_asset_id, source in result:
            asset_id_to_source[platform_asset_id] = source
        return asset_id_to_source


def get_image_sources_for_blip2(brand_id, ad_platform):
    engine = get_engine()
    with Session(engine) as session:
        platform_info_id = get_platform_info_id(
            brand_id=brand_id, platform_id=ad_platform.value
        )
        ads_stmt = sqlalchemy.select(Ads.id).where(
            Ads.platform_info_id == platform_info_id
        )

        stmt = (
            sqlalchemy.select(ImageAsset.platform_asset_id, ImageAsset.source)
            .where(ImageAsset.blip2_image_desc.is_(None))
            .where(ImageAsset.ad_id.in_(ads_stmt))
        )
        logger.debug("SQL: %s", str(stmt))
        result = session.execute(stmt).fetchall()
        asset_id_to_source = {}
        for platform_asset_id, source in result:
            asset_id_to_source[platform_asset_id] = source

        return asset_id_to_source


def get_video_sources_for_audio_detection(brand_id, ad_platform):
    engine = get_engine()
    with Session(engine) as session:
        platform_info_id = get_platform_info_id(
            brand_id=brand_id, platform_id=ad_platform.value
        )
        ad_ids = sqlalchemy.select(Ads.id).where(
            Ads.platform_info_id == platform_info_id
        )
        stmt = (
            sqlalchemy.select(VideoAsset.platform_asset_id, VideoAsset.source)
            .where(VideoAsset.is_audio_present.is_(None))
            .where(VideoAsset.ad_id.in_(ad_ids))
        )
        return session.execute(stmt).fetchall()


def get_image_assets(brand_id: int, ad_platform: AdvertisementChannel):
    engine = get_engine()
    platform_info_id = get_platform_info_id(
        brand_id=brand_id, platform_id=ad_platform.value
    )
    with Session(engine) as session:
        ad_ids = sqlalchemy.select(Ads.id).where(
            Ads.platform_info_id == platform_info_id
        )
        stmt = sqlalchemy.select(ImageAsset).where(ImageAsset.ad_id.in_(ad_ids))
        result = session.scalars(stmt).fetchall()
        return result


def get_video_assets(brand_id: int, ad_platform: AdvertisementChannel):
    engine = get_engine()
    platform_info_id = get_platform_info_id(
        brand_id=brand_id, platform_id=ad_platform.value
    )
    with Session(engine) as session:
        ad_ids = sqlalchemy.select(Ads.id).where(
            Ads.platform_info_id == platform_info_id
        )
        stmt = sqlalchemy.select(VideoAsset).where(VideoAsset.ad_id.in_(ad_ids))
        result = session.scalars(stmt).fetchall()
        return result


def get_text_assets(brand_id: int, ad_platform: AdvertisementChannel):
    engine = get_engine()
    platform_info_id = get_platform_info_id(
        brand_id=brand_id, platform_id=ad_platform.value
    )
    with Session(engine) as session:
        ad_ids = sqlalchemy.select(Ads.id).where(
            Ads.platform_info_id == platform_info_id
        )
        stmt = sqlalchemy.select(TextAsset).where(TextAsset.ad_id.in_(ad_ids))
        result = session.scalars(stmt).fetchall()
        return result


def get_latest_insight_date(
    account_id: str, channel: AdvertisementChannel
) -> datetime.date:
    engine = get_engine()
    platform_info_id = get_platform_info_id(account_id=account_id, platform_id=channel)
    with Session(engine) as session:
        platform_ad_id_stmt = sqlalchemy.select(Ads.platform_ad_id).where(
            Ads.platform_info_id == platform_info_id
        )
        ad_ids = session.scalars(platform_ad_id_stmt).fetchall()
        stmt = sqlalchemy.select(func.max(DailyInsights.date)).where(
            DailyInsights.platform_ad_id.in_(ad_ids)
        )
        latest_date = session.scalar(stmt)
        if type(latest_date) is datetime.datetime:
            latest_date = latest_date.date()
        return latest_date


def fetch_media_assets_for_account(
    account_id: str, asset_types: Optional[List[str]] = None
) -> Dict[str, List]:
    asset_types = asset_types or ["image", "video"]
    assets = {}
    engine = get_engine()
    with Session(engine) as session:
        for asset_type in asset_types:
            if asset_type == "images":
                table = ImageAsset
            elif asset_type == "videos":
                table = VideoAsset
            elif asset_type == "text":
                logger.warning("Text assets not supported for media asset fetch")
            else:
                logger.warning(f"Unknown asset type {asset_type}")
                continue

            stmt = (
                sqlalchemy.select(table.platform_asset_id, table.source)
                .join(Ads, table.ad_id == Ads.id)
                .join(PlatformInfo, Ads.platform_info_id == PlatformInfo.id)
                .where(PlatformInfo.account_id == account_id)
                .where(PlatformInfo.deleted_at.is_(None))
            )

            assets = session.execute(stmt).fetchall()
            assets[asset_type] = assets

    return assets


def get_assets_for_ad(
    platform_ad_id: str, account_id: str, channel: AdvertisementChannel
):
    engine = get_engine()
    if account_id is None:
        platform_info_id = None
    else:
        platform_info_id = get_platform_info_id(
            platform_id=channel.value, account_id=account_id
        )

    assets = {
        "text": [],
        "image": [],
        "video": [],
    }

    with Session(engine) as session:
        stmt = (
            sqlalchemy.select(
                TextAsset.platform_asset_id, TextAsset.type, TextAsset.text
            )
            .join(Ads, onclause=TextAsset.ad_id == Ads.id)
            .where(Ads.platform_ad_id == platform_ad_id)
            .where(Ads.platform_info_id == platform_info_id)
        )

        assets["text"] = session.execute(stmt).all()

        image_stmt = (
            sqlalchemy.select(ImageAsset.platform_asset_id)
            .join(Ads, onclause=ImageAsset.ad_id == Ads.id)
            .where(Ads.platform_ad_id == platform_ad_id)
            .where(Ads.platform_info_id == platform_info_id)
        )

        assets["image"] = session.execute(image_stmt).all()

        video_assets = (
            sqlalchemy.select(VideoAsset.platform_asset_id)
            .join(Ads, onclause=VideoAsset.ad_id == Ads.id)
            .where(Ads.platform_ad_id == platform_ad_id)
            .where(Ads.platform_info_id == platform_info_id)
        )

        assets["video"] = session.execute(video_assets).all()

        return assets


def get_imported_video_ids_for_brand(brand_id: int, channel: AdvertisementChannel):
    engine = get_engine()
    with Session(engine) as session:
        platform_info_id = get_platform_info_id(
            brand_id=brand_id, platform_id=channel.value
        )
        video_ids_stmt = sqlalchemy.select(VideoAsset.platform_asset_id).where(
            VideoAsset.ad_id.in_(
                sqlalchemy.select(Ads.id).where(
                    Ads.platform_info_id == platform_info_id
                )
            )
        )
        return session.scalars(video_ids_stmt).fetchall()


def get_min_date_for_brand(brand_id: int, channel: AdvertisementChannel):
    engine = get_engine()
    with Session(engine) as session:
        platform_info_id = get_platform_info_id(
            brand_id=brand_id, platform_id=channel.value
        )
        stmt = sqlalchemy.select(func.max(DailyInsights.date)).where(
            DailyInsights.platform_info_id == platform_info_id
        )
        try:
            return session.scalar(stmt)
        except Exception as e:
            logger.error(f"Error getting min date for brand {brand_id}: {e}")
            return None


def get_imported_image_ids_for_brand(brand_id: int, channel: AdvertisementChannel):
    engine = get_engine()
    with Session(engine) as session:
        platform_info_id = get_platform_info_id(
            brand_id=brand_id, platform_id=channel.value
        )
        image_ids_stmt = sqlalchemy.select(ImageAsset.platform_asset_id).where(
            ImageAsset.ad_id.in_(
                sqlalchemy.select(Ads.id).where(
                    Ads.platform_info_id == platform_info_id
                )
            )
        )
        return session.scalars(image_ids_stmt).fetchall()


def _extract_image_ids_from_creatives(platform_info_id: int):
    engine = get_engine()
    with Session(engine) as session:
        stmt = sqlalchemy.select(
            AdCreatives.platform_ad_creative_id,
            AdCreatives.image_hash,
            AdCreatives.object_story_spec_json,
            AdCreatives.asset_feed_spec_json,
        ).where(AdCreatives.platform_info_id == platform_info_id)
        result = session.execute(stmt).fetchall()
        img_ids_by_ad_creative_id = {}
        for ad_creative_id, img_id, oss_json_str, afs_json_str in result:
            oss_json = json.loads(oss_json_str) if oss_json_str else None
            while type(oss_json) == str:
                oss_json = json.loads(oss_json)

            oss_image_hash = oss_json.get("image_hash") if oss_json else None

            img_ids = [x for x in [img_id, oss_image_hash] if x is not None]
            if afs_json_str:
                afs_json = json.loads(afs_json_str)
                while type(afs_json) == str:
                    afs_json = json.loads(afs_json)

                if afs_json:
                    afs_images = afs_json.get("images")
                    if afs_images:
                        img_ids.extend([x["hash"] for x in afs_images if x is not None])

            img_ids = list(set(img_ids))
            img_ids_by_ad_creative_id[ad_creative_id] = img_ids
        return img_ids_by_ad_creative_id


def _extract_video_ids_from_creatives(platform_info_id: int):
    engine = get_engine()
    with Session(engine) as session:
        stmt = sqlalchemy.select(
            AdCreatives.platform_ad_creative_id,
            AdCreatives.video_id,
            AdCreatives.object_story_spec_json,
            AdCreatives.asset_feed_spec_json,
        ).where(AdCreatives.platform_info_id == platform_info_id)
        result = session.execute(stmt).fetchall()
        video_ids_by_ad_creative_id = {}
        for ad_creative_id, video_id, oss_json_str, afs_json_str in result:
            oss_json = json.loads(oss_json_str) if oss_json_str else None
            while type(oss_json) == str:
                oss_json = json.loads(oss_json)

            video_data = oss_json.get("video_data", {}) if oss_json else None
            oss_video_id = video_data.get("video_id") if video_data else None

            video_ids = [x for x in [video_id, oss_video_id] if x is not None]
            if afs_json_str:
                afs_json = json.loads(afs_json_str)
                while type(afs_json) == str:
                    afs_json = json.loads(afs_json)

                if afs_json:
                    afs_videos = afs_json.get("videos")
                    if afs_videos:
                        video_ids.extend(
                            [x["video_id"] for x in afs_videos if x is not None]
                        )
            video_ids = list(set(video_ids))
            video_ids_by_ad_creative_id[ad_creative_id] = video_ids

        return video_ids_by_ad_creative_id


def _invert_video_id_dict(video_ids_by_ad_creative_id):
    video_id_to_ad_creative_id = {}
    for ad_creative_id, video_ids in video_ids_by_ad_creative_id.items():
        for video_id in video_ids:
            if video_id not in video_id_to_ad_creative_id:
                video_id_to_ad_creative_id[video_id] = []
            video_id_to_ad_creative_id[video_id].append(ad_creative_id)
    return video_id_to_ad_creative_id


def _invert_image_id_dict(image_ids_by_ad_creative_id):
    image_id_to_ad_creative_id = {}
    for ad_creative_id, image_ids in image_ids_by_ad_creative_id.items():
        for image_id in image_ids:
            if image_id not in image_id_to_ad_creative_id:
                image_id_to_ad_creative_id[image_id] = []
            image_id_to_ad_creative_id[image_id].append(ad_creative_id)
    return image_id_to_ad_creative_id


# Write commands #


def upsert_ad_creative(ad_creative: dict, logging_prefix: str = None):
    my_logger = get_logger(__name__, logging_prefix)
    if not ad_creative.get("platform_ad_creative_id"):
        ad_creative["platform_ad_creative_id"] = hashlib.md5(
            json.dumps(ad_creative, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()

    engine = get_engine()
    with Session(engine) as session:
        # check if ad_creative already exists
        stmt = (
            sqlalchemy.select("*")
            .where(
                AdCreatives.platform_ad_creative_id
                == ad_creative["platform_ad_creative_id"]
            )
            .where(AdCreatives.platform_info_id == ad_creative["platform_info_id"])
        )
        result = session.execute(stmt).fetchone()
        if result:
            my_logger.debug(
                f'Ad creative {ad_creative["platform_ad_creative_id"]} already exists'
            )
            values_to_update = {}
            result_dict = result._asdict()
            for column in result_dict:
                if column not in ad_creative:
                    continue

                if column.endswith("_json"):
                    curr_val = ad_creative[column]
                    db_val = json.loads(result.__getattr__(column))
                else:
                    curr_val = ad_creative[column]
                    db_val = result.__getattr__(column)

                if curr_val != db_val:
                    if column.endswith("_json"):
                        values_to_update[column] = json.dumps(
                            curr_val, ensure_ascii=False
                        )
                    else:
                        values_to_update[column] = ad_creative[column]

            if len(values_to_update) > 0:
                my_logger.debug(
                    f'[platform_info_id={ad_creative["platform_info_id"]}] Updating creative {ad_creative["platform_ad_creative_id"]} with new values'
                )
                stmt = (
                    sqlalchemy.update(AdCreatives)
                    .where(
                        AdCreatives.platform_ad_creative_id
                        == ad_creative["platform_ad_creative_id"]
                    )
                    .values(values_to_update)
                )
                my_logger.debug("SQL: %s", str(stmt))
                session.execute(stmt)
                session.commit()
            return result.id
        else:
            my_logger.debug(
                f'Inserting ad creative {ad_creative["platform_ad_creative_id"]}'
            )

            ad_creative = AdCreatives(**ad_creative)
            session.add(ad_creative)
            session.commit()
            return ad_creative.id


def upsert_campaign(campaign: dict, logging_context: str = None):
    engine = get_engine()
    my_logger = get_logger(__name__, logging_context)
    with Session(engine) as session:
        # check if ad_creative already exists
        stmt = (
            sqlalchemy.select("*")
            .where(Campaigns.platform_campaign_id == campaign["platform_campaign_id"])
            .where(Campaigns.platform_info_id == campaign["platform_info_id"])
        )
        result = session.execute(stmt).fetchone()
        if result:
            my_logger.debug(
                f'Campaign {campaign["platform_campaign_id"]} already exists'
            )
            values_to_update = {}
            result_dict = result._asdict()
            for column in result_dict:
                if column in campaign and campaign[column] != result.__getattr__(
                    column
                ):
                    values_to_update[column] = campaign[column]

            if len(values_to_update) > 0:
                my_logger.debug(
                    f'Values to update for campaign {campaign["platform_campaign_id"]}: {values_to_update}'
                )
                my_logger.info(f'Updating campaign {campaign["platform_campaign_id"]}')
                stmt = (
                    sqlalchemy.update(Campaigns)
                    .where(
                        Campaigns.platform_campaign_id
                        == campaign["platform_campaign_id"]
                    )
                    .values(values_to_update)
                )
                my_logger.debug("SQL: %s", str(stmt))
                session.execute(stmt)
                session.commit()
        else:
            my_logger.debug(f'Inserting campaign {campaign["platform_campaign_id"]}')
            campaign = Campaigns(**campaign)
            session.add(campaign)
            session.commit()


def upsert_ad_set(ad_set: dict, logging_context: str = None):
    engine = get_engine()
    my_logger = get_logger(__name__, logging_context)
    with Session(engine) as session:
        # check if ad_creative already exists
        stmt = (
            sqlalchemy.select("*")
            .where(AdGroups.platform_ad_group_id == ad_set["platform_ad_group_id"])
            .where(AdGroups.platform_info_id == ad_set["platform_info_id"])
        )
        result = session.execute(stmt).fetchone()
        if result:
            my_logger.debug(f'AdGroup {ad_set["platform_ad_group_id"]} already exists')
            values_to_update = {}
            result_dict = result._asdict()
            for column in result_dict:
                if column in ad_set and ad_set[column] != result.__getattr__(column):
                    values_to_update[column] = ad_set[column]

            if len(values_to_update) > 0:
                my_logger.debug(
                    f'Values to update for ad group {ad_set["platform_ad_group_id"]}: {values_to_update}'
                )
                my_logger.info(f'Updating ad group {ad_set["platform_ad_group_id"]}')
                stmt = (
                    sqlalchemy.update(AdGroups)
                    .where(
                        AdGroups.platform_ad_group_id == ad_set["platform_ad_group_id"]
                    )
                    .where(AdGroups.platform_info_id == ad_set["platform_info_id"])
                    .values(values_to_update)
                )
                my_logger.debug("SQL: %s", str(stmt))
                session.execute(stmt)
                session.commit()
        else:
            my_logger.debug(f'Inserting ad_group {ad_set["platform_ad_group_id"]}')
            ad_set = AdGroups(**ad_set)
            session.add(ad_set)
            session.commit()


def upsert_ad(ad: dict, logging_context: str = None) -> int:
    engine = get_engine()
    my_logger = get_logger(__name__, logging_context)
    with Session(engine) as session:
        # check if ad already exists
        stmt = (
            sqlalchemy.select(Ads)
            .where(Ads.platform_ad_id == ad["platform_ad_id"])
            .where(Ads.platform_info_id == ad["platform_info_id"])
        )

        result = session.scalar(stmt)

        if result:
            my_logger.debug(f'Ad {ad["platform_ad_id"]} already exists')
            values_to_update = {}
            result_dict = result.as_dict()
            for column in result_dict:
                if column in ad and ad[column] != result.as_dict().get(column):
                    values_to_update[column] = ad[column]

            if len(values_to_update) > 0:
                my_logger.debug(
                    f'Values to update for ad {ad["platform_ad_id"]}: {values_to_update}'
                )
                stmt = (
                    sqlalchemy.update(Ads)
                    .where(Ads.platform_ad_id == ad["platform_ad_id"])
                    .where(Ads.platform_info_id == ad["platform_info_id"])
                    .values(values_to_update)
                )
                my_logger.debug("SQL: %s", str(stmt))
                session.execute(stmt)
                session.commit()

            return result.id
        else:
            my_logger.debug(f'Inserting ad_group {ad["platform_ad_id"]}')
            ad = Ads(**ad)
            session.add(ad)
            session.commit()
            return ad.id


def upsert_image_assets(image_asset_objs, logging_context: str = None):
    engine = get_engine()
    my_logger = get_logger(__name__, logging_context)
    with Session(engine) as session:
        for image_asset_obj in image_asset_objs:
            stmt = (
                sqlalchemy.select(ImageAsset)
                .where(
                    ImageAsset.platform_asset_id == image_asset_obj["platform_asset_id"]
                )
                .where(ImageAsset.ad_id == image_asset_obj["ad_id"])
            )
            my_logger.debug("SQL: %s", str(stmt))
            result = session.scalars(stmt).one_or_none()
            if result:
                my_logger.debug(
                    f'Image asset {image_asset_obj["platform_asset_id"]} already exists'
                )
                values_to_update = {}
                result_dict = result.as_dict()
                for column in result_dict:
                    if (
                        column in image_asset_obj
                        and image_asset_obj[column] != result_dict[column]
                    ):
                        if column == "source" and column.startswith(
                            configuration.S3_URL
                        ):
                            continue
                        values_to_update[column] = image_asset_obj[column]

                if len(values_to_update) > 0:
                    my_logger.debug(
                        f'Values to update for image asset {image_asset_obj["platform_asset_id"]}: {values_to_update}'
                    )
                    my_logger.info(
                        f'Updating image asset {image_asset_obj["platform_asset_id"]}'
                    )
                    stmt = (
                        sqlalchemy.update(ImageAsset)
                        .where(
                            ImageAsset.platform_asset_id
                            == image_asset_obj["platform_asset_id"]
                        )
                        .where(ImageAsset.ad_id == image_asset_obj["ad_id"])
                        .values(values_to_update)
                    )
                    logger.debug("SQL: %s", str(stmt))
                    session.execute(stmt)
                    session.commit()
                return result.id
            else:
                my_logger.info(
                    f'Inserting image asset {image_asset_obj["platform_asset_id"]}'
                )
                image_asset_obj = ImageAsset(**image_asset_obj)
                session.add(image_asset_obj)
                session.commit()
                return image_asset_obj.id


def upsert_video_assets(video_objs):
    engine = get_engine()
    with Session(engine) as session:
        for video_obj in video_objs:
            stmt = (
                sqlalchemy.select(VideoAsset.id)
                .where(VideoAsset.platform_asset_id == video_obj["platform_asset_id"])
                .where(VideoAsset.ad_id == video_obj["ad_id"])
            )
            logger.debug("SQL: %s", str(stmt))
            result = session.execute(stmt).fetchone()
            if result:
                logger.debug(
                    f'Video asset {video_obj["platform_asset_id"]} already exists'
                )
                values_to_update = {}
                result_dict = result._asdict()
                for column in result_dict:
                    if column in video_obj and video_obj[column] != result.__getattr__(
                        column
                    ):
                        values_to_update[column] = video_obj[column]

                if len(values_to_update) > 0:
                    logger.debug(
                        f'Values to update for video asset {video_obj["platform_asset_id"]}: {values_to_update}'
                    )
                    stmt = (
                        sqlalchemy.update(VideoAsset)
                        .where(
                            VideoAsset.platform_asset_id
                            == video_obj["platform_asset_id"]
                        )
                        .where(VideoAsset.ad_id == video_obj["ad_id"])
                        .values(values_to_update)
                    )
                    logger.debug("SQL: %s", str(stmt))
                    session.execute(stmt)
                    session.commit()
            else:
                logger.debug(f'Inserting video asset {video_obj["platform_asset_id"]}')
                video_obj = VideoAsset(**video_obj)
                session.add(video_obj)
                session.commit()


def upsert_insights(insights_obj):
    engine = get_engine()
    with Session(engine) as session:
        stmt = (
            sqlalchemy.select(DailyInsights)
            .where(DailyInsights.platform_ad_id == insights_obj["platform_ad_id"])
            .where(DailyInsights.date == insights_obj["date"])
        )
        result = session.scalar(stmt)
        if result:
            values_to_update = {}
            result_dict = result.as_dict()
            for column in result_dict:
                if column in ["date", "platform_ad_id"]:
                    continue

                if (
                    column in insights_obj
                    and insights_obj[column] != result_dict[column]
                ):
                    values_to_update[column] = insights_obj[column]

            if len(values_to_update) > 0:
                stmt = (
                    sqlalchemy.update(DailyInsights)
                    .where(
                        DailyInsights.platform_ad_id == insights_obj["platform_ad_id"]
                    )
                    .where(DailyInsights.date == insights_obj["date"])
                    .values(values_to_update)
                )
                logger.debug("SQL: %s", str(stmt))
                n_attempts = 0
                try:
                    session.execute(stmt)
                    session.commit()
                except Exception as e:
                    n_attempts += 1
                    logger.warning(f"Attempt {n_attempts + 1} failed with error {e}")
                    if n_attempts > 3:
                        logger.error(
                            f'Failed to update insights for {insights_obj["platform_ad_id"]}'
                        )
                        raise e
        else:
            logger.debug(f'Inserting insights for ad {insights_obj["platform_ad_id"]}')
            insights_obj = DailyInsights(**insights_obj)
            session.add(insights_obj)
            session.commit()


def upsert_asset_insights(breakdown: str, asset_insights_obj: dict):
    engine = get_engine()

    if breakdown == "image":
        table = ImageAssetInsights
    elif breakdown == "video":
        table = VideoAssetInsights
    else:
        table = TextAssetInsights

    with Session(engine) as session:
        stmt = (
            sqlalchemy.select(table.id)
            .where(table.platform_asset_id == asset_insights_obj["platform_asset_id"])
            .where(table.date == asset_insights_obj["date"])
        )
        logger.debug("SQL: %s", str(stmt))
        result = session.execute(stmt).fetchone()
        if result:
            logger.debug(
                f'Insights for {breakdown} {asset_insights_obj["platform_asset_id"]}|{asset_insights_obj["date"]} already exists'
            )
            values_to_update = {}
            result_dict = result._asdict()
            for column in result_dict:
                if column in asset_insights_obj and asset_insights_obj[
                    column
                ] != result.__getattr__(column):
                    values_to_update[column] = asset_insights_obj[column]

            if len(values_to_update) > 0:
                logger.debug(
                    f'Values to update for insights for {asset_insights_obj["platform_asset_id"]}: {values_to_update}'
                )
                stmt = (
                    sqlalchemy.update(table)
                    .where(
                        table.platform_asset_id
                        == asset_insights_obj["platform_asset_id"]
                    )
                    .where(table.date == asset_insights_obj["date"])
                    .values(values_to_update)
                )
                logger.debug("SQL: %s", str(stmt))
                session.execute(stmt)
                session.commit()
        else:
            logger.info(
                f'Inserting insights for {breakdown} {asset_insights_obj["platform_asset_id"]}|{asset_insights_obj["date"]}'
            )
            asset_insights_obj = table(**asset_insights_obj)
            session.add(asset_insights_obj)
            session.commit()


def upsert_text_assets(text_asset_objs: List[Dict]):
    engine = get_engine()
    text_asset_ids = []
    with Session(engine) as session:
        for text_asset in text_asset_objs:
            platform_ad_id = str(text_asset["platform_ad_id"])
            del text_asset["platform_ad_id"]
            text = text_asset["text"]
            text_type = text_asset["type"]

            platform_asset_id = hashlib.md5(
                f"{platform_ad_id}_{text}_{text_type}".encode("utf-8")
            ).hexdigest()
            text_asset["platform_asset_id"] = platform_asset_id

            stmt = sqlalchemy.select(TextAsset).where(
                TextAsset.platform_asset_id == platform_asset_id
            )
            logger.debug("SQL: %s", str(stmt))
            result = session.scalar(stmt)
            if result:
                logger.debug(f"Text asset {platform_asset_id} already exists")
                values_to_update = {}
                result_dict = result.as_dict()
                for column in result_dict:
                    if (
                        column in text_asset
                        and text_asset[column] != result_dict[column]
                    ):
                        values_to_update[column] = text_asset[column]

                if len(values_to_update) > 0:
                    logger.debug(
                        f'Values to update for text asset {text_asset["platform_asset_id"]}: {values_to_update}'
                    )
                    stmt = (
                        sqlalchemy.update(TextAsset)
                        .where(
                            TextAsset.platform_asset_id
                            == text_asset["platform_asset_id"]
                        )
                        .where(TextAsset.ad_id == text_asset["ad_id"])
                        .values(values_to_update)
                    )
                    logger.debug("SQL: %s", str(stmt))
                    session.execute(stmt)
                    session.commit()
                text_asset_ids.append(result.id)
            else:
                logger.debug(f'Inserting text asset {text_asset["platform_asset_id"]}')
                text_asset_obj = TextAsset(**text_asset)
                session.add(text_asset_obj)
                session.commit()
                text_asset_ids.append(text_asset_obj.id)

    return text_asset_ids


def update_source(brand_id, ad_platform, asset_id, media_type, s3_path):
    if media_type == "image":
        table = ImageAsset
    else:
        table = VideoAsset

    engine = get_engine()
    with Session(engine) as session:
        platform_info_id = get_platform_info_id(
            brand_id=brand_id, platform_id=ad_platform.value
        )
        stmt = (
            sqlalchemy.update(table)
            .where(table.platform_asset_id == asset_id)
            .where(table.platform_info_id == platform_info_id)
            .values(source=s3_path)
        )
        logger.debug("SQL: %s", str(stmt))
        session.execute(stmt)
        session.commit()


def update_blip2_description(brand_id, ad_platform, image_url, blip2_description):
    engine = get_engine()
    with Session(engine) as session:
        stmt = (
            sqlalchemy.update(ImageAsset)
            .where(ImageAsset.source == image_url)
            .values(blip2_image_desc=blip2_description)
        )
        logger.debug("SQL: %s", str(stmt))
        session.execute(stmt)
        session.commit()


def upsert_network_insights(insights_obj: dict):
    engine = get_engine()
    with Session(engine) as session:
        stmt = (
            sqlalchemy.select(NetworkInsights)
            .where(NetworkInsights.platform_ad_id == insights_obj["platform_ad_id"])
            .where(NetworkInsights.date == insights_obj["date"])
        )
        logger.debug("SQL: %s", str(stmt))
        result = session.execute(stmt).fetchone()
        if result:
            logger.debug(
                f'Network Insights for {insights_obj["platform_ad_id"]}|{insights_obj["date"]} already exists'
            )
            values_to_update = {}
            result_dict = result._asdict()
            for column in result_dict:
                if column in insights_obj and insights_obj[
                    column
                ] != result.__getattr__(column):
                    values_to_update[column] = insights_obj[column]

            if len(values_to_update) > 0:
                logger.debug(
                    f'Values to update for network insights for {insights_obj["platform_ad_id"]}: {values_to_update}'
                )
                stmt = (
                    sqlalchemy.update(NetworkInsights)
                    .where(
                        NetworkInsights.platform_ad_id == insights_obj["platform_ad_id"]
                    )
                    .where(NetworkInsights.date == insights_obj["date"])
                    .values(values_to_update)
                )
                logger.debug("SQL: %s", str(stmt))
                session.execute(stmt)
                session.commit()
        else:
            logger.info(
                f'Inserting network insights for {insights_obj["platform_ad_id"]}'
            )
            insights_obj = NetworkInsights(**insights_obj)
            session.add(insights_obj)
            session.commit()


def update_video_n_faces(video_source: str, n_faces: int):
    engine = get_engine()
    with Session(engine) as session:
        stmt = (
            sqlalchemy.update(VideoAsset)
            .where(VideoAsset.source == video_source)
            .values(no_of_faces=n_faces)
        )
        session.execute(stmt)
        session.commit()


# Script commands #
def script_get_common_ad_ids():
    engine = get_engine()
    stmt = (
        sqlalchemy.select(ImageAsset.ad_id, Ads.platform_ad_id, PlatformInfo.account_id)
        .join(Ads, onclause=ImageAsset.ad_id == Ads.id)
        .join(PlatformInfo, onclause=Ads.platform_info_id == PlatformInfo.id)
        .where(ImageAsset.ad_id.in_(sqlalchemy.select(VideoAsset.ad_id)))
        .where(PlatformInfo.deleted_at.is_(None))
        .where(PlatformInfo.platform_id == 1)
    )
    with Session(engine) as session:
        result = session.execute(stmt).fetchall()
        return result


def script_delete_image_asset_insights_from_sql(
    platform_ad_id: str, account_id: str, channel=AdvertisementChannel.FACEBOOK
):
    """
    !! DO NOT USE THIS FUNCTION IN PRODUCTION CODE !!\n
    Delete image asset insights from SQL
    """
    engine = get_engine()
    platform_info_id = get_platform_info_id(
        platform_id=channel.value, account_id=account_id
    )
    with Session(engine) as session:
        stmt = (
            sqlalchemy.delete(ImageAssetInsights)
            .where(ImageAssetInsights.platform_ad_id == platform_ad_id)
            .where(ImageAssetInsights.platform_info_id == platform_info_id)
        )
        session.execute(stmt)
        session.commit()
